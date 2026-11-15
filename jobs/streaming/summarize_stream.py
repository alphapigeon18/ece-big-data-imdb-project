"""Streaming - step 4 (Spark batch): summarise a streaming test run.

Reads what the streaming job wrote (matched events, window counts and
progress reports) and produces the tables and metrics for REPORT.md.

Outputs (outputs/streaming/):
    run_metrics(.md)          totals, throughput, match rate, lateness
    window_counts(.md)        5-minute tumbling windows (matched edits)
    match_rate_by_window(.md) matched edits / all edits per window
    top_entities(.md)         most edited IMDb titles / people
    sample_matches(.md)       sample of matched events
    query_progress(.md)       per-query micro-batch statistics
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.io_utils import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "streaming"


def main():
    spark = get_spark("stream-summary")
    out = config.STREAM_DIR
    matched = spark.read.parquet(str(out / "matched")).cache()
    windows = spark.read.parquet(str(out / "window_counts"))
    all_windows = spark.read.parquet(str(out / "all_window_counts"))
    progress = spark.read.json(str(out / "progress.jsonl")).cache()

    # ----------------------------------------------------- query progress
    state = F.explode_outer("stateOperators").alias("op")
    per_query = (
        progress.select(
            "name", "batchId", "numInputRows", "inputRowsPerSecond",
            "processedRowsPerSecond", "durationMs.triggerExecution",
            state,
        )
        .groupBy("name").agg(
            F.count("batchId").alias("n_batches"),
            F.sum("numInputRows").alias("input_rows"),
            F.round(F.avg("processedRowsPerSecond"), 1)
            .alias("avg_processed_rows_per_s"),
            F.round(F.avg("triggerExecution"), 0)
            .alias("avg_batch_duration_ms"),
            F.max("op.numRowsTotal").alias("max_state_rows"),
            F.max("op.numRowsDroppedByWatermark")
            .alias("max_rows_dropped_by_watermark"),
        )
        .orderBy("name")
    )
    save_result(per_query, SECTION, "query_progress",
                title="Micro-batch statistics per streaming query")

    # ----------------------------------------------------------- metrics
    total_events = all_windows.agg(F.sum("n_events")).first()[0] or 0
    late = matched.where(F.col("ingest_delay_s") > 600).count()
    stats = matched.agg(
        F.count("*").alias("matched_edits"),
        F.countDistinct("imdb_id").alias("distinct_entities"),
        F.sum((F.col("entity_type") == "title").cast("int"))
        .alias("title_edits"),
        F.sum((F.col("entity_type") == "person").cast("int"))
        .alias("person_edits"),
        F.min("event_time").alias("first_event"),
        F.max("event_time").alias("last_event"),
        F.round(F.avg("ingest_delay_s"), 1).alias("avg_ingest_delay_s"),
        F.round(F.expr("percentile_approx(ingest_delay_s, 0.95)"), 1)
        .alias("p95_ingest_delay_s"),
    ).withColumn("events_in_closed_windows", F.lit(total_events)).withColumn(
        "matched_events_older_than_10min", F.lit(late)
    )
    save_result(stats, SECTION, "run_metrics", title="Streaming run metrics")

    save_result(
        windows.orderBy("window_start", "entity_type", "confidence"),
        SECTION, "window_counts", md_rows=60,
        title="Matched edits per 5-minute tumbling window",
    )

    rate = (
        all_windows.groupBy("window_start").agg(
            F.sum("n_events").alias("all_edits"))
        .join(
            windows.groupBy("window_start").agg(
                F.sum("n_edits").alias("matched_edits")),
            "window_start", "left",
        )
        .fillna(0, ["matched_edits"])
        .withColumn("match_rate_pct", F.round(
            100 * F.col("matched_edits") / F.col("all_edits"), 2))
        .orderBy("window_start")
    )
    save_result(rate, SECTION, "match_rate_by_window", md_rows=60,
                title="Share of Wikipedia edits matching the IMDb dataset")

    top = (
        matched.groupBy("entity_type", "imdb_id", "display_name")
        .agg(F.count("*").alias("n_edits"),
             F.countDistinct("user").alias("n_editors"),
             F.first("confidence").alias("confidence"))
        .orderBy(F.desc("n_edits"))
    )
    save_result(top, SECTION, "top_entities", md_rows=25,
                title="Most edited IMDb entities during the run")

    save_result(
        matched.select("event_time", "wiki", "title", "entity_type",
                       "imdb_id", "display_name", "confidence",
                       F.round("ingest_delay_s", 1).alias("delay_s"))
        .orderBy(F.expr("CASE confidence WHEN 'high' THEN 0 "
                        "WHEN 'medium' THEN 1 ELSE 2 END"), "event_time"),
        SECTION, "sample_matches", md_rows=25,
        title="Sample of matched events",
    )
    spark.stop()


if __name__ == "__main__":
    main()
