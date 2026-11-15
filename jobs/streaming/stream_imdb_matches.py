"""Streaming - step 3: PySpark Structured Streaming job that detects live
Wikipedia edits about titles and people of the IMDb dataset.

Pipeline:
    Kafka topic (JSON events)
      -> parse with an explicit schema, event time = edit timestamp
      -> watermark + de-duplication of redelivered events (event_id)
      -> normalised page title  ==  stream-static join  ==  IMDb lookup
      -> confidence rules on the Wikipedia disambiguator "(2010 film)"
      -> sinks:
         * matched events                       (Parquet, append)
         * 5-min tumbling counts per type       (Parquet, append)
         * 5-min tumbling counts of all events  (Parquet, append)
         * 10-min sliding / 5-min top pages     (console, update)
      + every micro-batch progress report     (JSONL, via a listener)

Late data: events are aggregated on *event time*. The watermark
(``--watermark``, default 10 minutes) tells Spark how long to wait for late
events; a window is finalised and written once the watermark passes its
end, and events older than the watermark are dropped from the aggregations
(Spark reports them as ``numRowsDroppedByWatermark``). Every matched event
also stores its ingestion delay so lateness can be measured.

Usage (inside the Spark container):
    python jobs/streaming/stream_imdb_matches.py --minutes 20
    # offline test without Kafka (directory of JSON lines files):
    python jobs/streaming/stream_imdb_matches.py --source files \
        --input-dir data/streaming/test_events --minutes 1
"""

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyspark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType, IntegerType, LongType, StringType, StructField, StructType,
)

from common import config  # noqa: E402
from common.spark import get_spark  # noqa: E402
from common.text import disambiguator, normalize_key  # noqa: E402

OLD_NEW = StructType([
    StructField("old", LongType()), StructField("new", LongType()),
])
EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("event_id", StringType()),
    StructField("event_dt", StringType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("title_url", StringType()),
    StructField("comment", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("wiki", StringType()),
    StructField("server_name", StringType()),
    StructField("length", OLD_NEW),
    StructField("revision", OLD_NEW),
])

FILM_WORDS = r"film|movie|series|miniseries|tv|serie|téléfilm|feuilleton"
PERSON_WORDS = (r"actor|actress|director|filmmaker|screenwriter|writer|"
                r"comedian|producer|acteur|actrice|réalisateur|scénariste")


class ProgressToFile(StreamingQueryListener):
    """Append every query progress report to a JSON lines file."""

    def __init__(self, path):
        self.path = path

    def _write(self, record):
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    def onQueryStarted(self, event):
        print(f"[stream] query started: {event.name}")

    def onQueryProgress(self, event):
        self._write(json.loads(event.progress.json))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[stream] query terminated: {event.id}")


def read_source(spark, args):
    if args.source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", os.environ.get(
                "KAFKA_BOOTSTRAP", "kafka:19092"))
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 20000)
            .load()
            .select(F.col("value").cast("string").alias("value"),
                    F.col("timestamp").alias("ingest_time"))
        )
    else:
        raw = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", 1)
            .load(args.input_dir)
            .select("value", F.current_timestamp().alias("ingest_time"))
        )
    return raw


def match_confidence():
    dis = F.col("disambiguator")
    dis_kind = (
        F.when(dis == "", "none")
        .when(dis.rlike(FILM_WORDS), "title")
        .when(dis.rlike(PERSON_WORDS), "person")
        .otherwise("other")
    )
    year_in_dis = F.regexp_extract(dis, r"(\d{4})", 1)
    return (
        F.when(
            (dis_kind == F.col("entity_type"))
            & ((year_in_dis == "") | (year_in_dis == F.col("year")
                                      .cast("string"))),
            "high",
        )
        .when(dis_kind == F.col("entity_type"), "medium")
        .when((dis_kind == "none") & (F.col("n_candidates") == 1), "medium")
        .when(dis_kind == "none", "low")
        .otherwise("rejected")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=20)
    parser.add_argument("--source", choices=["kafka", "files"],
                        default="kafka")
    parser.add_argument("--input-dir", default=str(
        config.STREAM_DIR / "test_events"))
    parser.add_argument("--topic", default=os.environ.get(
        "KAFKA_TOPIC", "wiki-changes"))
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--reset", action="store_true",
                        help="delete previous outputs and checkpoints")
    args = parser.parse_args()

    out = config.STREAM_DIR
    if args.reset and out.exists():
        import shutil
        for sub in ["matched", "window_counts", "all_window_counts",
                    "checkpoints", "progress.jsonl"]:
            target = out / sub
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()

    extra = {}
    if args.source == "kafka":
        extra["spark.jars.packages"] = (
            "org.apache.spark:spark-sql-kafka-0-10_2.13:"
            f"{pyspark.__version__}"
        )
    spark = get_spark("stream-imdb-matches", extra)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.streams.addListener(ProgressToFile(str(out / "progress.jsonl")))

    events = (
        read_source(spark, args)
        .select(F.from_json("value", EVENT_SCHEMA).alias("e"), "ingest_time")
        .select("e.*", "ingest_time")
        .where(F.col("title").isNotNull() & F.col("timestamp").isNotNull())
        .withColumn("event_time", F.timestamp_seconds("timestamp"))
        .withColumn("event_id", F.coalesce("event_id",
                                           F.col("id").cast("string")))
        .withWatermark("event_time", args.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .withColumn("key", normalize_key("title"))
        .withColumn("disambiguator", disambiguator("title"))
    )

    lookup = spark.read.parquet(str(out / "lookup"))
    matched = (
        events.join(F.broadcast(lookup), "key")
        .withColumn("confidence", match_confidence())
        .where(F.col("confidence") != "rejected")
        .withColumn("processing_time", F.current_timestamp())
        .withColumn(
            "ingest_delay_s",
            F.col("ingest_time").cast("double")
            - F.col("event_time").cast("double"),
        )
        .withColumn("size_change",
                    F.col("length.new") - F.col("length.old"))
    )
    checkpoints = out / "checkpoints"

    q_matched = (
        matched.select(
            "event_id", "event_time", "ingest_time", "processing_time",
            "ingest_delay_s", "wiki", "title", "title_url", "type", "user",
            "bot", "minor", "comment", "size_change", "entity_type",
            "subtype", "imdb_id", "display_name", "year", "votes",
            "n_candidates", "confidence",
        )
        .writeStream.queryName("matched_events")
        .format("parquet").outputMode("append")
        .option("path", str(out / "matched"))
        .option("checkpointLocation", str(checkpoints / "matched"))
        .trigger(processingTime=args.trigger)
        .start()
    )

    q_windows = (
        matched.groupBy(F.window("event_time", args.window),
                        "entity_type", "confidence")
        .agg(
            F.count("*").alias("n_edits"),
            F.approx_count_distinct("imdb_id").alias("n_entities"),
            F.sum(F.col("bot").cast("int")).alias("n_bot_edits"),
            F.sum("size_change").alias("bytes_changed"),
            F.avg("ingest_delay_s").alias("avg_ingest_delay_s"),
        )
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "*")
        .drop("window")
        .writeStream.queryName("matched_window_counts")
        .format("parquet").outputMode("append")
        .option("path", str(out / "window_counts"))
        .option("checkpointLocation", str(checkpoints / "window_counts"))
        .trigger(processingTime=args.trigger)
        .start()
    )

    q_all = (
        events.groupBy(F.window("event_time", args.window), "wiki")
        .agg(F.count("*").alias("n_events"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "wiki", "n_events")
        .writeStream.queryName("all_window_counts")
        .format("parquet").outputMode("append")
        .option("path", str(out / "all_window_counts"))
        .option("checkpointLocation", str(checkpoints / "all_window_counts"))
        .trigger(processingTime=args.trigger)
        .start()
    )

    q_console = (
        matched.groupBy(
            F.window("event_time", "10 minutes", "5 minutes"),
            "entity_type", "display_name",
        )
        .agg(F.count("*").alias("n_edits"))
        .writeStream.queryName("sliding_top_pages")
        .format("console").outputMode("update")
        .option("truncate", "false").option("numRows", 20)
        .option("checkpointLocation", str(checkpoints / "console"))
        .trigger(processingTime=args.trigger)
        .start()
    )

    queries = [q_matched, q_windows, q_all, q_console]
    print(f"[stream] running for {args.minutes} minutes...")
    spark.streams.awaitAnyTermination(timeout=int(args.minutes * 60))
    for query in queries:
        if query.isActive:
            if args.source == "files":
                query.processAllAvailable()
            query.stop()
    print("[stream] stopped")
    spark.stop()


if __name__ == "__main__":
    main()
