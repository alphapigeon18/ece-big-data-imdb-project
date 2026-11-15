"""Exploration Q1 - feature films whose runtime is notably different from
the average runtime of their genre.

A film is an outlier for a genre when its runtime is more than
``Z_THRESHOLD`` standard deviations away from the genre mean
(|z-score| >= 2 by default). A film with several genres is evaluated
against each of them. The IQR rule is computed as well for comparison.

Outputs:
    outputs/exploration/q1_genre_runtime_stats(.md)
    outputs/exploration/q1_runtime_outliers(.md)
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common.films import feature_films, films_by_genre  # noqa: E402
from common.io_utils import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"
Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", 2.0))


def main():
    spark = get_spark("q1-runtime-outliers")
    films = films_by_genre(feature_films(spark, rated=False))

    stats = films.groupBy("genre").agg(
        F.count("*").alias("n_films"),
        F.avg("runtime").alias("avg_runtime"),
        F.stddev("runtime").alias("std_runtime"),
        F.percentile_approx("runtime", [0.25, 0.5, 0.75]).alias("q"),
    ).select(
        "genre", "n_films", "avg_runtime", "std_runtime",
        F.col("q")[0].alias("q1"), F.col("q")[1].alias("median"),
        F.col("q")[2].alias("q3"),
    )

    scored = (
        films.join(stats, "genre")
        .withColumn(
            "z_score",
            (F.col("runtime") - F.col("avg_runtime")) / F.col("std_runtime"),
        )
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn(
            "iqr_outlier",
            (F.col("runtime") < F.col("q1") - 1.5 * F.col("iqr"))
            | (F.col("runtime") > F.col("q3") + 1.5 * F.col("iqr")),
        )
        .withColumn("z_outlier", F.abs("z_score") >= Z_THRESHOLD)
        .withColumn(
            "direction",
            F.when(F.col("z_score") > 0, "longer").otherwise("shorter"),
        )
        .cache()
    )

    outliers = scored.where("z_outlier").select(
        "genre", "tconst", "title", "year", "runtime",
        F.round("avg_runtime", 1).alias("genre_avg_runtime"),
        F.round("z_score", 2).alias("z_score"), "direction",
    ).orderBy("genre", F.desc(F.abs("z_score")))

    summary = scored.groupBy("genre").agg(
        F.first("n_films").alias("n_films"),
        F.round(F.first("avg_runtime"), 1).alias("avg_runtime"),
        F.round(F.first("std_runtime"), 1).alias("std_runtime"),
        F.first("median").alias("median_runtime"),
        F.sum(F.when(F.col("z_outlier") & (F.col("direction") == "longer"),
                     1).otherwise(0)).alias("n_longer"),
        F.sum(F.when(F.col("z_outlier") & (F.col("direction") == "shorter"),
                     1).otherwise(0)).alias("n_shorter"),
        F.sum(F.col("z_outlier").cast("int")).alias("n_outliers_z"),
        F.sum(F.col("iqr_outlier").cast("int")).alias("n_outliers_iqr"),
    ).withColumn(
        "pct_outliers_z",
        F.round(100 * F.col("n_outliers_z") / F.col("n_films"), 2),
    ).orderBy(F.desc("n_films"))

    save_result(summary, SECTION, "q1_genre_runtime_stats", md_rows=40,
                title=f"Runtime per genre (outlier: |z| >= {Z_THRESHOLD})")
    save_result(outliers, SECTION, "q1_runtime_outliers", md_rows=40,
                title="Feature films with an atypical runtime for their genre")
    spark.stop()


if __name__ == "__main__":
    main()
