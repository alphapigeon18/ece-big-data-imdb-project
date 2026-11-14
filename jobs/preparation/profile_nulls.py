"""Data preparation - step 1: missing value profile of every IMDb column.

A value is counted as missing when it is a real null, the IMDb ``\\N``
marker or a blank string. Raw files are read as strings so that nothing is
lost by an implicit cast.

Outputs:
    outputs/preparation/null_profile(.md)      one row per table x column
    outputs/preparation/top5_null_columns(.md) five most incomplete columns
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common.io_utils import missing_condition, read_raw, save_result  # noqa
from common.schemas import IMDB_FILES  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "preparation"


def profile_table(df, table):
    """Return one row per column with the missing count and percentage.

    A single aggregation pass is used per table (one scan of the file).
    """
    aggs = [F.count(F.lit(1)).alias("__total")]
    aggs += [
        F.sum(F.when(missing_condition(c), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    row = df.agg(*aggs).collect()[0]
    total = row["__total"]
    return [
        (table, c, total, row[c], 100.0 * row[c] / total if total else 0.0)
        for c in df.columns
    ]


def main():
    spark = get_spark("prep-null-profile")
    records = []
    for table in IMDB_FILES:
        print(f"profiling {table}...")
        records += profile_table(read_raw(spark, table), table)

    profile = spark.createDataFrame(
        records,
        "table string, column string, total_rows long, missing long, "
        "pct_missing double",
    ).withColumn("pct_missing", F.round("pct_missing", 3))

    save_result(
        profile.orderBy("table", "column"), SECTION, "null_profile",
        md_rows=100, title="Missing values per column",
    )
    save_result(
        profile.orderBy(F.desc("pct_missing")).limit(5), SECTION,
        "top5_null_columns", title="Top 5 columns by % missing",
    )
    spark.stop()


if __name__ == "__main__":
    main()
