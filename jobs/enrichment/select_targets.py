"""Enrichment - step 1 (Spark): select the films and people to enrich.

Scope: for every (year, genre) between YEAR_MIN and YEAR_MAX, the TOP_N
feature films with the most votes (ties: higher rating), and for each of
those films the first N_PRINCIPALS principals in IMDb billing order.

The scope is controlled by environment variables only (TOP_N,
N_PRINCIPALS, YEAR_MIN, YEAR_MAX): setting TOP_N to a very large value
runs the exact same code on the whole dataset.

Outputs (data/enrichment/):
    targets_films/        Parquet, one row per (film, year, genre, rank)
    targets_principals/   Parquet, one row per (film, principal)
    films_to_fetch.csv    distinct tconst, title, year  (input of the fetcher)
    people_to_fetch.csv   distinct nconst, name         (input of the fetcher)
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.films import feature_films, films_by_genre  # noqa: E402
from common.io_utils import read_table, write_parquet  # noqa: E402
from common.spark import get_spark  # noqa: E402

TOP_N = int(os.environ.get("TOP_N", 10))
N_PRINCIPALS = int(os.environ.get("N_PRINCIPALS", 3))
YEAR_MIN = int(os.environ.get("YEAR_MIN", config.YEAR_MIN))
YEAR_MAX = int(os.environ.get("YEAR_MAX", config.YEAR_MAX))


def write_single_csv(df, path):
    """Write a small DataFrame as one CSV file (input of a non-Spark tool)."""
    tmp = path.with_suffix(".tmp")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        str(tmp)
    )
    part = next(tmp.glob("part-*.csv"))
    part.replace(path)
    for leftover in tmp.iterdir():
        leftover.unlink()
    tmp.rmdir()


def main():
    spark = get_spark("enrich-select-targets")
    out = config.ENRICH_DIR
    out.mkdir(parents=True, exist_ok=True)

    films = films_by_genre(feature_films(spark)).where(
        F.col("year").between(YEAR_MIN, YEAR_MAX)
    )
    rank = Window.partitionBy("year", "genre").orderBy(
        F.desc("votes"), F.desc("rating"), "tconst"
    )
    targets = (
        films.withColumn("rank_in_genre_year", F.row_number().over(rank))
        .where(F.col("rank_in_genre_year") <= TOP_N)
        .select("tconst", "title", "year", "genre", "rank_in_genre_year",
                "rating", "votes")
        .cache()
    )
    write_parquet(targets, out / "targets_films", partitions=4)

    first = Window.partitionBy("tconst").orderBy("ordering")
    principals = (
        read_table(spark, "title_principals")
        .join(targets.select("tconst").distinct(), "tconst", "left_semi")
        .withColumn("principal_rank", F.row_number().over(first))
        .where(F.col("principal_rank") <= N_PRINCIPALS)
        .join(
            read_table(spark, "name_basics").select(
                "nconst", F.col("primaryName").alias("name"), "birthYear",
                "deathYear",
            ),
            "nconst", "left",
        )
        .select("tconst", "principal_rank", "ordering", "nconst", "name",
                "category", "birthYear", "deathYear")
        .cache()
    )
    write_parquet(principals, out / "targets_principals", partitions=4)

    write_single_csv(
        targets.select("tconst", "title", "year").distinct().orderBy("tconst"),
        out / "films_to_fetch.csv",
    )
    write_single_csv(
        principals.select("nconst", "name").distinct().orderBy("nconst"),
        out / "people_to_fetch.csv",
    )
    n_films = targets.select("tconst").distinct().count()
    n_people = principals.select("nconst").distinct().count()
    print(f"{targets.count()} (film, genre, year) slots -> {n_films} films, "
          f"{n_people} people to enrich")
    spark.stop()


if __name__ == "__main__":
    main()
