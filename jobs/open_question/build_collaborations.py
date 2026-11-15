"""Open question - step 1: build the director x actor collaboration table.

Question: do recurring director-actor partnerships (the same actor cast by
the same director on >= MIN_PARTNERSHIP films) produce better rated films
than the director's other films, and does the effect fade as the
partnership repeats?

This job joins title.crew (directors), title.principals (cast),
title.basics (feature films, year), title.ratings and name.basics, and
numbers every film of a pair in chronological order (collaboration index).

Outputs:
    data/derived/collaborations/           Parquet, one row per
                                           (director, actor, film)
    outputs/open_question/top_partnerships(.md)
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.films import (  # noqa: E402
    acting_credits, directing_credits, feature_films, people_names,
)
from common.io_utils import save_result, write_parquet  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "open_question"
MIN_PARTNERSHIP = int(os.environ.get("MIN_PARTNERSHIP", 3))
DERIVED = config.DATA_DIR / "derived"


def main():
    spark = get_spark("open-build-collaborations")
    films = feature_films(spark, min_votes=config.MIN_VOTES).where(
        F.col("year").isNotNull()
    ).select("tconst", "title", "year", "rating", "votes")

    directed = directing_credits(spark).withColumnRenamed(
        "nconst", "director")
    acted = acting_credits(spark).withColumnRenamed("nconst", "actor")

    pairs = (
        directed.join(films, "tconst")
        .join(acted, "tconst")
        .where(F.col("director") != F.col("actor"))   # self-casting excluded
    )
    pair = Window.partitionBy("director", "actor")
    collaborations = (
        pairs
        .withColumn("pair_n_films", F.count("*").over(pair))
        .withColumn("collab_index", F.row_number().over(
            pair.orderBy("year", "tconst")))
        .withColumn("pair_first_year", F.min("year").over(pair))
        .withColumn("is_partnership",
                    F.col("pair_n_films") >= MIN_PARTNERSHIP)
    )
    write_parquet(collaborations, DERIVED / "collaborations", partitions=16)

    stored = spark.read.parquet(str(DERIVED / "collaborations"))
    names = people_names(spark)
    top = (
        stored.where("is_partnership")
        .groupBy("director", "actor").agg(
            F.count("*").alias("n_films"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.sum("votes").alias("total_votes"),
        )
        .join(names.withColumnRenamed("nconst", "director")
              .withColumnRenamed("name", "director_name"), "director", "left")
        .join(names.withColumnRenamed("nconst", "actor")
              .withColumnRenamed("name", "actor_name"), "actor", "left")
        .select("director_name", "actor_name", "n_films", "first_year",
                "last_year", "avg_rating", "total_votes")
        .orderBy(F.desc("n_films"), F.desc("total_votes"))
    )
    save_result(top, SECTION, "top_partnerships", md_rows=25,
                title=f"Longest director-actor partnerships "
                      f"(>= {MIN_PARTNERSHIP} films)")
    spark.stop()


if __name__ == "__main__":
    main()
