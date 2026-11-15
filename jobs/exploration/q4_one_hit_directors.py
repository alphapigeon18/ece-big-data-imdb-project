"""Exploration Q4 - directors with exactly one feature film rated >= 8.0
and several (>= 2) other feature films rated <= 6.0.

Only films with at least ``MIN_VOTES`` votes are considered so that a
rating based on a handful of votes does not decide the result.

Output: outputs/exploration/q4_one_hit_directors(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.films import (  # noqa: E402
    directing_credits, feature_films, people_names,
)
from common.io_utils import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"
HIT = 8.0
FLOP = 6.0
MIN_FLOPS = 2


def main():
    spark = get_spark("q4-one-hit-directors")
    films = feature_films(spark, min_votes=config.MIN_VOTES)
    directed = directing_credits(spark).join(films, "tconst")

    film = F.struct("rating", "votes", "title", "year")
    per_director = directed.groupBy("nconst").agg(
        F.count("*").alias("n_films"),
        F.sum((F.col("rating") >= HIT).cast("int")).alias("n_hits"),
        F.sum((F.col("rating") <= FLOP).cast("int")).alias("n_flops"),
        F.max(F.when(F.col("rating") >= HIT, film)).alias("hit"),
        F.sort_array(F.collect_list(F.when(F.col("rating") <= FLOP, film)))
        .alias("flops"),
        F.avg(F.when(F.col("rating") < HIT, F.col("rating")))
        .alias("avg_other_rating"),
    )
    result = (
        per_director
        .where((F.col("n_hits") == 1) & (F.col("n_flops") >= MIN_FLOPS))
        .join(people_names(spark), "nconst", "left")
        .select(
            "nconst", "name", "n_films",
            F.col("hit.title").alias("hit_title"),
            F.col("hit.year").alias("hit_year"),
            F.col("hit.rating").alias("hit_rating"),
            "n_flops",
            F.round("avg_other_rating", 2).alias("avg_other_rating"),
            F.slice(F.transform(
                "flops",
                lambda f: F.concat(f.title, F.lit(" ("), f.rating, F.lit(")")),
            ), 1, 5).alias("worst_flops"),
        )
        .orderBy(F.desc("n_flops"), F.desc("hit_rating"))
    )
    save_result(result, SECTION, "q4_one_hit_directors", md_rows=50,
                title=f"Directors with exactly one film >= {HIT} and "
                      f">= {MIN_FLOPS} films <= {FLOP} "
                      f"(films with >= {config.MIN_VOTES} votes)")
    spark.stop()


if __name__ == "__main__":
    main()
