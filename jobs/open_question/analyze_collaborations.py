"""Open question - step 2: are partnership films better rated, and does the
effect fade with repetition?

Input: data/derived/collaborations (see build_collaborations.py).

1. Director-level paired comparison: for every director who has at least
   one partnership, mean rating of films that include a partner actor vs
   mean rating of the director's other films (same director on both sides,
   so director talent is controlled for).
2. Repetition curve: rating of partnership films relative to the
   director's own mean rating ("rating lift"), by collaboration index
   (1st, 2nd, 3rd, 4th, 5th+ film together).
3. The same lift by decade in which the partnership started.

Outputs (outputs/open_question/):
    director_paired_comparison(.md)
    lift_by_collaboration_index(.md)
    lift_by_start_decade(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.io_utils import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "open_question"
MIN_OTHER_FILMS = 2


def main():
    spark = get_spark("open-analyze-collaborations")
    collab = spark.read.parquet(
        str(config.DATA_DIR / "derived" / "collaborations")
    )

    # One row per (director, film): does the film include a partner actor?
    director_films = collab.groupBy(
        "director", "tconst", "year", "rating", "votes"
    ).agg(F.max(F.col("is_partnership").cast("int")).alias("has_partner"))
    by_director = Window.partitionBy("director")
    director_films = director_films.withColumn(
        "director_mean", F.avg("rating").over(by_director)
    ).cache()

    # ------------------------------------------------ 1. paired comparison
    per_director = (
        director_films.groupBy("director").pivot("has_partner", [1, 0]).agg(
            F.avg("rating").alias("rating"), F.count("rating").alias("n")
        )
        .withColumnsRenamed({"1_rating": "partner_rating", "1_n": "partner_n",
                             "0_rating": "other_rating", "0_n": "other_n"})
        .where((F.col("partner_n") >= 1)
               & (F.col("other_n") >= MIN_OTHER_FILMS))
        .withColumn("diff", F.col("partner_rating") - F.col("other_rating"))
        .cache()
    )
    paired = per_director.agg(
        F.count("*").alias("n_directors"),
        F.sum("partner_n").alias("partnership_films"),
        F.sum("other_n").alias("other_films"),
        F.round(F.avg("partner_rating"), 3).alias("avg_partnership_rating"),
        F.round(F.avg("other_rating"), 3).alias("avg_other_rating"),
        F.round(F.avg("diff"), 3).alias("mean_diff"),
        F.round(F.expr("percentile_approx(diff, 0.5)"), 3)
        .alias("median_diff"),
        F.round(F.stddev("diff") / F.sqrt(F.count("*")), 4)
        .alias("std_error"),
        F.round(100 * F.avg((F.col("diff") > 0).cast("int")), 1)
        .alias("pct_directors_better_with_partners"),
    ).withColumn(
        "t_statistic", F.round(F.col("mean_diff") / F.col("std_error"), 2)
    )
    save_result(paired, SECTION, "director_paired_comparison",
                title="Partnership films vs the same director's other films")

    # ---------------------------------------------- 2. repetition curve
    lift = (
        collab.where("is_partnership")
        .join(director_films.select("director", "tconst", "director_mean"),
              ["director", "tconst"])
        .withColumn("lift", F.col("rating") - F.col("director_mean"))
        .withColumn(
            "collaboration",
            F.when(F.col("collab_index") >= 5, F.lit("5+"))
            .otherwise(F.col("collab_index").cast("string")),
        )
        .cache()
    )
    curve = lift.groupBy("collaboration").agg(
        F.count("*").alias("n_pair_films"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(F.avg("lift"), 3).alias("avg_lift_vs_director_mean"),
        F.round(F.expr("percentile_approx(votes, 0.5)"), 0)
        .alias("median_votes"),
    ).orderBy("collaboration")
    save_result(curve, SECTION, "lift_by_collaboration_index",
                title="Rating lift by n-th film of the partnership")

    decade = (
        lift.withColumn("start_decade",
                        (F.col("pair_first_year") / 10).cast("int") * 10)
        .groupBy("start_decade").agg(
            F.countDistinct("director", "actor").alias("n_partnerships"),
            F.count("*").alias("n_pair_films"),
            F.round(F.avg("lift"), 3).alias("avg_lift_vs_director_mean"),
        ).orderBy("start_decade")
    )
    save_result(decade, SECTION, "lift_by_start_decade",
                title="Rating lift by decade the partnership started")
    spark.stop()


if __name__ == "__main__":
    main()
