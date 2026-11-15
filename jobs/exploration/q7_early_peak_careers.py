"""Exploration Q7 - actors with a 20+ year film career and 15+ films whose
popularity peaked early.

Career = feature films (>= ``MIN_VOTES`` votes) credited as actor/actress.
A career "peaked early" when both conditions hold:

* the best rated film (ties: most votes) is in the first third of the
  career span, and
* the mean rating of the first half of the career is higher than the mean
  rating of the second half (split at the career mid-year).

Output: outputs/exploration/q7_early_peak_careers(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.films import (  # noqa: E402
    acting_credits, feature_films, people_names,
)
from common.io_utils import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"
MIN_SPAN = 20
MIN_FILMS = 15
EARLY_FRACTION = 1 / 3


def main():
    spark = get_spark("q7-early-peak")
    films = feature_films(spark, min_votes=config.MIN_VOTES).where(
        F.col("year").isNotNull()
    )
    credits = acting_credits(spark).join(films, "tconst")

    careers = credits.groupBy("nconst").agg(
        F.min("year").alias("first_year"),
        F.max("year").alias("last_year"),
        F.count("*").alias("n_films"),
        F.max(F.struct("rating", "votes", "year", "title")).alias("peak"),
    ).withColumn("span", F.col("last_year") - F.col("first_year")).where(
        (F.col("span") >= MIN_SPAN) & (F.col("n_films") >= MIN_FILMS)
    )

    halves = (
        credits.join(careers.select("nconst", "first_year", "span"), "nconst")
        .withColumn(
            "half",
            F.when(F.col("year") - F.col("first_year") <= F.col("span") / 2,
                   "early").otherwise("late"),
        )
        .groupBy("nconst").pivot("half", ["early", "late"])
        .agg(F.avg("rating").alias("avg"), F.count("rating").alias("n"))
    )

    result = (
        careers.join(halves, "nconst")
        .withColumn(
            "peak_position",
            (F.col("peak.year") - F.col("first_year")) / F.col("span"),
        )
        .where(
            (F.col("peak_position") <= EARLY_FRACTION)
            & (F.col("early_avg") > F.col("late_avg"))
        )
        .join(people_names(spark), "nconst", "left")
        .select(
            "nconst", "name", "first_year", "last_year", "span", "n_films",
            F.col("peak.title").alias("peak_title"),
            F.col("peak.year").alias("peak_year"),
            F.col("peak.rating").alias("peak_rating"),
            F.round("peak_position", 2).alias("peak_position"),
            F.round("early_avg", 2).alias("early_avg_rating"),
            F.round("late_avg", 2).alias("late_avg_rating"),
            F.round(F.col("early_avg") - F.col("late_avg"), 2)
            .alias("rating_drop"),
        )
        .orderBy(F.desc("rating_drop"))
        .cache()
    )
    save_result(result, SECTION, "q7_early_peak_careers", md_rows=50,
                title=f"Actors with >= {MIN_SPAN} years and >= {MIN_FILMS} "
                      "films whose career peaked early")

    population = careers.count()
    share = result.count()
    print(f"{share} / {population} long careers peaked early")
    share_df = spark.createDataFrame(
        [(population, share, round(100 * share / max(population, 1), 2))],
        "long_careers long, early_peak long, pct double",
    )
    save_result(share_df, SECTION, "q7_early_peak_share",
                title="Share of long careers that peaked early")
    spark.stop()


if __name__ == "__main__":
    main()
