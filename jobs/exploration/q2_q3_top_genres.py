"""Exploration Q2 + Q3 - most popular genres per year and how they compare
with the other genres.

Popularity of a genre in a year = total number of IMDb votes received by
the feature films of that genre released that year (votes measure audience
engagement; the film count and mean rating are reported as well).

Outputs:
    outputs/exploration/q2_top3_genres_per_year(.md)
    outputs/exploration/q3_top3_vs_rest_per_year(.md)
    outputs/exploration/q3_top3_vs_rest_by_decade(.md)
    outputs/exploration/q3_genre_top3_frequency(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.films import feature_films, films_by_genre  # noqa: E402
from common.io_utils import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"
TOP_N = 3


def ratio(numerator, denominator):
    """Rounded integer ratio of two columns."""
    return F.round(F.col(numerator) / F.col(denominator), 0).cast("long")


def main():
    spark = get_spark("q2-q3-top-genres")
    films = films_by_genre(feature_films(spark)).where(
        F.col("year").between(config.YEAR_MIN, config.YEAR_MAX)
    )

    per_genre = films.groupBy("year", "genre").agg(
        F.count("*").alias("n_films"),
        F.sum("votes").alias("total_votes"),
        F.avg("rating").alias("avg_rating"),
    )
    by_year = Window.partitionBy("year")
    ranked = (
        per_genre
        .withColumn("rank", F.row_number().over(
            by_year.orderBy(F.desc("total_votes"), "genre")))
        .withColumn("vote_share_pct", F.round(
            100 * F.col("total_votes") / F.sum("total_votes").over(by_year),
            2))
        .withColumn("is_top", F.col("rank") <= TOP_N)
        .cache()
    )

    # ----------------------------------------------------------------- Q2
    top3 = ranked.where("is_top").select(
        "year", "rank", "genre", "n_films", "total_votes",
        F.round("avg_rating", 2).alias("avg_rating"), "vote_share_pct",
    ).orderBy("year", "rank")
    save_result(top3, SECTION, "q2_top3_genres_per_year", md_rows=200,
                title="Top 3 genres per year (by total votes)")

    # ----------------------------------------------------------------- Q3
    group = F.when(F.col("is_top"), "top3").otherwise("rest")
    groups = ranked.withColumn("group", group).groupBy("year", "group").agg(
        F.countDistinct("genre").alias("n_genres"),
        F.sum("n_films").alias("n_films"),
        F.sum("total_votes").alias("votes"),
        F.sum("vote_share_pct").alias("vote_share_pct"),
        (F.sum(F.col("avg_rating") * F.col("n_films")) / F.sum("n_films"))
        .alias("avg_rating"),
    )
    wide = (
        groups.groupBy("year").pivot("group", ["top3", "rest"]).agg(
            F.first("n_genres").alias("genres"),
            F.first("n_films").alias("films"),
            F.first("votes").alias("votes"),
            F.first("vote_share_pct").alias("share"),
            F.first("avg_rating").alias("rating"),
        )
        .withColumn("top3_votes_per_film", ratio("top3_votes", "top3_films"))
        .withColumn("rest_votes_per_film", ratio("rest_votes", "rest_films"))
        .withColumn("top3_votes_per_genre", ratio("top3_votes", "top3_genres"))
        .withColumn("rest_votes_per_genre", ratio("rest_votes", "rest_genres"))
        .select(
            "year",
            F.round("top3_share", 2).alias("top3_vote_share_pct"),
            "top3_films", "rest_films", "rest_genres",
            "top3_votes_per_film", "rest_votes_per_film",
            "top3_votes_per_genre", "rest_votes_per_genre",
            F.round("top3_rating", 2).alias("top3_avg_rating"),
            F.round("rest_rating", 2).alias("rest_avg_rating"),
        )
        .orderBy("year")
        .cache()
    )
    save_result(wide, SECTION, "q3_top3_vs_rest_per_year", md_rows=60,
                title="Top 3 genres vs all other genres, per year")

    decade = (
        wide.withColumn("decade", (F.col("year") / 10).cast("int") * 10)
        .groupBy("decade").agg(
            F.round(F.avg("top3_vote_share_pct"), 1)
            .alias("avg_top3_vote_share_pct"),
            F.round(F.avg(F.col("top3_votes_per_film")
                          / F.col("rest_votes_per_film")), 2)
            .alias("votes_per_film_ratio_top3_vs_rest"),
            F.round(F.avg(F.col("top3_films")
                          / (F.col("top3_films") + F.col("rest_films"))
                          * 100), 1)
            .alias("avg_top3_film_share_pct"),
            F.round(F.avg("top3_avg_rating"), 2).alias("top3_avg_rating"),
            F.round(F.avg("rest_avg_rating"), 2).alias("rest_avg_rating"),
        ).orderBy("decade")
    )
    save_result(decade, SECTION, "q3_top3_vs_rest_by_decade",
                title="Top 3 vs rest, averaged per decade")

    frequency = (
        ranked.where("is_top").groupBy("genre").agg(
            F.count("*").alias("years_in_top3"),
            F.sum((F.col("rank") == 1).cast("int")).alias("years_ranked_1"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        ).orderBy(F.desc("years_in_top3"))
    )
    save_result(frequency, SECTION, "q3_genre_top3_frequency",
                title="How often each genre is in the yearly top 3")
    spark.stop()


if __name__ == "__main__":
    main()
