"""Exploration Q5 - TV series with >= 3 seasons of >= 10 episodes whose
worst episode is in the last season and best episode in the first two.

* Season size counts every episode listed in title.episode.
* Best / worst episodes are chosen among rated episodes with at least
  ``EPISODE_MIN_VOTES`` votes (ties: more votes wins).
* The last season is the highest season number of the series.

Output: outputs/exploration/q5_series_best_early_worst_last(.md)
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.io_utils import read_table, save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"
MIN_EPISODES = 10
MIN_SEASONS = 3
EPISODE_MIN_VOTES = int(os.environ.get("EPISODE_MIN_VOTES", 10))


def main():
    spark = get_spark("q5-tv-series")
    episodes = read_table(spark, "title_episode").where(
        F.col("seasonNumber").isNotNull()
    )
    series = read_table(spark, "title_basics").where(
        F.col("titleType").isin("tvSeries", "tvMiniSeries")
    ).select(
        F.col("tconst").alias("parentTconst"),
        F.col("primaryTitle").alias("series_title"),
        F.col("startYear").alias("series_start"),
    )

    seasons = episodes.groupBy("parentTconst", "seasonNumber").agg(
        F.count("*").alias("n_episodes")
    )
    eligible = seasons.groupBy("parentTconst").agg(
        F.sum((F.col("n_episodes") >= MIN_EPISODES).cast("int"))
        .alias("n_big_seasons"),
        F.max("seasonNumber").alias("last_season"),
        F.count("*").alias("n_seasons"),
    ).where(F.col("n_big_seasons") >= MIN_SEASONS)

    rated = (
        episodes.join(eligible.select("parentTconst"), "parentTconst")
        .join(read_table(spark, "title_ratings"), "tconst")
        .where(F.col("numVotes") >= EPISODE_MIN_VOTES)
    )
    by_series = Window.partitionBy("parentTconst")
    ep = F.struct(
        "seasonNumber", "episodeNumber", "tconst", "averageRating", "numVotes"
    )
    extremes = (
        rated
        .withColumn("best_rank", F.row_number().over(by_series.orderBy(
            F.desc("averageRating"), F.desc("numVotes"))))
        .withColumn("worst_rank", F.row_number().over(by_series.orderBy(
            F.asc("averageRating"), F.desc("numVotes"))))
        .where((F.col("best_rank") == 1) | (F.col("worst_rank") == 1))
        .groupBy("parentTconst").agg(
            F.max(F.when(F.col("best_rank") == 1, ep)).alias("best"),
            F.max(F.when(F.col("worst_rank") == 1, ep)).alias("worst"),
            F.count("*").alias("_n"),
        )
    )
    season_avg = rated.groupBy("parentTconst").pivot(
        "seasonNumber", [1, 2]
    ).agg(F.round(F.avg("averageRating"), 2)).withColumnsRenamed(
        {"1": "s1_avg_rating", "2": "s2_avg_rating"}
    )
    last_avg = rated.join(
        eligible.select("parentTconst", "last_season"), "parentTconst"
    ).where(F.col("seasonNumber") == F.col("last_season")).groupBy(
        "parentTconst"
    ).agg(F.round(F.avg("averageRating"), 2).alias("last_season_avg_rating"))

    result = (
        extremes.join(eligible, "parentTconst")
        .where(
            (F.col("worst.seasonNumber") == F.col("last_season"))
            & (F.col("best.seasonNumber") <= 2)
        )
        .join(series, "parentTconst")
        .join(season_avg, "parentTconst", "left")
        .join(last_avg, "parentTconst", "left")
        .select(
            "parentTconst", "series_title", "series_start", "n_seasons",
            "n_big_seasons", "last_season",
            F.format_string("S%dE%d", "best.seasonNumber",
                            "best.episodeNumber").alias("best_episode"),
            F.col("best.averageRating").alias("best_rating"),
            F.format_string("S%dE%d", "worst.seasonNumber",
                            "worst.episodeNumber").alias("worst_episode"),
            F.col("worst.averageRating").alias("worst_rating"),
            "s1_avg_rating", "s2_avg_rating", "last_season_avg_rating",
        )
        .orderBy(F.desc(F.col("best_rating") - F.col("worst_rating")))
    )
    save_result(result, SECTION, "q5_series_best_early_worst_last",
                md_rows=50,
                title="Series whose best episode is in seasons 1-2 and "
                      "worst episode in the last season")
    spark.stop()


if __name__ == "__main__":
    main()
