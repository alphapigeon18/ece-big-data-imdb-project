"""Reusable building blocks for the analysis jobs."""

from pyspark.sql import functions as F

from common import config
from common.io_utils import read_table

ACTING = ["actor", "actress"]


def feature_films(spark, rated=True, min_votes=0):
    """Feature-length films: titleType = movie, runtime >= 40 min, not adult.

    Columns: tconst, title, year, runtime, genres
    (+ rating, votes when ``rated``).
    """
    films = (
        read_table(spark, "title_basics")
        .where(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= config.FEATURE_MIN_RUNTIME)
            & (F.coalesce(F.col("isAdult"), F.lit(0)) == 0)
        )
        .select(
            "tconst",
            F.col("primaryTitle").alias("title"),
            F.col("startYear").alias("year"),
            F.col("runtimeMinutes").alias("runtime"),
            "genres",
        )
    )
    if not rated:
        return films
    ratings = read_table(spark, "title_ratings").where(
        F.col("numVotes") >= min_votes
    )
    return films.join(
        ratings.select(
            "tconst",
            F.col("averageRating").alias("rating"),
            F.col("numVotes").alias("votes"),
        ),
        "tconst",
    )


def films_by_genre(films):
    """One row per (film, genre); films without genre are dropped."""
    return films.withColumn("genre", F.explode("genres")).drop("genres")


def people_names(spark):
    return read_table(spark, "name_basics").select(
        "nconst", F.col("primaryName").alias("name")
    )


def acting_credits(spark):
    """(tconst, nconst) pairs where the person is credited as actor/actress."""
    return (
        read_table(spark, "title_principals")
        .where(F.col("category").isin(ACTING))
        .select("tconst", "nconst")
        .distinct()
    )


def directing_credits(spark):
    """(tconst, nconst) pairs from title.crew directors."""
    return (
        read_table(spark, "title_crew")
        .select("tconst", F.explode("directors").alias("nconst"))
        .distinct()
    )
