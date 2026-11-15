"""Data quality rules shared by the quality report and the cleaning job.

Keeping the rules in one module guarantees that what is reported in
REPORT.md is exactly what the cleaning job fixes.
"""

from pyspark.sql import functions as F

from common import config

ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound"]
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]


def row_rules():
    """Single-row rules: {table: [(rule, description, condition)]}."""
    year = config.CURRENT_YEAR
    return {
        "name_basics": [
            ("death_before_birth", "deathYear < birthYear",
             F.col("deathYear") < F.col("birthYear")),
            ("birth_in_future", f"birthYear > {year}",
             F.col("birthYear") > year),
            ("death_in_future", f"deathYear > {year}",
             F.col("deathYear") > year),
            ("lifespan_over_120",
             "deathYear - birthYear > 120, or alive and born > 120 years ago",
             ((F.col("deathYear") - F.col("birthYear")) > 120)
             | (F.col("deathYear").isNull()
                & (F.col("birthYear") < year - 120))),
        ],
        "title_basics": [
            ("end_before_start", "endYear < startYear",
             F.col("endYear") < F.col("startYear")),
            ("start_in_future", f"startYear > {year} (announced titles)",
             F.col("startYear") > year),
            ("runtime_non_positive", "runtimeMinutes <= 0",
             F.col("runtimeMinutes") <= 0),
            ("runtime_over_1000", "runtimeMinutes > 1000",
             F.col("runtimeMinutes") > 1000),
            ("invalid_is_adult", "isAdult not in (0, 1)",
             ~F.coalesce(F.col("isAdult").isin(0, 1), F.lit(False))),
        ],
        "title_ratings": [
            ("rating_out_of_range", "averageRating not in [1, 10]",
             ~F.col("averageRating").between(1, 10)),
            ("votes_non_positive", "numVotes <= 0",
             F.col("numVotes") <= 0),
        ],
        "title_episode": [
            ("missing_season_number", "seasonNumber is null",
             F.col("seasonNumber").isNull()),
        ],
    }


# (rule, child table, child column, parent table, parent column, is_array)
FK_RULES = [
    ("ratings_orphan_title", "title_ratings", "tconst",
     "title_basics", "tconst", False),
    ("principals_orphan_title", "title_principals", "tconst",
     "title_basics", "tconst", False),
    ("principals_orphan_person", "title_principals", "nconst",
     "name_basics", "nconst", False),
    ("crew_orphan_title", "title_crew", "tconst",
     "title_basics", "tconst", False),
    ("crew_orphan_director", "title_crew", "directors",
     "name_basics", "nconst", True),
    ("crew_orphan_writer", "title_crew", "writers",
     "name_basics", "nconst", True),
    ("episode_orphan_title", "title_episode", "tconst",
     "title_basics", "tconst", False),
    ("episode_orphan_parent", "title_episode", "parentTconst",
     "title_basics", "tconst", False),
    ("akas_orphan_title", "title_akas", "titleId",
     "title_basics", "tconst", False),
    ("known_for_orphan_title", "name_basics", "knownForTitles",
     "title_basics", "tconst", True),
]


def orphan_keys(child_df, column, parent_df, parent_column, is_array):
    """Distinct values of ``child_df.column`` missing in the parent table.

    Working on distinct keys (a few million values) instead of full rows
    (up to ~100 million for title.principals) keeps the shuffle small.
    """
    values = (
        F.explode(column) if is_array else F.col(column)
    ).alias("__value")
    keys = child_df.select(values).where(F.col("__value").isNotNull())
    parents = parent_df.select(F.col(parent_column).alias("__value"))
    return keys.distinct().join(parents.distinct(), "__value", "left_anti")


def fk_orphans(tables, child, column, parent, parent_column, is_array):
    """Rows (or exploded array values) of ``child`` without a parent row."""
    missing = orphan_keys(
        tables[child], column, tables[parent], parent_column, is_array
    )
    df = tables[child]
    if is_array:
        df = df.select("*", F.explode(column).alias("__value"))
    else:
        df = df.withColumn("__value", F.col(column))
    # The set of missing keys is usually tiny: adaptive query execution
    # turns this into a broadcast join at run time.
    return df.join(missing, "__value", "left_semi")


def cross_rules(tables):
    """Rules that need several tables: [(rule, table, description, df)]."""
    names = tables["name_basics"].select("nconst", "birthYear", "deathYear")
    titles = tables["title_basics"].select(
        "tconst", "titleType", "startYear"
    )
    credits = (
        tables["title_principals"]
        .where(~F.col("category").isin(ARCHIVE_CATEGORIES))
        .join(names, "nconst")
        .join(titles, "tconst")
    )
    parent = titles.select(
        F.col("tconst").alias("parentTconst"),
        F.col("titleType").alias("parentType"),
        F.col("startYear").alias("parentStart"),
    )
    episodes = (
        tables["title_episode"]
        .join(titles, "tconst")
        .join(parent, "parentTconst")
    )
    return [
        ("credited_before_birth", "title_principals",
         "title startYear < person birthYear (archive footage excluded)",
         credits.where(F.col("startYear") < F.col("birthYear"))),
        ("credited_long_after_death", "title_principals",
         "title startYear > deathYear + 5 (archive footage excluded)",
         credits.where(F.col("startYear") > F.col("deathYear") + 5)),
        ("episode_before_series", "title_episode",
         "episode startYear < parent series startYear",
         episodes.where(F.col("startYear") < F.col("parentStart"))),
        ("episode_parent_not_series", "title_episode",
         "parent title is not a tvSeries / tvMiniSeries",
         episodes.where(~F.col("parentType").isin(SERIES_TYPES))),
        ("episode_not_tv_episode", "title_episode",
         "episode tconst whose titleType is not tvEpisode",
         episodes.where(F.col("titleType") != "tvEpisode")),
    ]


def duplicate_keys(df, keys):
    """Rows whose primary key appears more than once."""
    return (
        df.groupBy(*keys).count().where(F.col("count") > 1)
    )
