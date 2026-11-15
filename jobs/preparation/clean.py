"""Data preparation - step 4: build the clean layer used by all analyses.

Handling strategy (see REPORT.md for the justification):

* Values that are impossible but whose row is still useful are set to null
  and the row is tagged in a ``dq_flags`` array column (people and titles).
* Suspicious but possible values are only tagged (``dq_flags``).
* Rows of child tables whose foreign key has no parent are moved to
  ``data/quarantine/<table>`` instead of being silently deleted.
* Orphan identifiers inside list columns (crew, knownForTitles) are removed
  from the list.
* Duplicate primary keys are removed (first occurrence kept).

Outputs:
    data/clean/<table>/                 cleaned Parquet
    data/quarantine/<table>/            rejected rows with the reason
    outputs/preparation/cleaning_summary(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import config, quality  # noqa: E402
from common.io_utils import read_table, save_result, write_parquet  # noqa
from common.schemas import IMDB_FILES, PRIMARY_KEYS  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "preparation"


def flags_column(rules):
    """Array of the names of the rules a row breaks (empty if none)."""
    return F.array_compact(
        F.array(*[F.when(cond, F.lit(name)) for name, _, cond in rules])
    )


def keep_valid_ids(df, keys, array_col, parent_ids):
    """Remove from ``array_col`` the identifiers missing in ``parent_ids``."""
    exploded = df.select(
        *keys, F.posexplode(array_col).alias("__pos", "__id")
    )
    valid = exploded.join(
        parent_ids, exploded["__id"] == parent_ids["__pid"], "left_semi"
    )
    regrouped = valid.groupBy(*keys).agg(
        F.array_sort(F.collect_list(F.struct("__pos", "__id")))
        .alias("__pairs")
    ).select(*keys, F.col("__pairs.__id").alias("__clean"))
    return (
        df.join(regrouped, keys, "left")
        .withColumn(array_col, F.col("__clean"))
        .drop("__clean")
    )


def split_orphans(df, checks, staged):
    """Split ``df`` into (valid rows, quarantined rows with a reason).

    Missing parent keys are computed on distinct values first (small
    shuffle), then the rows are split with a semi / anti join on that
    small set of keys.
    """
    rejected = None
    valid = df
    for rule, column, parent, parent_column in checks:
        missing = quality.orphan_keys(
            valid, column, staged[parent], parent_column, False
        ).withColumnRenamed("__value", column).cache()
        bad = valid.join(missing, column, "left_semi").withColumn(
            "dq_reason", F.lit(rule)
        )
        valid = valid.join(missing, column, "left_anti")
        rejected = bad if rejected is None else rejected.unionByName(bad)
    return valid, rejected


def clean_names(staged):
    df = staged["name_basics"]
    rules = quality.row_rules()["name_basics"]
    titles = staged["title_basics"].select(F.col("tconst").alias("__pid"))

    # People credited on a title released before their birth year.
    before_birth = [
        r for r in quality.cross_rules(staged)
        if r[0] == "credited_before_birth"
    ][0][3].select("nconst").distinct().withColumn("__cbb", F.lit(True))

    df = (
        df.join(before_birth, "nconst", "left")
        .withColumn("dq_flags", F.concat(
            flags_column(rules),
            F.when(F.col("__cbb"), F.array(F.lit("credited_before_birth")))
            .otherwise(F.array().cast("array<string>")),
        ))
        .drop("__cbb")
    )
    impossible = F.col("deathYear") < F.col("birthYear")
    df = (
        df.withColumn(
            "birthYear",
            F.when(impossible | (F.col("birthYear") > config.CURRENT_YEAR),
                   None).otherwise(F.col("birthYear")),
        )
        .withColumn(
            "deathYear",
            F.when(impossible | (F.col("deathYear") > config.CURRENT_YEAR),
                   None).otherwise(F.col("deathYear")),
        )
    )
    return keep_valid_ids(df, ["nconst"], "knownForTitles", titles), None


def clean_titles(staged):
    df = staged["title_basics"]
    rules = quality.row_rules()["title_basics"]
    df = df.withColumn("dq_flags", flags_column(rules))
    df = (
        df.withColumn(
            "endYear",
            F.when(F.col("endYear") < F.col("startYear"), None)
            .otherwise(F.col("endYear")),
        )
        .withColumn(
            "runtimeMinutes",
            F.when(F.col("runtimeMinutes") <= 0, None)
            .otherwise(F.col("runtimeMinutes")),
        )
        .withColumn(
            "isAdult",
            F.when(F.col("isAdult").isin(0, 1), F.col("isAdult")),
        )
    )
    return df, None


def clean_ratings(staged):
    df = staged["title_ratings"]
    rules = quality.row_rules()["title_ratings"]
    invalid = F.lit(False)
    for _, _, cond in rules:
        invalid = invalid | F.coalesce(cond, F.lit(True))
    bad_values = df.where(invalid).withColumn(
        "dq_reason", F.lit("invalid_rating_values")
    )
    valid, orphans = split_orphans(
        df.where(~invalid),
        [("ratings_orphan_title", "tconst", "title_basics", "tconst")],
        staged,
    )
    return valid, orphans.unionByName(bad_values)


def clean_principals(staged):
    return split_orphans(
        staged["title_principals"],
        [
            ("principals_orphan_title", "tconst", "title_basics", "tconst"),
            ("principals_orphan_person", "nconst", "name_basics", "nconst"),
        ],
        staged,
    )


def clean_crew(staged):
    valid, rejected = split_orphans(
        staged["title_crew"],
        [("crew_orphan_title", "tconst", "title_basics", "tconst")],
        staged,
    )
    people = staged["name_basics"].select(F.col("nconst").alias("__pid"))
    for column in ["directors", "writers"]:
        valid = keep_valid_ids(valid, ["tconst"], column, people)
    return valid, rejected


def clean_episodes(staged):
    return split_orphans(
        staged["title_episode"],
        [
            ("episode_orphan_title", "tconst", "title_basics", "tconst"),
            ("episode_orphan_parent", "parentTconst", "title_basics",
             "tconst"),
        ],
        staged,
    )


def clean_akas(staged):
    return split_orphans(
        staged["title_akas"],
        [("akas_orphan_title", "titleId", "title_basics", "tconst")],
        staged,
    )


CLEANERS = {
    "name_basics": clean_names,
    "title_basics": clean_titles,
    "title_ratings": clean_ratings,
    "title_principals": clean_principals,
    "title_crew": clean_crew,
    "title_episode": clean_episodes,
    "title_akas": clean_akas,
}


def main():
    spark = get_spark("prep-clean")
    staged = {
        t: read_table(spark, t, layer="staged").dropDuplicates(
            PRIMARY_KEYS[t]
        )
        for t in IMDB_FILES
    }
    summary = []
    for table, cleaner in CLEANERS.items():
        print(f"cleaning {table}...")
        n_in = read_table(spark, table, layer="staged").count()
        clean_df, rejected = cleaner(staged)
        write_parquet(clean_df, config.CLEAN_DIR / table, partitions=16)
        n_rejected = 0
        if rejected is not None:
            write_parquet(rejected, config.QUARANTINE_DIR / table)
            n_rejected = spark.read.parquet(
                str(config.QUARANTINE_DIR / table)
            ).count()
        written = spark.read.parquet(str(config.CLEAN_DIR / table))
        n_flagged = (
            written.where(F.size("dq_flags") > 0).count()
            if "dq_flags" in written.columns else 0
        )
        summary.append(
            (table, n_in, written.count(), n_rejected, n_flagged)
        )

    report = spark.createDataFrame(
        summary,
        "table string, rows_staged long, rows_clean long, "
        "rows_quarantined long, rows_flagged long",
    ).withColumn(
        "duplicates_removed",
        F.col("rows_staged") - F.col("rows_clean") - F.col("rows_quarantined"),
    )
    save_result(report, SECTION, "cleaning_summary",
                title="Cleaning summary")
    spark.stop()


if __name__ == "__main__":
    main()
