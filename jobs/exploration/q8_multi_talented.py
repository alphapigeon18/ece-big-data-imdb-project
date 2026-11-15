"""Exploration Q8 - multi-talented film professionals (actor / director /
writer) and how their films perform when they hold several roles on the
same film.

* Roles come from title.principals (actor, actress, director, writer) and
  title.crew (directors, writers); actor and actress are merged.
* Multi-talented: at least two of the three roles, each held on at least
  ``MIN_FILMS_PER_ROLE`` feature films.
* For every multi-talented person, films where they held >= 2 roles
  ("multi-role") are compared with films where they held one role.

Outputs:
    outputs/exploration/q8_role_combination_performance(.md)
    outputs/exploration/q8_paired_comparison(.md)
    outputs/exploration/q8_top_multi_talented(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.films import feature_films, people_names  # noqa: E402
from common.io_utils import read_table, save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"
MIN_FILMS_PER_ROLE = 3
ROLE_MAP = {"actor": "actor", "actress": "actor", "director": "director",
            "writer": "writer"}


def credits_with_roles(spark):
    role_map = F.create_map(*[F.lit(x) for kv in ROLE_MAP.items() for x in kv])
    principals = (
        read_table(spark, "title_principals")
        .where(F.col("category").isin(list(ROLE_MAP)))
        .select("tconst", "nconst", role_map[F.col("category")].alias("role"))
    )
    crew = read_table(spark, "title_crew")
    directors = crew.select(
        "tconst", F.explode("directors").alias("nconst"),
        F.lit("director").alias("role"),
    )
    writers = crew.select(
        "tconst", F.explode("writers").alias("nconst"),
        F.lit("writer").alias("role"),
    )
    return principals.unionByName(directors).unionByName(writers).distinct()


def main():
    spark = get_spark("q8-multi-talented")
    films = feature_films(spark, min_votes=config.MIN_VOTES)
    credits = credits_with_roles(spark).join(
        films.select("tconst", "rating", "votes"), "tconst"
    )

    talents = (
        credits.groupBy("nconst", "role").agg(F.count("*").alias("n"))
        .where(F.col("n") >= MIN_FILMS_PER_ROLE)
        .groupBy("nconst").agg(
            F.sort_array(F.collect_list("role")).alias("roles")
        )
        .where(F.size("roles") >= 2)
    )

    per_film = (
        credits.join(talents.select("nconst"), "nconst")
        .groupBy("nconst", "tconst", "rating", "votes")
        .agg(F.sort_array(F.collect_set("role")).alias("film_roles"))
        .withColumn("multi_role", F.size("film_roles") >= 2)
        .withColumn("combination", F.array_join("film_roles", "+"))
        .cache()
    )

    # 1. Performance of each role combination held on the same film.
    combos = per_film.groupBy("combination").agg(
        F.countDistinct("nconst").alias("n_people"),
        F.count("*").alias("n_credits"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.round(F.expr("percentile_approx(votes, 0.5)"), 0)
        .alias("median_votes"),
    ).orderBy(F.desc("n_credits"))
    save_result(combos, SECTION, "q8_role_combination_performance",
                title="Mean rating by role combination held on one film "
                      "(multi-talented people only)")

    # 2. Paired comparison: same person, multi-role vs single-role films.
    paired = (
        per_film.groupBy("nconst").pivot("multi_role", [True, False]).agg(
            F.avg("rating").alias("rating"), F.count("rating").alias("n"),
        )
        .withColumnsRenamed({
            "true_rating": "multi_rating", "true_n": "multi_n",
            "false_rating": "single_rating", "false_n": "single_n",
        })
        .where(F.col("multi_n").isNotNull() & F.col("single_n").isNotNull())
        .withColumn("diff", F.col("multi_rating") - F.col("single_rating"))
        .cache()
    )
    summary = paired.agg(
        F.count("*").alias("n_people_with_both"),
        F.round(F.avg("diff"), 3).alias("mean_rating_diff_multi_minus_single"),
        F.round(F.expr("percentile_approx(diff, 0.5)"), 3)
        .alias("median_diff"),
        F.round(100 * F.avg((F.col("diff") > 0).cast("int")), 1)
        .alias("pct_people_better_in_multi_role"),
        F.round(F.avg("multi_rating"), 3).alias("avg_multi_role_rating"),
        F.round(F.avg("single_rating"), 3).alias("avg_single_role_rating"),
    )
    save_result(summary, SECTION, "q8_paired_comparison",
                title="Same person: multi-role films vs single-role films")

    # 3. Most prolific multi-talented people.
    top = (
        paired.join(talents, "nconst")
        .join(people_names(spark), "nconst", "left")
        .select(
            "nconst", "name", F.array_join("roles", "+").alias("roles"),
            "multi_n", F.round("multi_rating", 2).alias("multi_rating"),
            "single_n", F.round("single_rating", 2).alias("single_rating"),
            F.round("diff", 2).alias("diff"),
        )
        .orderBy(F.desc("multi_n"), F.desc("multi_rating"))
    )
    save_result(top, SECTION, "q8_top_multi_talented", md_rows=30,
                title="Multi-talented people with the most multi-role films")
    spark.stop()


if __name__ == "__main__":
    main()
