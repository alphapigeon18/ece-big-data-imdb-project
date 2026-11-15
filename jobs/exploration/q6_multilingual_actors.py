"""Exploration Q6 - actors / actresses successful in films of several
original languages.

* Successful film: feature film rated >= ``SUCCESS_RATING`` with at least
  ``SUCCESS_MIN_VOTES`` votes (see common/config.py).
* Original language: inferred from title.akas (see common/languages.py).

Outputs:
    outputs/exploration/q6_actors_2plus_languages(.md)
    outputs/exploration/q6_actors_3plus_languages(.md)
    outputs/exploration/q6_language_pairs(.md)
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
from common.languages import original_languages  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "exploration"


def main():
    spark = get_spark("q6-multilingual-actors")
    successes = (
        feature_films(spark, min_votes=config.SUCCESS_MIN_VOTES)
        .where(F.col("rating") >= config.SUCCESS_RATING)
        .join(original_languages(spark), "tconst")
    )
    credits = acting_credits(spark).join(successes, "tconst")

    per_lang = credits.groupBy("nconst", "original_language").agg(
        F.count("*").alias("n"),
        F.max(F.struct("votes", "title")).alias("top"),
    )
    per_person = (
        per_lang.groupBy("nconst").agg(
            F.count("*").alias("n_languages"),
            F.sum("n").alias("n_successful_films"),
            F.sort_array(F.collect_list("original_language"))
            .alias("languages"),
            F.sort_array(F.collect_list(F.concat(
                "original_language", F.lit(": "), F.col("top.title")
            ))).alias("best_known_film_per_language"),
        )
        .where(F.col("n_languages") >= 2)
        .join(people_names(spark), "nconst", "left")
        .select("nconst", "name", "n_languages", "n_successful_films",
                "languages", "best_known_film_per_language")
        .orderBy(F.desc("n_languages"), F.desc("n_successful_films"))
        .cache()
    )
    save_result(per_person, SECTION, "q6_actors_2plus_languages", md_rows=30,
                title="Actors with successful films in >= 2 languages")
    save_result(per_person.where(F.col("n_languages") >= 3), SECTION,
                "q6_actors_3plus_languages", md_rows=100,
                title="Actors with successful films in >= 3 languages")

    pairs = (
        per_person.select("nconst", F.explode("languages").alias("l1"))
        .join(per_person.select("nconst", F.explode("languages").alias("l2")),
              "nconst")
        .where(F.col("l1") < F.col("l2"))
        .groupBy("l1", "l2").agg(F.count("*").alias("n_actors"))
        .orderBy(F.desc("n_actors"))
    )
    save_result(pairs, SECTION, "q6_language_pairs", md_rows=20,
                title="Most common language pairs among these actors")
    spark.stop()


if __name__ == "__main__":
    main()
