"""Streaming - step 1 (Spark batch): build the IMDb lookup table used to
recognise relevant Wikipedia pages in the live stream.

The lookup contains

* titles: feature films, TV series and mini-series with at least
  ``LOOKUP_MIN_VOTES`` votes (default 1000), keyed on the normalised
  primary title (and original title when different);
* people: actors, actresses, directors and writers credited on those titles,
  keyed on their normalised name.

When several entities share a key, the most voted one is kept and the
number of candidates is stored (``n_candidates``) so the stream job can
qualify ambiguous matches.

Output: data/streaming/lookup/ (Parquet)
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.io_utils import read_table, write_parquet  # noqa: E402
from common.spark import get_spark  # noqa: E402
from common.text import normalize_key  # noqa: E402

LOOKUP_MIN_VOTES = int(os.environ.get("LOOKUP_MIN_VOTES", 1000))
TITLE_TYPES = ["movie", "tvSeries", "tvMiniSeries"]
PERSON_CATEGORIES = ["actor", "actress", "director", "writer"]
MIN_KEY_LENGTH = 3


def main():
    spark = get_spark("stream-build-lookup")
    ratings = read_table(spark, "title_ratings").where(
        F.col("numVotes") >= LOOKUP_MIN_VOTES
    )
    titles = (
        read_table(spark, "title_basics")
        .where(F.col("titleType").isin(TITLE_TYPES))
        .join(ratings, "tconst")
        .cache()
    )
    title_keys = titles.select(
        F.explode(F.array_distinct(F.array(
            normalize_key("primaryTitle"), normalize_key("originalTitle"),
        ))).alias("key"),
        F.lit("title").alias("entity_type"),
        F.col("tconst").alias("imdb_id"),
        F.col("primaryTitle").alias("display_name"),
        F.col("titleType").alias("subtype"),
        F.col("startYear").alias("year"),
        F.col("numVotes").alias("votes"),
    )

    credits = (
        read_table(spark, "title_principals")
        .where(F.col("category").isin(PERSON_CATEGORIES))
        .join(titles.select("tconst", "numVotes"), "tconst")
        .groupBy("nconst").agg(
            F.sum("numVotes").alias("votes"),
            F.first("category").alias("subtype"),
        )
    )
    person_keys = (
        credits.join(read_table(spark, "name_basics"), "nconst")
        .select(
            normalize_key("primaryName").alias("key"),
            F.lit("person").alias("entity_type"),
            F.col("nconst").alias("imdb_id"),
            F.col("primaryName").alias("display_name"),
            "subtype",
            F.col("birthYear").alias("year"),
            "votes",
        )
    )

    by_key = Window.partitionBy("key")
    lookup = (
        title_keys.unionByName(person_keys)
        .where(F.length("key") >= MIN_KEY_LENGTH)
        .withColumn("n_candidates", F.count("*").over(by_key))
        .withColumn("_r", F.row_number().over(
            by_key.orderBy(F.desc("votes"), "imdb_id")))
        .where("_r = 1")
        .drop("_r")
    )
    write_parquet(lookup, config.STREAM_DIR / "lookup", partitions=4)
    written = spark.read.parquet(str(config.STREAM_DIR / "lookup"))
    written.groupBy("entity_type").count().show()
    spark.stop()


if __name__ == "__main__":
    main()
