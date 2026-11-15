"""Enrichment - step 3 (Spark): clean, type and join the fetched records.

* Raw JSONL files are read with an explicit schema (schema enforcement:
  records that do not match are counted as corrupt and reported).
* Text is normalised (whitespace collapsed, empty strings -> null) and the
  latest record per IMDb id wins if the fetcher was run several times.
* Synopsis = plot section, or the lead section when the article has no plot
  section; a ``note`` column states programmatically what is missing.
* Every enriched row carries its lineage: source_url, api_url,
  wikidata_uri, retrieved_at and the content licence.

Outputs:
    data/enrichment/enriched_films/        Parquet
    data/enrichment/enriched_principals/   Parquet
    outputs/enrichment/coverage(.md), sample_films(.md),
    outputs/enrichment/sample_principals(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    LongType, StringType, StructField, StructType,
)

from common import config  # noqa: E402
from common.io_utils import save_result, write_parquet  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "enrichment"
LICENSE = "CC BY-SA 4.0 (Wikipedia contributors)"
RAW = config.ENRICH_DIR / "raw"


def schema(*names, longs=()):
    fields = [StructField(n, LongType() if n in longs else StringType())
              for n in names]
    return StructType(fields + [StructField("_corrupt", StringType())])


FILM_SCHEMA = schema(
    "imdb_id", "status", "wikidata_uri", "wiki_title", "source_url",
    "page_id", "revision_id", "api_url", "plot_heading", "plot_text",
    "lead_text", "retrieved_at", longs=("page_id", "revision_id"),
)
PEOPLE_SCHEMA = schema(
    "imdb_id", "status", "wikidata_uri", "wiki_title", "source_url",
    "api_url", "bio_text", "retrieved_at",
)


def read_jsonl(spark, path, struct):
    df = (
        spark.read.schema(struct)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .json(str(path))
        .cache()
    )
    corrupt = df.where(F.col("_corrupt").isNotNull()).count()
    print(f"{path.name}: {df.count()} records, {corrupt} corrupt")
    latest = Window.partitionBy("imdb_id").orderBy(F.desc("retrieved_at"))
    return (
        df.where(F.col("_corrupt").isNull() & F.col("imdb_id").isNotNull())
        .withColumn("_r", F.row_number().over(latest))
        .where("_r = 1")
        .drop("_r", "_corrupt")
    ), corrupt


def clean_text(column):
    """Collapse whitespace, strip, and turn empty strings into null."""
    text = F.trim(F.regexp_replace(F.col(column), r"\s+", " "))
    return F.when(F.length(text) > 0, text)


def note_for(status, has_text_cond, fallback_note=None):
    notes = {
        "no_wikidata_item": "No Wikidata item linked to this IMDb id.",
        "no_enwiki_article": "No English Wikipedia article for this item.",
        "page_missing": "Wikipedia article could not be retrieved.",
    }
    expr = F.when(F.col(status).isNull(), "Not fetched yet.")
    for key, text in notes.items():
        expr = expr.when(F.col(status) == key, text)
    if fallback_note is not None:
        expr = expr.when(F.col(status) == "no_plot_section", fallback_note)
    return expr.when(~has_text_cond, "Article has no usable text.")


def main():
    spark = get_spark("enrich-build")
    targets = spark.read.parquet(str(config.ENRICH_DIR / "targets_films"))
    principals = spark.read.parquet(
        str(config.ENRICH_DIR / "targets_principals")
    )
    films_raw, films_corrupt = read_jsonl(
        spark, RAW / "film_extracts.jsonl", FILM_SCHEMA)
    people_raw, people_corrupt = read_jsonl(
        spark, RAW / "people_extracts.jsonl", PEOPLE_SCHEMA)

    films = films_raw.select(
        F.col("imdb_id").alias("tconst"), "status", "wikidata_uri",
        "source_url", "api_url", "plot_heading", "retrieved_at",
        "revision_id",
        clean_text("plot_text").alias("plot"),
        clean_text("lead_text").alias("lead"),
    )
    synopsis_type = (
        F.when(F.col("plot").isNotNull(), F.concat(
            F.lit("plot section ('"), F.col("plot_heading"), F.lit("')")))
        .when(F.col("lead").isNotNull(), F.lit("lead section"))
    )
    enriched_films = (
        targets.join(films, "tconst", "left")
        .withColumn("synopsis", F.coalesce("plot", "lead"))
        .withColumn("synopsis_origin", synopsis_type)
        .withColumn("note", note_for(
            "status", F.col("synopsis").isNotNull(),
            "Article has no plot section; lead section used instead.",
        ))
        .withColumn("license", F.when(F.col("synopsis").isNotNull(),
                                      F.lit(LICENSE)))
        .select(
            "year", "genre", "rank_in_genre_year", "tconst", "title",
            "rating", "votes", "synopsis", "synopsis_origin",
            F.col("source_url").alias("lineage_source_url"),
            F.col("api_url").alias("lineage_api_url"),
            F.col("wikidata_uri").alias("lineage_wikidata_uri"),
            F.col("revision_id").alias("lineage_revision_id"),
            F.col("retrieved_at").alias("lineage_retrieved_at"),
            "license", "note",
        )
        .cache()
    )

    people = people_raw.select(
        F.col("imdb_id").alias("nconst"), "status", "wikidata_uri",
        "source_url", "api_url", "retrieved_at",
        clean_text("bio_text").alias("biography"),
    )
    enriched_people = (
        principals.join(people, "nconst", "left")
        .withColumn("note", note_for("status",
                                     F.col("biography").isNotNull()))
        .withColumn("license", F.when(F.col("biography").isNotNull(),
                                      F.lit(LICENSE)))
        .select(
            "tconst", "principal_rank", "nconst", "name", "category",
            "birthYear", "deathYear", "biography",
            F.col("source_url").alias("lineage_source_url"),
            F.col("api_url").alias("lineage_api_url"),
            F.col("wikidata_uri").alias("lineage_wikidata_uri"),
            F.col("retrieved_at").alias("lineage_retrieved_at"),
            "license", "note",
        )
        .cache()
    )

    write_parquet(enriched_films, config.ENRICH_DIR / "enriched_films", 4)
    write_parquet(enriched_people, config.ENRICH_DIR / "enriched_principals",
                  4)

    distinct_films = enriched_films.dropDuplicates(["tconst"])
    distinct_people = enriched_people.dropDuplicates(["nconst"])
    coverage = spark.createDataFrame([
        ("films", distinct_films.count(),
         distinct_films.where(F.col("synopsis").isNotNull()).count(),
         distinct_films.where(F.col("synopsis_origin").startswith("plot"))
         .count(), films_corrupt),
        ("principals", distinct_people.count(),
         distinct_people.where(F.col("biography").isNotNull()).count(),
         None, people_corrupt),
    ], "entity string, n_targets long, n_enriched long, "
       "n_with_plot_section long, n_corrupt_raw_records long").withColumn(
        "pct_enriched",
        F.round(100 * F.col("n_enriched") / F.col("n_targets"), 1),
    )
    save_result(coverage, SECTION, "coverage", title="Enrichment coverage")

    notes = (
        distinct_films.groupBy(F.lit("film").alias("entity"), "note").count()
        .unionByName(distinct_people.groupBy(
            F.lit("principal").alias("entity"), "note").count())
        .orderBy("entity", F.desc("count"))
    )
    save_result(notes, SECTION, "notes_breakdown",
                title="Why some records are not enriched")

    short = F.concat(F.substring("synopsis", 1, 200), F.lit("..."))
    save_result(
        enriched_films.where(F.col("rank_in_genre_year") == 1)
        .withColumn("synopsis", short)
        .orderBy(F.col("synopsis").isNull(), F.desc("year"), "genre")
        .limit(500),
        SECTION, "sample_films", md_rows=15,
        title="Sample of enriched films (rank 1 per genre and year)",
    )
    save_result(
        enriched_people.withColumn(
            "biography",
            F.concat(F.substring("biography", 1, 200), F.lit("...")),
        ).orderBy(F.col("biography").isNull(), "tconst", "principal_rank")
        .limit(500),
        SECTION, "sample_principals", md_rows=15,
        title="Sample of enriched principals",
    )
    spark.stop()


if __name__ == "__main__":
    main()
