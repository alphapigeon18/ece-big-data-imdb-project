"""Reading IMDb tables and writing analysis results."""

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

from common import config
from common.schemas import COLUMNS, IMDB_FILES, TYPES

IMDB_NULL = "\\N"


def read_raw(spark, table):
    """Read one raw IMDb TSV with every column as a string.

    Quoting is disabled because IMDb titles contain unbalanced double quotes.
    The literal ``\\N`` marker is kept so it can be profiled.
    """
    path = str(config.RAW_DIR / IMDB_FILES[table])
    df = (
        spark.read.option("sep", "\t")
        .option("header", "true")
        .option("quote", "\u0000")
        .option("inferSchema", "false")
        .csv(path)
    )
    expected = COLUMNS[table]
    if df.columns != expected:
        raise ValueError(
            f"{table}: unexpected columns {df.columns}, expected {expected}"
        )
    return df


def missing_condition(column):
    """Boolean column: value is null, the IMDb ``\\N`` marker or blank."""
    c = F.col(column)
    return c.isNull() | (c == IMDB_NULL) | (F.trim(c) == "")


def cast_table(df, table):
    """Replace missing markers by real nulls and cast to the target types."""
    out = []
    for column in df.columns:
        value = F.when(missing_condition(column), None).otherwise(
            F.col(column)
        )
        target = TYPES[table].get(column, "string")
        if target == "array":
            value = F.split(value, ",")
        elif target != "string":
            value = value.cast(target)
        out.append(value.alias(column))
    return df.select(*out)


def read_table(spark, table, layer="clean"):
    """Read a typed table from the best available layer.

    Layers, from most to least refined:

    * ``clean``  - Parquet written by ``jobs/preparation/clean.py``
    * ``staged`` - typed Parquet written by ``jobs/preparation/stage.py``
    * ``raw``    - the TSV, typed on the fly (slow, gzip is not splittable)

    Asking for a layer returns that layer or the closest less refined one
    that exists, so every job can run before the preparation step.
    """
    order = ["clean", "staged", "raw"]
    dirs = {"clean": config.CLEAN_DIR, "staged": config.STAGED_DIR}
    for candidate in order[order.index(layer):]:
        if candidate == "raw":
            return cast_table(read_raw(spark, table), table)
        path = dirs[candidate] / table
        if path.exists():
            return spark.read.parquet(str(path))
    raise RuntimeError("unreachable")


def write_parquet(df, path, partitions=None):
    """Overwrite a Parquet dataset, optionally repartitioned."""
    if partitions:
        df = df.repartition(partitions)
    df.write.mode("overwrite").parquet(str(path))


def _flatten_for_csv(df):
    """Convert nested columns to strings so the frame can be written as CSV."""
    cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            cols.append(
                F.concat_ws(",", F.col(field.name).cast("array<string>"))
                .alias(field.name)
            )
        elif isinstance(field.dataType, (StructType, MapType)):
            cols.append(F.to_json(F.col(field.name)).alias(field.name))
        else:
            cols.append(F.col(field.name))
    return df.select(*cols)


def _markdown_table(rows, columns):
    def fmt(value):
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.3f}"
        return str(value).replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(fmt(row[c]) for c in columns) + " |")
    return "\n".join(lines)


def save_result(df, section, name, md_rows=50, title=None):
    """Persist a result as CSV (all rows) and a Markdown preview.

    The CSV folder ``outputs/<section>/<name>/`` contains a single part file.
    ``outputs/<section>/<name>.md`` contains the first ``md_rows`` rows, ready
    to be pasted into REPORT.md.
    """
    out_dir = config.OUTPUT_DIR / section
    out_dir.mkdir(parents=True, exist_ok=True)
    flat = _flatten_for_csv(df).cache()
    total = flat.count()
    (
        flat.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(out_dir / name))
    )
    rows = flat.limit(md_rows).collect()
    header = f"### {title or name}\n\n" if title else ""
    shown = min(total, md_rows)
    body = _markdown_table(rows, flat.columns)
    Path(out_dir / f"{name}.md").write_text(
        f"{header}{body}\n\n_{shown} of {total} rows shown._\n",
        encoding="utf-8",
    )
    flat.unpersist()
    print(f"[saved] {section}/{name}: {total} rows")
    return total
