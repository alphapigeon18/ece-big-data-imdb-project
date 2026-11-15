"""Data preparation - step 3: data quality report.

Runs every rule of ``common/quality.py`` on the staged (typed, not yet
cleaned) tables and reports how many rows break each rule, with examples.

Output: outputs/preparation/quality_issues(.md)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from common import quality  # noqa: E402
from common.io_utils import read_table, save_result  # noqa: E402
from common.schemas import IMDB_FILES, PRIMARY_KEYS  # noqa: E402
from common.spark import get_spark  # noqa: E402

SECTION = "preparation"
N_EXAMPLES = 5


def examples(df, key):
    """Return up to N_EXAMPLES key values as a comma separated string."""
    rows = df.select(F.col(key).cast("string")).limit(N_EXAMPLES).collect()
    return ", ".join(r[0] for r in rows if r[0] is not None)


def main():
    spark = get_spark("prep-quality-checks")
    tables = {t: read_table(spark, t, layer="staged") for t in IMDB_FILES}
    sizes = {t: df.count() for t, df in tables.items()}
    results = []

    def record(rule, table, kind, description, bad, key):
        n_bad = bad.count()
        pct = 100.0 * n_bad / sizes[table] if sizes[table] else 0.0
        results.append((
            rule, table, kind, description, n_bad, round(pct, 4),
            examples(bad, key) if n_bad else "",
        ))
        print(f"{rule:32s} {n_bad:>12,d}")

    for table, rules in quality.row_rules().items():
        key = PRIMARY_KEYS[table][0]
        for rule, description, condition in rules:
            record(rule, table, "row", description,
                   tables[table].where(condition), key)

    for rule, child, col, parent, pcol, is_array in quality.FK_RULES:
        bad = quality.fk_orphans(tables, child, col, parent, pcol, is_array)
        what = "values" if is_array else "rows"
        record(rule, child, "foreign_key",
               f"{what} of {child}.{col} missing in {parent}.{pcol}",
               bad, "__value")

    for rule, table, description, bad in quality.cross_rules(tables):
        record(rule, table, "cross_table", description, bad, "tconst")

    for table, keys in PRIMARY_KEYS.items():
        dup = quality.duplicate_keys(tables[table], keys)
        record(f"duplicate_{table}_key", table, "duplicate",
               f"primary key ({', '.join(keys)}) not unique", dup, keys[0])

    report = spark.createDataFrame(
        results,
        "rule string, table string, kind string, description string, "
        "n_rows long, pct_of_table double, examples string",
    )
    save_result(report, SECTION, "quality_issues", md_rows=100,
                title="Data quality issues (staged layer)")
    spark.stop()


if __name__ == "__main__":
    main()
