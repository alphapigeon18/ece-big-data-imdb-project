"""Data preparation - step 2: convert the raw TSV files to typed Parquet.

Gzipped TSV files cannot be split, so every scan of a raw file is single
threaded. Converting them once to Parquet (columnar, splittable, typed) makes
all following jobs much faster. ``\\N`` markers become real nulls and list
columns become arrays; no row is modified or removed at this stage.

Output: data/staged/<table>/ (Parquet)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402
from common.io_utils import read_table, write_parquet  # noqa: E402
from common.schemas import IMDB_FILES  # noqa: E402
from common.spark import get_spark  # noqa: E402


def main():
    spark = get_spark("prep-stage")
    config.STAGED_DIR.mkdir(parents=True, exist_ok=True)
    for table in IMDB_FILES:
        print(f"staging {table}...")
        df = read_table(spark, table, layer="raw")
        write_parquet(df, config.STAGED_DIR / table, partitions=16)
    spark.stop()


if __name__ == "__main__":
    main()
