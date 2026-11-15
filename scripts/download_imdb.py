"""Download the seven IMDb non-commercial TSV files into data/raw.

Usage (inside the Spark container):
    python scripts/download_imdb.py            # skip files already present
    python scripts/download_imdb.py --force    # re-download everything
"""

import argparse
import shutil
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from common import config  # noqa: E402
from common.schemas import IMDB_FILES  # noqa: E402

BASE_URL = "https://datasets.imdbws.com/"


def download(name, dest, force):
    target = dest / name
    if target.exists() and not force:
        print(f"[skip] {name} already downloaded")
        return
    tmp = target.with_suffix(target.suffix + ".part")
    url = BASE_URL + name
    print(f"[get ] {url}")
    request = urllib.request.Request(
        url, headers={"User-Agent": "ece-big-data-project/1.0"}
    )
    with urllib.request.urlopen(request) as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out, length=1024 * 1024)
    tmp.rename(target)
    print(f"[done] {name} ({target.stat().st_size / 1e6:.0f} MB)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name in IMDB_FILES.values():
        download(name, config.RAW_DIR, args.force)


if __name__ == "__main__":
    main()
