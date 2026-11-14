"""Project-wide paths and tunable parameters.

Every path can be overridden with an environment variable so that the same
code runs inside the Docker container, on a laptop or on a cluster.
"""

import os
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(
    os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2])
)
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_ROOT / "data"))

RAW_DIR = Path(os.environ.get("RAW_DIR", DATA_DIR / "raw"))
STAGED_DIR = Path(os.environ.get("STAGED_DIR", DATA_DIR / "staged"))
CLEAN_DIR = Path(os.environ.get("CLEAN_DIR", DATA_DIR / "clean"))
QUARANTINE_DIR = Path(
    os.environ.get("QUARANTINE_DIR", DATA_DIR / "quarantine")
)
ENRICH_DIR = Path(os.environ.get("ENRICH_DIR", DATA_DIR / "enrichment"))
STREAM_DIR = Path(os.environ.get("STREAM_DIR", DATA_DIR / "streaming"))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", PROJECT_ROOT / "outputs"))

CURRENT_YEAR = int(os.environ.get("CURRENT_YEAR", date.today().year))

# Analysis parameters (kept in one place so REPORT.md can reference them).
FEATURE_MIN_RUNTIME = 40          # minutes, AMPAS / BFI definition
YEAR_MIN = 1980
YEAR_MAX = 2025
MIN_VOTES = int(os.environ.get("MIN_VOTES", 100))
SUCCESS_RATING = 7.0
SUCCESS_MIN_VOTES = int(os.environ.get("SUCCESS_MIN_VOTES", 1000))
