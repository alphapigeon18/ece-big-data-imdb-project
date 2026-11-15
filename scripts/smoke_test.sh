#!/usr/bin/env bash
# Run the batch pipeline end-to-end on a small synthetic dataset (about one
# minute). Nothing is written to outputs/ or to the real data folders.
#   docker exec -it imdb_spark bash scripts/smoke_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export DATA_DIR="data/smoke"
export RAW_DIR="data/smoke/raw"
export OUTPUT_DIR="data/smoke/outputs"
export SPARK_SHUFFLE_PARTITIONS=8 SPARK_DRIVER_MEMORY=2g

python tests/make_sample_data.py --out "$RAW_DIR"
for job in \
    jobs/preparation/profile_nulls.py jobs/preparation/stage.py \
    jobs/preparation/quality_checks.py jobs/preparation/clean.py \
    jobs/exploration/q*.py \
    jobs/enrichment/select_targets.py \
    jobs/streaming/build_lookup.py \
    jobs/open_question/build_collaborations.py \
    jobs/open_question/analyze_collaborations.py; do
  [[ -f "$job" ]] || continue
  echo "=== $job"
  python "$job" > "$DATA_DIR/last_job.log" 2>&1 \
    || { tail -40 "$DATA_DIR/last_job.log"; exit 1; }
  grep -E "^\[saved\]" "$DATA_DIR/last_job.log" || true
done
echo "smoke test OK - results in $OUTPUT_DIR"
