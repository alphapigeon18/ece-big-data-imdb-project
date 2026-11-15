#!/usr/bin/env bash
# Run every batch job of the project, in order, inside the Spark container:
#   docker exec -it imdb_spark bash scripts/run_batch.sh            # all
#   docker exec -it imdb_spark bash scripts/run_batch.sh exploration
# Sections: download preparation exploration enrichment lookup open_question
set -euo pipefail
cd "$(dirname "$0")/.."

# Jobs are launched with "python" (not spark-submit) so that the Spark
# configuration set in common/spark.py, including the driver memory read
# from SPARK_DRIVER_MEMORY, is applied when the JVM starts.
run() {
  echo
  echo "=== $1 ($(date +%H:%M:%S))"
  python "$@"
}

section="${1:-all}"
want() { [[ "$section" == "all" || "$section" == "$1" ]]; }

if want download; then
  python scripts/download_imdb.py
fi

if want preparation; then
  run jobs/preparation/profile_nulls.py
  run jobs/preparation/stage.py
  run jobs/preparation/quality_checks.py
  run jobs/preparation/clean.py
fi

if want exploration; then
  for job in jobs/exploration/q*.py; do
    run "$job"
  done
fi

if want enrichment; then
  run jobs/enrichment/select_targets.py
  if [[ -z "${CONTACT_EMAIL:-}" ]]; then
    echo "CONTACT_EMAIL is not set: skipping the Wikipedia fetch." >&2
  else
    python jobs/enrichment/fetch_wikipedia.py ${FETCH_ARGS:-}
  fi
  run jobs/enrichment/build_enriched.py
fi

if want lookup; then
  run jobs/streaming/build_lookup.py
fi

if want open_question; then
  run jobs/open_question/build_collaborations.py
  run jobs/open_question/analyze_collaborations.py
fi

python scripts/update_report.py
echo "done."
