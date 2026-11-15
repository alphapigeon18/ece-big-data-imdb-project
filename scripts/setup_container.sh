#!/usr/bin/env bash
# One-time set-up of the Spark container (re-run after "docker compose up"
# recreates it):
#   docker exec imdb_spark bash scripts/setup_container.sh
#
# The Jupyter image only exposes PySpark to the notebook server. This script
# registers $SPARK_HOME/python and py4j in the Python path with a .pth file
# so that "docker exec imdb_spark python <job>.py" can import pyspark, then
# installs the extra packages of requirements.txt.
set -euo pipefail
cd "$(dirname "$0")/.."

site_packages="$(python -c 'import site; print(site.getsitepackages()[0])')"
py4j="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip | head -n 1)"
printf '%s\n%s\n' "$SPARK_HOME/python" "$py4j" > "$site_packages/spark.pth"
echo "registered $SPARK_HOME/python and $(basename "$py4j")"

pip install --quiet -r requirements.txt
python -c "import pyspark; print('pyspark', pyspark.__version__, 'OK')"
