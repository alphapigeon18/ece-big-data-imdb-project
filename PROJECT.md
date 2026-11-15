# IMDb big data project - execution and operations guide

## Contributors

| Name | Email | Git username |
| --- | --- | --- |
| Romain NOM | `<email>` | `<github-username>` |
| Mathis Groult | `<email>` | `<github-username>` |

## Technology stack

- Apache Spark 4 (PySpark batch and Structured Streaming), run in the
  `quay.io/jupyter/pyspark-notebook` image
- Apache Kafka 4 (`apache/kafka-native`), KRaft single node
- Docker Compose
- Sources: [IMDb non-commercial datasets](https://data.imdb.com/non-commercial-datasets/),
  Wikidata and English Wikipedia (enrichment), Wikimedia EventStreams
  (streaming)

## Repository layout

```text
compose.yaml            Kafka + Spark containers
requirements.txt        extra Python packages for the Spark container
scripts/                container set-up, download, run-all, smoke test,
                        report update
jobs/common/            shared code (Spark session, I/O, schemas, rules)
jobs/preparation/       null profile, staging, quality checks, cleaning
jobs/exploration/       questions Q1 to Q8
jobs/enrichment/        Wikipedia enrichment (select, fetch, build)
jobs/streaming/         Kafka producer and Structured Streaming job
jobs/open_question/     director-actor partnerships
tests/                  synthetic dataset generator
outputs/                result tables (CSV + Markdown) used in REPORT.md
```

Data never enters the repository: it lives in the `imdb_data` Docker
volume mounted on `data/`.

## 1. Environment set-up

Prerequisites: Docker Desktop (or Docker Engine + Compose v2), 8 GB of RAM
available for Docker, about 15 GB of free disk space, Internet access.

```bash
git clone <repository-url>
cd <repository>
docker compose up -d
docker exec imdb_spark bash scripts/setup_container.sh
```

`setup_container.sh` makes PySpark importable from `docker exec` (the
Jupyter image only exposes it to the notebook server) and installs
`requirements.txt`. Run it again whenever the container is recreated.

The Spark container is called `imdb_spark` and its working directory is
the repository root. All commands below are run from the host and work in
PowerShell, Git Bash, macOS and Linux terminals.

Optional settings (environment variables of the container, see
`jobs/common/config.py`): `SPARK_DRIVER_MEMORY` (default `4g`, set it in
the host shell before `docker compose up`), `MIN_VOTES`, `TOP_N`,
`N_PRINCIPALS`, `LOOKUP_MIN_VOTES`.

### Smoke test (optional, a few minutes)

Runs the whole batch pipeline on a small synthetic dataset with the IMDb
layout, without touching the real data or `outputs/`:

```bash
docker exec -it imdb_spark bash scripts/smoke_test.sh
```

## 2. Batch pipeline

```bash
# 1. Download the 7 IMDb files (about 1.5 GB) into data/raw
docker exec -it imdb_spark python scripts/download_imdb.py

# 2. Data preparation: null profile, typed Parquet, quality report, cleaning
docker exec -it imdb_spark bash scripts/run_batch.sh preparation

# 3. Exploration questions Q1-Q8
docker exec -it imdb_spark bash scripts/run_batch.sh exploration

# 4. Open question
docker exec -it imdb_spark bash scripts/run_batch.sh open_question
```

`bash scripts/run_batch.sh` without argument runs every step in order.
Each job writes its result to `outputs/<section>/<name>/` (CSV) and
`outputs/<section>/<name>.md` (preview), and `scripts/update_report.py`
copies the previews into `REPORT.md`.

## 3. Data enrichment

```bash
# Select the top 10 films per genre and year (1980-2025) + 3 principals each
docker exec -it imdb_spark python jobs/enrichment/select_targets.py

# Fetch synopses and biographies (Wikidata + Wikipedia, rate limited).
# CONTACT_EMAIL is mandatory (Wikimedia User-Agent policy). The fetch can be
# interrupted and restarted: already fetched ids are skipped.
docker exec -it -e CONTACT_EMAIL=<your email> imdb_spark \
    python jobs/enrichment/fetch_wikipedia.py

# Clean, type and join the fetched records with Spark
docker exec -it imdb_spark python jobs/enrichment/build_enriched.py
```

For a quick test, add `--limit-films 50 --limit-people 150` to the fetch.

## 4. Streaming pipeline

```bash
# 1. Build the IMDb lookup table (titles and people to look for)
docker exec -it imdb_spark python jobs/streaming/build_lookup.py

# 2. Terminal A - start the Spark Structured Streaming job (25 minutes)
docker exec -it imdb_spark python jobs/streaming/stream_imdb_matches.py \
    --minutes 25 --reset

# 3. Terminal B - publish Wikipedia changes to Kafka (22 minutes),
#    replaying the last 15 minutes first to generate late events
docker exec -it -e CONTACT_EMAIL=<your email> imdb_spark \
    python jobs/streaming/producer.py --minutes 22 --since-minutes 15

# 4. After the run: metrics and tables for the report
docker exec -it imdb_spark python jobs/streaming/summarize_stream.py
docker exec -it imdb_spark python scripts/update_report.py
```

Kafka is reachable as `kafka:19092` from the containers and as
`localhost:9092` from the host. The topic `wiki-changes` is created by the
producer.

## 5. Stopping and cleaning

```bash
docker compose down        # stop the containers, keep the data volume
docker compose down -v     # also delete the data volume (IMDb files)
```

## Declaration of AI / LLM tools

| Tool | Used for |
| --- | --- |
| Claude (Anthropic, Claude Opus 5.5, claude.ai) | Generating a first version of the Python / PySpark code of every job, `compose.yaml`, the shell scripts and the synthetic test data generator; debugging the runs (memory issues, Kafka connector download); suggesting the split of the work into Git branches; drafting the methodology explanations of `REPORT.md`, with the professor's authorisation, reviewed and edited by the team |

All generated code was reviewed, run and, where needed, corrected by the
team before being committed through reviewed pull requests.
