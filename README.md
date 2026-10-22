# Bluesky Streamhouse

A streaming lakehouse deployed on a VPS — the live Bluesky firehose ingested through Redpanda into two pipelines, one on DuckDB and DuckLake, the other on Spark and Iceberg, with the same Bronze / Silver / Gold models and tests.

**Project page** → [julien-castellano.fr/projets/bluesky-streamhouse](https://julien-castellano.fr/projets/bluesky-streamhouse)

## Architecture

```
Bluesky Jetstream (WebSocket)
        ↓
Redpanda — raw_events topic, shared by both branches
        ↓
one branch per day, alternating
        ↓                                        ↓
DuckDB branch                                    Spark branch
        ↓                                        ↓
Bronze — Quix Streams → DuckLake                 Bronze — Spark Structured Streaming → Iceberg
        ↓                                        ↓
Silver — dbt + DuckDB                            Silver — dbt + Spark (Thrift server)
        ↓                                        ↓
Gold   — dbt + DuckDB                            Gold   — dbt + Spark (Thrift server)
        ↓                                        ↓
        └──── FastAPI — live and rolling metrics of both branches ────┘
```

Data files on Garage (S3-compatible); catalogs in Postgres (DuckLake) and Lakekeeper (Iceberg REST). Orchestrated by **Dagster**, one code location per branch: Silver/Gold every 15 minutes, nightly maintenance and tests.

## Stack

| Layer | DuckDB branch | Spark branch |
|---|---|---|
| Streaming | Quix Streams | Spark Structured Streaming |
| Table format | DuckLake | Apache Iceberg |
| Catalog | Postgres | Lakekeeper (Iceberg REST) |
| Transformation | dbt-duckdb | dbt-spark via Spark Thrift server |

| Shared | Tool |
|---|---|
| Source | Bluesky Jetstream |
| Message broker | Redpanda |
| Storage | Garage (S3-compatible) |
| Orchestration | Dagster |
| Serving | FastAPI |
| Metrics | cgroups v2 collector, Postgres |
| Packaging | uv |
| CI/CD | GitHub Actions, Coolify |

## Project structure

```
src/
├── config.py                   # Centralized settings
├── ingestion/
│   └── producer.py             # Jetstream → Redpanda raw_events
├── processing/
│   ├── bronze.py               # Bronze contract, shared by both branches
│   ├── quix/app.py             # DuckDB branch: Quix Streams → DuckLake Bronze
│   └── spark/
│       ├── stream_job.py       # Spark branch: Structured Streaming → Iceberg Bronze
│       └── thrift_server.py    # Spark Thrift server, the engine of dbt-spark
├── alternation/                # Daily calendar, engine supervisor, day completeness
├── benchmark/                  # Metrics collector, replay, parity, reset
├── maintenance/
│   ├── ducklake.py             # Retention, CHECKPOINT
│   └── iceberg.py              # Retention, compaction, snapshot expiry, orphan files
├── resources/                  # DuckLake, Redpanda, S3, Spark and Thrift clients
├── dagster/
│   ├── definitions.py          # DuckDB branch code location
│   └── spark_definitions.py    # Spark branch code location
└── serving/
    └── api.py                  # FastAPI — live and rolling metrics of both branches

dbt/
├── duckdb/                     # dbt-duckdb project
└── spark/                      # dbt-spark project, same models and tests
    models/
    ├── silver/                 # posts, likes, reposts, follows, deletes (incremental, deduplicated)
    └── gold/                   # activity per minute, languages, hashtags, active users, engagement
    tests/                      # Silver = deduplicated Bronze, Gold = Silver, AT-URI checks

docker/
├── producer/  quix/  spark/    # Streaming services
├── dagster/  dagster-spark/    # Dagster code servers (gRPC, port 4000), Spark Thrift server
├── lakekeeper/                 # Iceberg catalog setup (warehouse on Garage)
├── api/                        # FastAPI serving (port 8000)
├── postgres/                   # Local databases (init.sql)
└── collector/                  # Metrics collector
```

## Local development

`make up` needs a `.env` with the local credentials read by `docker-compose.dev.yaml`: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `BUCKET`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `DUCKLAKE_CATALOG_DB`, `GARAGE_RPC_SECRET`, `LAKEKEEPER_ENCRYPTION_KEY`, `REDPANDA_CONSOLE_PORT`.

```bash
make install              # uv sync --all-groups
make up                   # Redpanda, Garage, Postgres, Lakekeeper
make produce              # Jetstream → Redpanda
make quix                 # DuckDB branch streaming
make spark                # Spark branch streaming (container)
make spark-thrift         # Spark Thrift server on localhost:10000
make dbt-build            # DuckDB branch Silver/Gold
make dbt-spark-build      # Spark branch Silver/Gold
make iceberg-maintenance  # Iceberg maintenance, once
make dagster              # Dagster UI (DuckDB branch code location)
make api                  # Dashboard API on localhost:8000
```

## Tests

```bash
# Lint and format
uv run ruff check .
uv run ruff format --check .

# Unit tests (the local-stack ones are skipped when `make up` is not running)
uv run pytest

# dbt data quality tests, both branches
make dbt-build
make dbt-spark-build
```

## CI

GitHub Actions runs on every push and pull request to `main` and `dev` (`.github/workflows/ci.yml`).

| Step | Command |
|---|---|
| Lint | `uv run ruff check .` |
| Format | `uv run ruff format --check .` |
| dbt parse (both branches) | `uv run dbt parse` |
| Unit tests | `uv run pytest` |
| Dagster definitions (both branches) | `uv run dagster definitions validate` |
| Lock file integrity | `uv lock --check` |

The `deploy` job (Coolify webhook) only triggers on push to `main` and is gated on the `lint-and-test` job.

## Deployment

Deployed on a VPS via Coolify with automatic deployment on push to `main` (GitHub Actions).
