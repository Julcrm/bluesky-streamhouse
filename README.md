# Bluesky Streamhouse

A streaming lakehouse benchmark deployed on a single VPS: the live Bluesky firehose goes through two complete pipelines, one built on DuckDB and DuckLake, the other on Spark and Iceberg, with the same data contract, the same tests and the same resource budget.

**The question:** on one small node, at real traffic (~200-600 messages/s), what does Spark cost compared with a single-node engine, and from which load would it pay off?

## Architecture

```
Bluesky Jetstream (WebSocket)
        ↓
Redpanda — raw_events topic, shared by both branches
        ↓
one branch per day, 07:00-19:00, alternating
        ↓                                        ↓
DuckDB branch                                    Spark branch
        ↓                                        ↓
Bronze — Quix Streams → DuckLake                 Bronze — Spark Structured Streaming → Iceberg
        ↓                                        ↓
Silver — dbt + DuckDB                            Silver — dbt + Spark (Thrift server)
        ↓                                        ↓
Gold   — dbt + DuckDB                            Gold   — dbt + Spark (Thrift server)
        ↓                                        ↓
        └──────── same contract, same tests, Gold parity checked ────────┘
```

Data files of both branches on Garage (S3-compatible); catalogs in Postgres (DuckLake) and Lakekeeper (Iceberg REST). Every container is measured by `bench-collector` (cgroup samples every 10 s).

Orchestrated by **Dagster**, one code location per branch: Silver/Gold every 15 minutes during the branch's day, nightly maintenance and tests, and a weekly controlled test.

## How the benchmark stays fair

| Rule | How |
|---|---|
| Same data | Both branches read the same Redpanda topic; each day goes to one branch, and a frozen 20-minute sample is replayed through both every week (1×, 4×, max) |
| Same contract | Bronze columns, deduplication on `seq`, Silver/Gold models and dbt tests are identical; a parity test compares the Gold tables row by row |
| Same budget | Memory limits, 2 threads (DuckDB) against 2 cores (`local[2]`) for the transformation; limits are reported as context, the result is the measured consumption |
| No tuning during the run | Hypotheses, thresholds, code and settings are frozen by a dated commit before the 14-day measurement period; any fix restarts it |
| Clean measures | Per-message ratios by traffic level, windows flagged when a neighbour on the VPS uses the CPU or memory runs low |

Both stacks are tuned seriously (shuffle partitions, heap sizes, compaction, catalog settings): Spark is not a straw man.

## Stack

| Layer | DuckDB branch | Spark branch |
|---|---|---|
| Streaming | Quix Streams | Spark Structured Streaming |
| Table format | DuckLake | Apache Iceberg |
| Catalog | Postgres (DuckLake metadata) | Lakekeeper (Iceberg REST) |
| Transformation | dbt-duckdb | dbt-spark via Spark Thrift server |

| Shared | Tool |
|---|---|
| Source | Bluesky Jetstream |
| Message broker | Redpanda |
| Object storage | Garage (S3-compatible) |
| Orchestration | Dagster |
| Metrics | cgroups v2 collector, Postgres |
| Packaging | uv |
| CI/CD | GitHub Actions, Coolify |

## Project structure

```
src/
├── config.py                   # Centralized settings (one place for every benchmark parameter)
├── ingestion/
│   └── producer.py             # Jetstream → Redpanda raw_events
├── processing/
│   ├── bronze.py               # Bronze contract, shared by both branches
│   ├── quix/app.py             # DuckDB branch: Quix Streams → DuckLake Bronze
│   └── spark/
│       ├── stream_job.py       # Spark branch: Structured Streaming → Iceberg Bronze
│       └── thrift_server.py    # Spark Thrift server, the engine of dbt-spark
├── alternation/                # Daily calendar, engine supervisor, day completeness
├── benchmark/                  # Collector, windows, frozen sample, replay, controlled test, parity
├── maintenance/
│   ├── ducklake.py             # Retention, CHECKPOINT
│   └── iceberg.py              # Retention, compaction, snapshot expiry, orphan files
├── resources/                  # DuckLake, Redpanda, S3, Spark and Thrift clients
└── dagster/
    ├── definitions.py          # DuckDB branch code location
    └── spark_definitions.py    # Spark branch code location

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
└── collector/                  # Benchmark metrics collector
```

## Local development

```bash
make install        # uv sync --all-groups
make up             # Redpanda, Garage, Postgres, Lakekeeper
make produce        # Jetstream → Redpanda
make quix           # DuckDB branch streaming
make spark          # Spark branch streaming (container)
make spark-thrift   # Spark Thrift server on localhost:10000
make dbt-build      # DuckDB branch Silver/Gold
make dbt-spark-build
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

Deployed on a VPS (4 vCPU, 8 GB) via Coolify, with automatic deployment on push to `main`. The VPS is shared with other projects: their CPU use is measured, and windows where it is too high are excluded.

## Status

Measurement period: **22 October → 4 November 2026** (7 days per branch). Results, raw data and analysis scripts will be published here, whatever they show, along with the limits of a single-node benchmark.
