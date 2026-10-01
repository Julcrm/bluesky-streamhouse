"""
Centralized configuration for the Bluesky Streamhouse project.
All URLs, topics, storage paths and parameters are defined here.
Values that differ between environments are read from environment variables,
loaded from `.env` when present (local runs; containers get real env vars).
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Bluesky Jetstream (v2 API) ---
JETSTREAM_URL = os.getenv(
    "JETSTREAM_URL",
    "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents",
)

# Collections ingested (decision D2)
JETSTREAM_COLLECTIONS = (
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
)
# identity/account/sync events ignore the collection filter: keep commits only
JETSTREAM_KINDS = ("commit",)
# On restart, never replay more than this: an older cursor is capped (gap is logged)
JETSTREAM_MAX_REPLAY_MINUTES = int(os.getenv("JETSTREAM_MAX_REPLAY_MINUTES", "60"))

# --- Redpanda (Kafka API) ---
# 127.0.0.1 rather than localhost: avoids librdkafka trying IPv6 (::1) first
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
# Shared by every librdkafka client (producer, Quix): Redpanda only listens on IPv4,
# but `redpanda` (coolify network) and `localhost` also resolve to IPv6
KAFKA_CLIENT_CONFIG = {"broker.address.family": "v4"}

PRODUCER_STATS_INTERVAL_SECONDS = 30

RAW_EVENTS_TOPIC = "raw_events"
RAW_EVENTS_PARTITIONS = 3
# 24h safety buffer for restarts and the daily branch switch — no replay (decision D1)
RAW_EVENTS_RETENTION_MS = 24 * 60 * 60 * 1000

# Branch B consumer group (branch A tracks offsets in its Spark checkpoint)
QUIX_CONSUMER_GROUP = "branch-b-quix"
# One checkpoint = one DuckLake commit, same cadence as Spark's 5 s trigger
QUIX_COMMIT_INTERVAL_SECONDS = 5.0
# Also commit after this many messages: bounds batch size and memory during catch-up.
# Branch A must use the same cap (Spark maxOffsetsPerTrigger) to keep commit parity
QUIX_COMMIT_EVERY = 50_000
# A crashed instance keeps its partitions until its session expires (45 s by default):
# 10 s shortens recovery after a kill. Heartbeats (3 s default) must stay below a third
QUIX_SESSION_TIMEOUT_MS = 10_000
# Without committed offsets, start from the oldest retained event (no silent skip).
# Phase 6 replaces this with explicit 19:00 → 19:00 offsets (D10)
QUIX_AUTO_OFFSET_RESET = os.getenv("QUIX_AUTO_OFFSET_RESET", "earliest")

# --- Garage / S3 ---
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:3900")
# Garage rejects signatures without the configured region (decision D8)
S3_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
BUCKET = os.getenv("BUCKET", "bluesky-streamhouse")

ICEBERG_WAREHOUSE = f"s3a://{BUCKET}/iceberg"
SPARK_CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/spark"
# One DuckLake catalog per writer (decision D15): Quix writes Bronze, dbt writes Silver
# and Gold. Data paths must not overlap: a catalog's CHECKPOINT deletes every file under
# its DATA_PATH that it does not track, so a nested path would lose the other's files
DUCKLAKE_BRONZE_DATA_PATH = f"s3://{BUCKET}/ducklake/bronze/"
DUCKLAKE_TRANSFORM_DATA_PATH = f"s3://{BUCKET}/ducklake/transform/"

# --- Postgres (DuckLake catalog) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DUCKLAKE_CATALOG_DB = os.getenv("DUCKLAKE_CATALOG_DB", "ducklake_catalog")
# Both catalogs live in the same Postgres database, one metadata schema each. DuckLake
# numbers snapshots per catalog: with a single catalog, every dbt commit collided with
# the concurrent Quix commit (duplicate snapshot id, retried up to 10 times, and each
# retry re-sends the inlined rows): 0.7 s inserts went to 8 s in a local test
DUCKLAKE_BRONZE_ALIAS = "bronze"
DUCKLAKE_BRONZE_METADATA_SCHEMA = "bronze"
DUCKLAKE_TRANSFORM_ALIAS = "transform"
DUCKLAKE_TRANSFORM_METADATA_SCHEMA = "transform"
# Inserts up to this many rows stay in the Postgres catalog until flushed to Parquet
# (decision D15). A live checkpoint is 5 s of traffic: ~1 100 to ~2 800 rows in
# production (226-560 msg/s by hour), so it is inlined. An inlined insert costs more
# than its row count (prod: ~1 s for 2 200 rows, ~20 s for 9 900 after a traffic burst),
# so bigger batches (bursts, catch-up) go straight to Parquet (~0.2 s) instead.
# DuckLake's default is 10, which never inlines a checkpoint
DUCKLAKE_DATA_INLINING_ROW_LIMIT = int(os.getenv("DUCKLAKE_DATA_INLINING_ROW_LIMIT", "5000"))
# The Quix sink moves inlined rows to Parquet this often: one file per day every
# ~5 min instead of one per checkpoint, and the flush cost stays in branch B's container
DUCKLAKE_INLINED_FLUSH_INTERVAL_SECONDS = 5 * 60
# DuckLake writes Snappy by default: zstd cuts Bronze from 213 to 121 B/row (decision D14).
# Persisted in the catalog, applies to files written afterwards
DUCKLAKE_PARQUET_COMPRESSION = "zstd"

# --- Dagster, branch B (decision D20) ---
# Code location name in the dagster-workspace workspace.yaml
DAGSTER_CODE_LOCATION = "bluesky_duckdb"
# dbt project of branch B, found from this file (no absolute path, unlike velib)
DBT_DUCKDB_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "duckdb"
# Silver -> Gold every 15 min, same freshness contract as branch A
DAGSTER_SCHEDULE_CRON = "*/15 * * * *"
DAGSTER_TIMEZONE = "Europe/Paris"
# A run retries on DuckLake's internal error when a Bronze scan races a Quix flush
# (it invalidates the DuckDB instance; a fresh run starts clean)
DAGSTER_RETRY_MAX = 2
DAGSTER_RETRY_DELAY_SECONDS = 30
# Upper bound on catch-up passes in one run: 07:00 catch-up is ~19 M Bronze rows, ~40
# passes of 500 000 rows (silver_max_rows_per_run in dbt_project.yml); the next run
# carries on from where this one stopped
CATCHUP_MAX_PASSES = 100

# --- Maintenance, branch B (decisions D14, D21) ---
# Retention by event day (tables are split by day: a DELETE drops whole files)
BRONZE_RETENTION_DAYS = 7
SILVER_RETENTION_DAYS = 7
GOLD_RETENTION_DAYS = 30
# Read positions in meta.silver_progress older than this are deleted (the last done
# position of each model is always kept)
SILVER_PROGRESS_RETENTION_DAYS = 7
# Nightly, outside the 07:00-19:00 window of the branches (D10)
MAINTENANCE_CRON = "0 2 * * *"
# DuckLake options persisted by set_option, applied by CHECKPOINT (D21): 24 h of time
# travel, files kept 1 h after they stop being used (a running scan may still read them)
DUCKLAKE_EXPIRE_OLDER_THAN = "1 day"
DUCKLAKE_DELETE_OLDER_THAN = "1 hour"
DUCKLAKE_TARGET_FILE_SIZE = "512MB"
# Quix commits every 5 s: a Bronze CHECKPOINT can lose the race and is retried
CHECKPOINT_RETRIES = 3
CHECKPOINT_RETRY_DELAY_SECONDS = 30
# Same budget as the dbt profile, inside the 1.5 GB code server
MAINTENANCE_DUCKDB_MEMORY_LIMIT = "1GB"
MAINTENANCE_DUCKDB_THREADS = 2
MAINTENANCE_DUCKDB_TEMP_DIRECTORY = "/tmp/duckdb_maintenance"
# A Silver/Gold run in progress at 02:00 is waited for, up to this long
MAINTENANCE_WAIT_FOR_RUN_SECONDS = 30 * 60
# Guard (D16, D21): pending snapshots older than the time travel window would be
# expired before Silver or Gold read them, so the maintenance stops instead
READ_POSITION_MAX_AGE_HOURS = 24
# Alerts (D14, D21): whole bucket (orphans and pending deletions included), and the
# Postgres database holding both catalogs
BUCKET_ALERT_BYTES = 80 * 10**9
CATALOG_ALERT_BYTES = 2 * 10**9
# Dagster runs of this code location only: the instance is shared with velib (D20)
DAGSTER_RUN_RETENTION_DAYS = 30

# --- Alerts (Resend, as velib) ---
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")
ALERT_FROM = "Dagster <alerts@julien-castellano.fr>"

# --- Benchmark (phase 7) ---
BENCHMARK_SAMPLE_SECONDS = 10
BENCHMARK_WINDOW_SECONDS = 5 * 60
