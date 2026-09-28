"""
Centralized configuration for the Bluesky Streamhouse project.
All URLs, topics, storage paths and parameters are defined here.
Values that differ between environments are read from environment variables,
loaded from `.env` when present (local runs; containers get real env vars).
"""

import os

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
# (decision D15). Above a live checkpoint (~2 000 rows, ~3 000 at peak), below a
# catch-up one (QUIX_COMMIT_EVERY): live writes are inlined, catch-up writes Parquet.
# DuckLake's default is 10, which never inlines a checkpoint.
# Measured cost, kept as a benchmark result: an inlined insert of this 13-column table
# grows faster than its row count (2 000 rows ~1.4 s, 8 000 rows ~20 s locally), so a
# slow commit makes the next checkpoint bigger and slower until it passes the limit and
# goes to Parquet (~0.2 s). Straight Parquet (limit 0) commits in ~0.1 s
DUCKLAKE_DATA_INLINING_ROW_LIMIT = int(os.getenv("DUCKLAKE_DATA_INLINING_ROW_LIMIT", "10000"))
# The Quix sink moves inlined rows to Parquet this often: one file per day every
# ~5 min instead of one per checkpoint, and the flush cost stays in branch B's container
DUCKLAKE_INLINED_FLUSH_INTERVAL_SECONDS = 5 * 60
# DuckLake writes Snappy by default: zstd cuts Bronze from 213 to 121 B/row (decision D14).
# Persisted in the catalog, applies to files written afterwards
DUCKLAKE_PARQUET_COMPRESSION = "zstd"

# --- Benchmark (phase 7) ---
BENCHMARK_SAMPLE_SECONDS = 10
BENCHMARK_WINDOW_SECONDS = 5 * 60
