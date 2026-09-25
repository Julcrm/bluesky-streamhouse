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

PRODUCER_STATS_INTERVAL_SECONDS = 30

RAW_EVENTS_TOPIC = "raw_events"
RAW_EVENTS_PARTITIONS = 3
# 24h safety buffer for restarts and the daily branch switch — no replay (decision D1)
RAW_EVENTS_RETENTION_MS = 24 * 60 * 60 * 1000

# Branch B consumer group (branch A tracks offsets in its Spark checkpoint)
QUIX_CONSUMER_GROUP = "branch-b-quix"

# --- Garage / S3 ---
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:3900")
# Garage rejects signatures without the configured region (decision D8)
S3_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
BUCKET = os.getenv("BUCKET", "bluesky-streamhouse")

ICEBERG_WAREHOUSE = f"s3a://{BUCKET}/iceberg"
SPARK_CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/spark"
DUCKLAKE_DATA_PATH = f"s3://{BUCKET}/ducklake/"

# --- Postgres (DuckLake catalog) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DUCKLAKE_CATALOG_DB = os.getenv("DUCKLAKE_CATALOG_DB", "ducklake_catalog")
DUCKLAKE_ALIAS = "lake"
# Inserts up to this many rows stay in the Postgres catalog until flushed to Parquet.
# 10 is DuckLake's default, pinned here so the benchmark setting is explicit (phase 2)
DUCKLAKE_DATA_INLINING_ROW_LIMIT = int(os.getenv("DUCKLAKE_DATA_INLINING_ROW_LIMIT", "10"))

# --- Benchmark (phase 7) ---
BENCHMARK_SAMPLE_SECONDS = 10
BENCHMARK_WINDOW_SECONDS = 5 * 60
