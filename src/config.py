"""
Centralized configuration for the Bluesky Streamhouse project.
All URLs, topics, storage paths and parameters are defined here.
Values that differ between environments are read from environment variables.
"""

import os

# --- Bluesky Jetstream ---
JETSTREAM_URL = os.getenv("JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe")

# Collections ingested (decision D2)
JETSTREAM_COLLECTIONS = (
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
)

# --- Redpanda (Kafka API) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

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
BUCKET = os.getenv("BUCKET", "bluesky-streamhouse")

ICEBERG_WAREHOUSE = f"s3a://{BUCKET}/iceberg"
SPARK_CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/spark"
DUCKLAKE_DATA_PATH = f"s3://{BUCKET}/ducklake/"

# --- Postgres (DuckLake catalog) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DUCKLAKE_CATALOG_DB = os.getenv("DUCKLAKE_CATALOG_DB", "ducklake_catalog")

# --- Benchmark (phase 7) ---
BENCHMARK_SAMPLE_SECONDS = 10
BENCHMARK_WINDOW_SECONDS = 5 * 60
