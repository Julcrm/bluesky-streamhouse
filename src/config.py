"""
Centralized configuration for the Bluesky Streamhouse project.
All URLs, topics, storage paths and parameters are defined here.
Values that differ between environments are read from environment variables,
loaded from `.env` when present (local runs; containers get real env vars).
"""

import os
from datetime import date
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

# DuckDB branch consumer group (the Spark branch tracks offsets in its Spark checkpoint)
QUIX_CONSUMER_GROUP = "branch-b-quix"
# One checkpoint = one DuckLake commit, same cadence as Spark's 5 s trigger
QUIX_COMMIT_INTERVAL_SECONDS = 5.0
# Also commit after this many messages: bounds batch size and memory during catch-up.
# Spark branch must use the same cap (Spark maxOffsetsPerTrigger) to keep commit parity
QUIX_COMMIT_EVERY = 50_000
# A crashed instance keeps its partitions until its session expires (45 s by default):
# 10 s shortens recovery after a kill. Heartbeats (3 s default) must stay below a third
QUIX_SESSION_TIMEOUT_MS = 10_000
# Without committed offsets, start from the oldest retained event (no silent skip).
# The alternation (D28) writes each day's start offsets to the group before the start
QUIX_AUTO_OFFSET_RESET = os.getenv("QUIX_AUTO_OFFSET_RESET", "earliest")

# --- Garage / S3 ---
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:3900")
# Garage rejects signatures without the configured region (decision D8)
S3_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
BUCKET = os.getenv("BUCKET", "bluesky-streamhouse")

# Iceberg tables live under s3://<bucket>/iceberg/ (key prefix of the Lakekeeper
# warehouse, set at warehouse creation): disjoint from the DuckLake paths below
ICEBERG_KEY_PREFIX = "iceberg"
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
# ~5 min instead of one per checkpoint, and the flush cost stays in the DuckDB branch's container
DUCKLAKE_INLINED_FLUSH_INTERVAL_SECONDS = 5 * 60
# DuckLake writes Snappy by default: zstd cuts Bronze from 213 to 121 B/row (decision D14).
# Persisted in the catalog, applies to files written afterwards
DUCKLAKE_PARQUET_COMPRESSION = "zstd"

# --- Dagster, the DuckDB branch (decision D20) ---
# Code location name in the dagster-workspace workspace.yaml: bluesky_duckdb (DuckDB branch)
# or bluesky_spark (Spark branch), set by each code server container
DAGSTER_CODE_LOCATION = os.getenv("DAGSTER_CODE_LOCATION", "bluesky_duckdb")
# Every asset key starts with it: one folder per project in the shared Dagster catalog
# (velib-lakehouse uses `velib`), then one per engine, then one per layer (bronze,
# silver, gold, maintenance, alternation). Engine folders (D30): both branches have
# the same model names (contract), and two code locations cannot declare the same key
DAGSTER_ASSET_PREFIX = "bluesky"
DAGSTER_ENGINE_DUCKDB = "duckdb"
DAGSTER_ENGINE_SPARK = "spark"
# dbt projects, found from this file (no absolute path, unlike velib)
DBT_DUCKDB_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "duckdb"
DBT_SPARK_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "spark"
# Silver -> Gold every 15 min, same freshness contract as the Spark branch
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

# --- Maintenance of the DuckDB branch (D14, D21); retention shared with the Spark branch ---
# Retention by event day (tables are split by day: a DELETE drops whole files)
BRONZE_RETENTION_DAYS = 7
SILVER_RETENTION_DAYS = 7
GOLD_RETENTION_DAYS = 30
# Read positions in meta.silver_progress older than this are deleted (the last done
# position of each model is always kept)
SILVER_PROGRESS_RETENTION_DAYS = 7
# Column a table's retention is measured on, by order of preference (both branches)
RETENTION_TIME_COLUMNS = ("event_time", "minute", "hour")
# Nightly, outside the 07:00-19:00 window of the branches (D10)
MAINTENANCE_CRON = "0 2 * * *"
# DuckLake options persisted by set_option, applied by CHECKPOINT (D21): 24 h of time
# travel, files kept 1 h after they stop being used (a running scan may still read them)
DUCKLAKE_EXPIRE_OLDER_THAN = "1 day"
DUCKLAKE_DELETE_OLDER_THAN = "1 hour"
DUCKLAKE_TARGET_FILE_SIZE = "512MB"
# Quix commits to Bronze every 5 s: DuckLake refuses a retention DELETE on a table it
# inserted into meanwhile (failed two nights in prod, 2026-10-05/06). The Bronze
# maintenance holds this Postgres advisory lock exclusive, each sink commit holds it
# shared, and the sink pauses (backpressure) while the maintenance runs
BRONZE_WRITE_LOCK_KEY = 2_026_100_601
QUIX_LOCKED_RETRY_SECONDS = 30.0
# A DELETE or CHECKPOINT losing a commit race anyway (another writer) is retried
COMMIT_CONFLICT_RETRIES = 3
COMMIT_CONFLICT_RETRY_DELAY_SECONDS = 30
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
# Postgres database holding both catalogs. A day dropped by the retention stays two
# nights in the bucket (expired the next night, deleted the night after), so the bucket
# runs ~30 GB above the live data: 90 GB leaves room for Redpanda in the 100 GB budget
BUCKET_ALERT_BYTES = 90 * 10**9
CATALOG_ALERT_BYTES = 2 * 10**9
# Dagster runs of this code location only: the instance is shared with velib (D20)
DAGSTER_RUN_RETENTION_DAYS = 30
# dagster-dbt's per-run target folders (~3 MB each), deleted by the housekeeping
DBT_TARGET_RETENTION_DAYS = 2

# --- Nightly checks (decision D25) ---
# Every 15 min, dbt tests check the last 2 hours written (dbt var test_window); every
# night the same tests run alone over a day, after the maintenance
NIGHTLY_CHECKS_CRON = "0 3 * * *"
NIGHTLY_CHECKS_TAG = "bluesky/nightly_checks"
NIGHTLY_TEST_VARS = {"test_window": "INTERVAL 1 DAY", "gold_check_window": "INTERVAL 1 DAY"}
# Waits for any other run of the location (Silver/Gold, maintenance), up to this long
NIGHTLY_CHECKS_WAIT_SECONDS = 2 * 60 * 60

# --- Alternation, one branch per day (decisions D1, D10, D28) ---
# Benchmark day J = 19:00 (J-1) -> 19:00 (J), Europe/Paris. The branch of J starts at
# 07:00, catches up the night from Redpanda, then runs live until its 19:00 bounds
ALTERNATION_TIMEZONE = "Europe/Paris"
ALTERNATION_OPEN_TIME = "07:00"
ALTERNATION_CLOSE_TIME = "19:00"
# An engine still running at this time is marked late (alert) and keeps going: no engine
# runs at night, so it may finish its day. Late days stay in the benchmark, marked
ALTERNATION_LATE_TIME = "19:30"
# Hard stop the next morning, before the next opening: an engine still running is
# stopped and its day is incomplete (messages missing from its Bronze)
ALTERNATION_HARD_STOP_TIME = "06:30"
# The DuckDB branch runs on this day, then the two branches alternate (D1). A benchmark
# setting: kept here, not in the environment (Coolify freezes a ${VAR:-default} at
# first deploy)
ALTERNATION_START_DATE = date(2026, 10, 7)
# Neutral database (neither branch's catalog) on the shared Postgres: branch_calendar,
# later the benchmark windows (phase 7)
BENCHMARK_DB = os.getenv("BENCHMARK_DB", "bluesky_benchmark")

# --- Benchmark collector (phase 7, decision D34) ---
# Raw samples every 10 s, kept 30 days; the 5-minute windows are computed from them
COLLECTOR_INTERVAL_SECONDS = 10
COLLECTOR_RETENTION_DAYS = 30
# Host cgroup tree and /proc, mounted read-only in the bench-collector container
COLLECTOR_CGROUP_ROOT = os.getenv("COLLECTOR_CGROUP_ROOT", "/host/cgroup")
COLLECTOR_PROC_ROOT = os.getenv("COLLECTOR_PROC_ROOT", "/host/proc")
# Docker socket proxy that only allows listing containers (id -> compose service)
COLLECTOR_DOCKER_URL = os.getenv("COLLECTOR_DOCKER_URL", "http://bench-docker-proxy:2375")
# Measured containers by compose service: (branch, layer), branch None for the shared
# ones (D31, D34). Branch values are the calendar's (src/alternation/calendar.py)
BENCHMARK_SERVICES: dict[str, tuple[str | None, str]] = {
    "spark": ("spark", "streaming"),
    "spark-thrift": ("spark", "transform"),
    "bluesky-spark": ("spark", "transform"),
    "lakekeeper": ("spark", "catalog"),
    "quix": ("duckdb", "streaming"),
    "bluesky-duckdb": ("duckdb", "transform"),
    "producer": (None, "ingestion"),
    "redpanda": (None, "broker"),
    "garage": (None, "storage"),
    "bench-collector": (None, "collector"),
}
# Catalogs in the shared Postgres: pg_stat_database counters, an estimate (D12, D34)
CATALOG_DATABASES = ("ducklake_catalog", "iceberg_catalog")
# 5-minute windows built at night from the samples and Bronze (D34)
BENCHMARK_WINDOW_SECONDS = 300
# A window counts in the ratios only if the samples cover 90 % of it (collector gaps)
BENCHMARK_MIN_COVERAGE = 0.9
# Traffic levels to compare the branches at equal throughput (msg/s, upper bound, label)
BENCHMARK_RATE_BUCKETS = ((200, "0-200"), (500, "200-500"), (float("inf"), "500+"))
# Host health flags (D32), provisional until the protocol freeze: a window is
# contaminated above 5 % CPU steal, above 3 s of memory or I/O stall in 5 min (1 %), or
# below 512 MiB of available memory
HOST_STEAL_PCT_MAX = 5.0
HOST_STALL_MS_MAX = 3000.0
HOST_MIN_AVAILABLE_MIB = 512.0
# The supervisor of each engine container reads the calendar this often
SUPERVISOR_POLL_SECONDS = 30
# Heartbeat of the supervisors and the producer, read by the container healthchecks
# (src/healthcheck.py): written at each loop, healthy while younger than 4 loops
HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/heartbeat")
# A crashed engine is restarted after this long (same day, from its last commit)
SUPERVISOR_RESTART_DELAY_SECONDS = 30
# Engines read their day's bounds this often while running (end offsets appear at 19:00)
ENGINE_BOUNDS_POLL_SECONDS = 30
# Caught up = first commit whose newest event is less than this old (same rule for both
# branches, D28): separates the 07:00 catch-up from live processing in the benchmark
CAUGHT_UP_LAG_SECONDS = 10
# Silver/Gold of a branch keep running this long after its engine stopped: the last
# Bronze commits of the day still have to reach Gold
TRANSFORM_TAIL_SECONDS = 60 * 60
# Branches whose engine is deployed: days of another branch are skipped without alert
# (the DuckDB branch ran alone first, D28; the Spark branch was deployed on a day of the
# DuckDB branch, its first day the next one)
DEPLOYED_BRANCHES = ("spark", "duckdb")
# Calendar alerts (email), each once per day: engine not started this long after the
# opening, day still open this long after the close (never closed), day still running
# this long after the hard stop (the supervisor did not act)
ALERT_NOT_STARTED_MINUTES = 15
ALERT_NOT_CLOSED_MINUTES = 15
ALERT_STOP_MISSED_MINUTES = 15

# --- Alerts (Resend, as velib) ---
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")
ALERT_FROM = "Dagster <alerts@julien-castellano.fr>"

# --- Spark branch: Spark Structured Streaming -> Iceberg (phase 4) ---
# Iceberg REST catalog (Lakekeeper, Postgres-backed). It signs the S3 requests of its
# clients (remote signing): Spark holds no S3 key. Garage has no STS for vended creds
ICEBERG_CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181/catalog")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "bluesky")
# Spark catalog name: tables are lakekeeper.<namespace>.<table>
SPARK_CATALOG = "lakekeeper"
ICEBERG_BRONZE_NAMESPACE = "bronze"
# Streaming state (Kafka offsets, batch ids) on a local volume: a checkpoint needs
# atomic renames, which S3 and Garage do not offer
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "state/spark-checkpoint/bronze")
# One checkpoint per benchmark day under this directory (D28): a new day starts from its
# start offsets (startingOffsets is only read by a new checkpoint), a restart in the day
# resumes its own. The last days are kept, older ones deleted
SPARK_DAY_CHECKPOINTS_DIR = os.getenv("SPARK_DAY_CHECKPOINTS_DIR", "state/spark-checkpoint/days")
SPARK_DAY_CHECKPOINTS_KEPT = 2
# Snapshot summary property holding "<query id>:<batch id>" of each Bronze append: a
# micro-batch replayed after a crash finds its key and is not written twice (what the
# native Iceberg streaming sink does, redone in foreachBatch for the day bounds)
SPARK_BATCH_SNAPSHOT_PROPERTY = "bluesky.streaming-batch"
# Same cadence and batch cap as Quix (decisions D13, D10 parity): 5 s, 50 000 messages
SPARK_TRIGGER_INTERVAL = f"{int(QUIX_COMMIT_INTERVAL_SECONDS)} seconds"
SPARK_MAX_OFFSETS_PER_TRIGGER = QUIX_COMMIT_EVERY
# Driver memory (local mode: the driver runs every task). Set before the JVM starts
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "512m")
# JVM flags of the driver (memory outside the heap: metaspace, JIT code cache, threads)
SPARK_DRIVER_JAVA_OPTIONS = os.getenv("SPARK_DRIVER_JAVA_OPTIONS", "")
# Shuffle partitions = cores of the node, not Spark's default 200 (tuning checklist)
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
# Jars: the image sets SPARK_JARS_DIR (resolved at build); a local run resolves packages
# 1.12.0 (2026-09-30) breaks remote-signed ranged reads (403 Invalid signature on every
# GET not starting at byte 0, i.e. Parquet footers and columns); 1.11.0 works (tested
# 2026-10-01 against Lakekeeper 0.13.6 and Garage 2.4.1)
ICEBERG_VERSION = "1.11.0"
SPARK_VERSION = "4.1.3"
SPARK_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}",
)

# --- Iceberg maintenance, the Spark branch (decision D21 for the DuckDB branch, same contract) ---
# Nightly, 30 min after the DuckDB branch's so the two never compete for the VPS
ICEBERG_MAINTENANCE_CRON = "30 2 * * *"
# Hourly compaction of the Bronze table on the Spark branch's days (D33): a commit every
# 5 s leaves ~720 small files and manifests an hour, which every incremental Silver read
# plans over. At :50, between two Silver/Gold ticks (:45 and :00)
ICEBERG_HOURLY_COMPACTION_CRON = "50 * * * *"
# Time travel kept: 24 h, as DuckLake (D21). With a commit every 5 s, ~17 000 snapshots
# stay listed in every metadata.json: their cost is measured, not avoided (H8)
ICEBERG_SNAPSHOT_RETENTION_HOURS = 24
# Iceberg refuses to remove orphans younger than 24 h (a running write may own them)
ICEBERG_ORPHAN_MIN_AGE_HOURS = 25
# Same target as DuckLake's CHECKPOINT (DUCKLAKE_TARGET_FILE_SIZE)
ICEBERG_TARGET_FILE_SIZE_BYTES = 512 * 1024 * 1024

# --- Spark Thrift server, Silver/Gold of the Spark branch (decision D6 revised) ---
# One long-lived JVM that dbt-spark (PyHive) connects to: no Spark session start per
# dbt command. Runs from the opening of a day of the Spark branch until the opening of
# the next day of the DuckDB branch (supervisor in service mode): it serves the Spark
# branch's Silver/Gold, maintenance and nightly tests
SPARK_THRIFT_HOST = os.getenv("SPARK_THRIFT_HOST", "localhost")
SPARK_THRIFT_PORT = 10000
# Two cores, as the DuckDB branch's dbt runs DuckDB with 2 threads (same transform budget). With
# local[*] (12 cores locally) 12 Parquet writers buffering row groups overflowed the heap
SPARK_THRIFT_CORES = 2
# Heap of the server JVM; the rest of the 1.5 GB container is off-heap (~600 MB of
# metaspace, code cache and native memory, whatever the heap). D31, measured 2026-10-07:
# 768 MB peaks at 1.37 GiB over 500 000-row passes + dbt build; 1 GB was OOM-killed
SPARK_THRIFT_DRIVER_MEMORY = os.getenv("SPARK_THRIFT_DRIVER_MEMORY", "768m")
# One run at a time in the Spark branch's location (D30): maintenance and completeness wait for
# the other runs (a 07:00 Silver/Gold catch-up included) up to this long
SPARK_RUN_WAIT_SECONDS = 2 * 60 * 60

# --- Benchmark (phase 7) ---
BENCHMARK_SAMPLE_SECONDS = 10
BENCHMARK_WINDOW_SECONDS = 5 * 60
