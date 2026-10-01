"""
Branch A streaming job: `raw_events` (Redpanda) -> Iceberg Bronze, with Spark
Structured Streaming. Same contract as branch B (src/processing/bronze.py): same
columns, one micro-batch every 5 s capped at 50 000 messages (decisions D10, D13).

- Catalog: Lakekeeper (Iceberg REST, Postgres-backed). It signs Spark's S3 requests
  (remote signing): no S3 key in this job.
- Exactly-once into Iceberg: the sink records each micro-batch id in the table's
  snapshots, so a batch replayed after a crash is not written twice (native to Spark;
  Silver still deduplicates on seq, the contract).
- Checkpoint (Kafka offsets, batch ids) on a local volume: it relies on atomic renames.

Usage: python -m src.processing.spark.stream_job
"""

import glob
import logging
import os
import signal
from types import FrameType

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src import config
from src.processing.bronze import BRONZE_COLUMNS, BRONZE_TABLE

logger = logging.getLogger(__name__)

BRONZE_IDENTIFIER = f"{config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"

# Jetstream v2 commit envelope, as the producer writes it to raw_events. `record` is
# not typed here: it is kept as raw JSON, like branch B
ENVELOPE = StructType(
    [
        StructField("$type", StringType()),
        StructField(
            "payload",
            StructType(
                [
                    StructField("seq", LongType()),
                    StructField("did", StringType()),
                    StructField("collection", StringType()),
                    StructField("operation", StringType()),
                    StructField("rkey", StringType()),
                    StructField("rev", StringType()),
                    StructField("cid", StringType()),
                    StructField("time", StringType()),
                ]
            ),
        ),
    ]
)
# Fields without which branch B (parse_bronze_event) drops a message
REQUIRED = ("seq", "did", "collection", "operation", "rkey", "event_time")


def parse_raw_events(raw: DataFrame, processed_at: Column | None = None) -> DataFrame:
    """Kafka rows (value, partition, offset) -> Bronze columns, in contract order.

    Mirrors parse_bronze_event: messages that are not `message` envelopes, or miss a
    required field or a valid time, are dropped. `processed_at` is the write time,
    one value per micro-batch (current_timestamp() is fixed for a batch).
    """
    value = F.col("value").cast("string")
    message = F.from_json(value, ENVELOPE)
    payload = message["payload"]
    parsed = raw.select(
        message["$type"].alias("_type"),
        payload["seq"].alias("seq"),
        payload["did"].alias("did"),
        payload["collection"].alias("collection"),
        payload["operation"].alias("operation"),
        payload["rkey"].alias("rkey"),
        payload["rev"].alias("rev"),
        payload["cid"].alias("cid"),
        # try_cast: Spark 4 runs in ANSI mode, where one malformed time would fail the
        # whole micro-batch (and the job, in a loop); NULL drops the message instead
        payload["time"].try_cast("timestamp").alias("event_time"),
        # Raw JSON text of the record, compact; NULL on delete
        F.get_json_object(value, "$.payload.record").alias("record"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        (processed_at if processed_at is not None else F.current_timestamp()).alias("processed_at"),
    )
    usable = F.col("_type") == "message"
    for column in REQUIRED:
        usable = usable & F.col(column).isNotNull()
    return parsed.where(usable).select(*(name for name, _, _ in BRONZE_COLUMNS))


def bronze_ddl(identifier: str = BRONZE_IDENTIFIER) -> str:
    """CREATE TABLE of the Iceberg Bronze table: contract columns, split by event day
    (hidden partitioning, the Iceberg equivalent of DuckLake's day split, D14)."""
    columns = ",\n    ".join(f"{name} {spark}" for name, _, spark in BRONZE_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {identifier} (\n    {columns}\n) USING iceberg\n"
        "PARTITIONED BY (days(event_time))\n"
        "TBLPROPERTIES (\n"
        "    'format-version' = '2',\n"
        # Same codec as branch B (D14)
        "    'write.parquet.compression-codec' = 'zstd',\n"
        # Every commit (one per 5 s, ~17 000 a day) writes a new metadata.json and keeps
        # the old ones by default: Iceberg deletes them beyond the last 100 itself
        "    'write.metadata.delete-after-commit.enabled' = 'true',\n"
        "    'write.metadata.previous-versions-max' = '100'\n"
        ")"
    )


def build_session(app_name: str = "bluesky-bronze") -> SparkSession:
    """Local-mode session (single node) with the Lakekeeper REST catalog."""
    catalog = f"spark.sql.catalog.{config.SPARK_CATALOG}"
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.driver.extraJavaOptions", config.SPARK_DRIVER_JAVA_OPTIONS)
        # Timestamps are UTC end to end, like branch B's DuckDB sessions
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "rest")
        .config(f"{catalog}.uri", config.ICEBERG_CATALOG_URI)
        .config(f"{catalog}.warehouse", config.ICEBERG_WAREHOUSE)
        # Ask Lakekeeper to sign S3 requests rather than hand out credentials
        .config(f"{catalog}.header.X-Iceberg-Access-Delegation", "remote-signing")
        .config("spark.ui.enabled", "false")
    )
    jars_dir = os.getenv("SPARK_JARS_DIR")
    if jars_dir:  # image: jars resolved at build time into this directory, never at runtime
        jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
        builder = builder.config("spark.jars", ",".join(jars))
    else:  # local run: Spark resolves the pinned packages
        builder = builder.config("spark.jars.packages", ",".join(config.SPARK_PACKAGES))
    return builder.getOrCreate()


def read_raw_events(spark: SparkSession) -> DataFrame:
    """raw_events from the oldest retained offset, capped per micro-batch like Quix."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.RAW_EVENTS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", config.SPARK_MAX_OFFSETS_PER_TRIGGER)
        # Offsets expired from Redpanda (24 h) while the job was down: carry on from the
        # oldest retained one, as Quix does (auto.offset.reset=earliest); the gap shows
        # in the offset audit of Bronze
        .option("failOnDataLoss", "false")
        .load()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}"
    )
    spark.sql(bronze_ddl())

    query = (
        parse_raw_events(read_raw_events(spark))
        .writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=config.SPARK_TRIGGER_INTERVAL)
        .option("checkpointLocation", config.SPARK_CHECKPOINT_DIR)
        # Rows of a batch span several days only around midnight or in a catch-up:
        # one open file per partition instead of sorting the batch
        .option("fanout-enabled", "true")
        .queryName("bronze")
        .toTable(BRONZE_IDENTIFIER)
    )

    # The handler only raises a flag: calling the JVM from it while the main thread
    # waits in awaitTermination is a reentrant py4j call (it broke the stop, exit 1)
    stopping = {"signal": None}

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        stopping["signal"] = signum

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    # One log line per batch, like the Quix sink: rows, batch duration
    last_batch = -1
    while query.isActive:
        query.awaitTermination(5)
        if stopping["signal"] is not None:
            logger.info(f"Signal {stopping['signal']}: stopping the query")
            # A batch cut short is replayed from the checkpoint at restart (exactly once)
            query.stop()
            break
        for progress in query.recentProgress:
            if progress["batchId"] > last_batch and progress["numInputRows"] > 0:
                last_batch = progress["batchId"]
                durations = progress.get("durationMs", {})
                logger.info(
                    f"Bronze batch {progress['batchId']}: {progress['numInputRows']} rows, "
                    f"{durations.get('triggerExecution', 0)} ms "
                    f"(add batch {durations.get('addBatch', 0)} ms)"
                )
    if query.exception():
        raise query.exception()
    spark.stop()


if __name__ == "__main__":
    main()
