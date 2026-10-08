"""
Spark branch streaming job: `raw_events` (Redpanda) -> Iceberg Bronze, with Spark
Structured Streaming. Same contract as the DuckDB branch (src/processing/bronze.py): same
columns, one micro-batch every 5 s capped at 50 000 messages (decisions D10, D13).

- Catalog: Lakekeeper (Iceberg REST, Postgres-backed). It signs Spark's S3 requests
  (remote signing): no S3 key in this job.
- Benchmark day (decision D28): started by the supervisor with ALTERNATION_DAY, the job
  reads from the day's start offsets (one checkpoint per day), drops what is past the
  end offsets, waits for them from 19:00, records caught_up_at and marks the day done
  once its committed offsets reach the end (src.alternation.engine, shared with Quix).
- Writes go through foreachBatch: a streaming query's plan is fixed, so the native
  Iceberg sink can neither filter on bounds that appear at 19:00 nor wait for them.
  Exactly-once is kept the way the native sink does it: each append records
  "<query id>:<batch id>" in its snapshot summary, and a replayed batch whose key is
  already there is skipped (Silver still deduplicates on seq, the contract).
- Checkpoint (Kafka offsets, batch ids) on a local volume: it relies on atomic renames.

Usage: python -m src.processing.spark.stream_job (without ALTERNATION_DAY: no bounds,
one checkpoint, from the oldest retained offset; local development)
"""

import json
import logging
import os
import shutil
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src import config
from src.alternation import calendar as cal
from src.alternation.engine import EngineDay
from src.processing.bronze import BRONZE_COLUMNS, BRONZE_TABLE
from src.resources.spark import build_session

logger = logging.getLogger(__name__)

BRONZE_IDENTIFIER = f"{config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"

__all__ = ["BRONZE_IDENTIFIER", "bronze_ddl", "build_session", "main", "parse_raw_events"]

# Jetstream v2 commit envelope, as the producer writes it to raw_events. `record` is
# not typed here: it is kept as raw JSON, like the DuckDB branch
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
# Fields without which the DuckDB branch (parse_bronze_event) drops a message
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
        # Same codec as the DuckDB branch (D14)
        "    'write.parquet.compression-codec' = 'zstd',\n"
        # Every commit (one per 5 s, ~17 000 a day) writes a new metadata.json and keeps
        # the old ones by default: Iceberg deletes them beyond the last 100 itself
        "    'write.metadata.delete-after-commit.enabled' = 'true',\n"
        "    'write.metadata.previous-versions-max' = '100'\n"
        ")"
    )


def read_raw_events(spark: SparkSession, starting_offsets: str = "earliest") -> DataFrame:
    """raw_events from `starting_offsets` (Spark's JSON, or earliest), capped per
    micro-batch like Quix. A checkpoint that already exists ignores it and resumes."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.RAW_EVENTS_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", config.SPARK_MAX_OFFSETS_PER_TRIGGER)
        # Offsets expired from Redpanda (24 h) while the job was down: carry on from the
        # oldest retained one, as Quix does (auto.offset.reset=earliest); the gap shows
        # in the offset audit of Bronze
        .option("failOnDataLoss", "false")
        .load()
    )


def before_end(end: cal.Offsets) -> Column:
    """Kafka rows of the day: offset before its partition's end (exclusive)."""
    condition = F.lit(False)
    for partition, offset in end.items():
        condition = condition | (
            (F.col("partition") == partition) & (F.col("offset") < F.lit(offset))
        )
    return condition


def day_checkpoint(day: EngineDay | None) -> str:
    """One checkpoint per benchmark day; the single historical one without a day."""
    if day is None:
        return config.SPARK_CHECKPOINT_DIR
    return str(Path(config.SPARK_DAY_CHECKPOINTS_DIR) / day.day.isoformat())


def prune_day_checkpoints(
    root: str = config.SPARK_DAY_CHECKPOINTS_DIR, keep: int = config.SPARK_DAY_CHECKPOINTS_KEPT
) -> list[str]:
    """Delete all but the last `keep` day checkpoints (ISO names sort by date)."""
    if not os.path.isdir(root):
        return []
    days = sorted(name for name in os.listdir(root) if os.path.isdir(os.path.join(root, name)))
    removed = days[:-keep] if keep else days
    for name in removed:
        shutil.rmtree(os.path.join(root, name))
    return removed


def committed_position(progress_json: str) -> cal.Offsets | None:
    """End offsets of the last completed batch, from the progress's compact JSON. Not
    from the progress object: PySpark 4 turns the offsets into str(dict), not JSON."""
    sources = json.loads(progress_json).get("sources") or []
    end = sources[0].get("endOffset") if sources else None
    return cal.offsets_from_json(end) if end else None


class BronzeBatchWriter:
    """foreachBatch writer: day bounds, then an idempotent append to Iceberg Bronze."""

    def __init__(
        self,
        checkpoint: str,
        day: EngineDay | None,
        identifier: str = BRONZE_IDENTIFIER,
    ) -> None:
        self.checkpoint = checkpoint
        self.day = day
        self.identifier = identifier
        self._query_id: str | None = None
        # Cost of the idempotence check of the last batch (benchmark: Spark branch complexity)
        self.last_key_check_ms = 0.0

    def query_id(self) -> str:
        """Id of the streaming query, stable across restarts on the same checkpoint
        (Spark writes it before the first batch)."""
        if self._query_id is None:
            with open(os.path.join(self.checkpoint, "metadata")) as f:
                self._query_id = json.load(f)["id"]
        return self._query_id

    def last_key(self, spark: SparkSession) -> str | None:
        """Key of the latest append made by this writer (maintenance snapshots, which
        carry none, are skipped)."""
        prop = config.SPARK_BATCH_SNAPSHOT_PROPERTY
        row = spark.sql(
            f"SELECT summary['{prop}'] AS batch_key FROM {self.identifier}.snapshots "
            f"WHERE summary['{prop}'] IS NOT NULL ORDER BY committed_at DESC LIMIT 1"
        ).first()
        return row["batch_key"] if row else None

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        end = self.day.bounds() if self.day is not None else None
        if end is not None:
            batch = batch.where(before_end(end))
        key = f"{self.query_id()}:{batch_id}"
        started = time.perf_counter()
        committed = self.last_key(spark)
        self.last_key_check_ms = (time.perf_counter() - started) * 1000
        if committed == key:
            logger.info(f"Batch {batch_id} already in Bronze (replay after a restart), skipped")
            return
        rows = parse_raw_events(batch)
        track_lag = self.day is not None and not self.day.caught_up
        if track_lag:
            rows = rows.persist()
        try:
            rows.writeTo(self.identifier).option(
                f"snapshot-property.{config.SPARK_BATCH_SNAPSHOT_PROPERTY}", key
            ).append()
            if track_lag:
                # Epoch microseconds: collected datetimes would be in the Python
                # process's time zone, whatever the session's
                newest = rows.agg(F.max(F.unix_micros("event_time")).alias("us")).first()["us"]
                if newest is not None:
                    self.day.record_commit(datetime.fromtimestamp(newest / 1e6, UTC))
        finally:
            if track_lag:
                rows.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    day = EngineDay.from_env()
    starting = "earliest"
    if day is not None:
        # A new day's checkpoint starts from startingOffsets: nothing else to move
        day.prepare(lambda _offsets: None)
        starting = cal.offsets_to_json(day.start)
        removed = prune_day_checkpoints()
        if removed:
            logger.info(f"Old day checkpoints removed: {removed}")
    checkpoint = day_checkpoint(day)
    spark = build_session("bluesky-bronze")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}"
    )
    spark.sql(bronze_ddl())

    writer = BronzeBatchWriter(checkpoint, day)
    query = (
        read_raw_events(spark, starting)
        .writeStream.foreachBatch(writer)
        .trigger(processingTime=config.SPARK_TRIGGER_INTERVAL)
        .option("checkpointLocation", checkpoint)
        .queryName("bronze")
        .start()
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
                    f"(add batch {durations.get('addBatch', 0)} ms, "
                    f"idempotence check {writer.last_key_check_ms:.0f} ms)"
                )
        # Committed position = end offsets of the last completed batch (like the
        # committed offsets of Quix's consumer group)
        progress = query.lastProgress
        if day is not None and not day.done and progress:
            raw = progress.json() if callable(progress.json) else progress.json
            position = committed_position(raw)
            if position is not None:
                day.check_complete(position)
    if query.exception():
        raise query.exception()
    spark.stop()


if __name__ == "__main__":
    main()
