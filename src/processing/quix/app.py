"""
Branch B streaming application.
Consumes `raw_events` and writes micro-batches to the DuckLake Bronze table on Garage (S3).

Delivery is at-least-once: Quix commits Kafka offsets only after the sink flush of a
checkpoint succeeds. All partitions of a checkpoint go in a single INSERT, so each
checkpoint (every QUIX_COMMIT_INTERVAL_SECONDS) is exactly one DuckLake snapshot, the
same cadence as Spark's 5 s trigger in branch A.
"""

import time
from datetime import UTC, datetime
from typing import Any

import duckdb
import pyarrow as pa
from loguru import logger
from quixstreams import Application
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

from src import config
from src.processing.bronze import (
    BRONZE_COLUMN_NAMES,
    BRONZE_COLUMNS,
    BRONZE_TABLE,
    BronzeEvent,
    duckdb_ddl,
    parse_bronze_event,
)
from src.resources.ducklake import DuckLakeSettings, connect

# Pause consumption this long when Garage or the Postgres catalog is unreachable
BACKPRESSURE_RETRY_SECONDS = 10.0

# Arrow types matching the DuckDB types of the Bronze contract
ARROW_TYPES = {
    "BIGINT": pa.int64(),
    "INTEGER": pa.int32(),
    "VARCHAR": pa.string(),
    "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"),
}
BRONZE_ARROW_SCHEMA = pa.schema([(name, ARROW_TYPES[duck]) for name, duck, _ in BRONZE_COLUMNS])


def columns_from_batches(batches: list[SinkBatch], processed_at: datetime) -> dict[str, list[Any]]:
    """Turn sink batches into Bronze columns (one list per column, in schema order)."""
    columns: dict[str, list[Any]] = {name: [] for name in BRONZE_COLUMN_NAMES}
    for batch in batches:
        for item in batch:
            event: BronzeEvent = item.value
            columns["seq"].append(event.seq)
            columns["did"].append(event.did)
            columns["collection"].append(event.collection)
            columns["operation"].append(event.operation)
            columns["rkey"].append(event.rkey)
            columns["rev"].append(event.rev)
            columns["cid"].append(event.cid)
            columns["event_time"].append(event.event_time)
            columns["record"].append(event.record)
            columns["kafka_partition"].append(batch.partition)
            columns["kafka_offset"].append(item.offset)
            columns["processed_at"].append(processed_at)
    return columns


def arrow_table_from_batches(batches: list[SinkBatch], processed_at: datetime) -> pa.Table:
    """Bronze rows as an Arrow table: DuckDB scans it without per-value conversion.

    Binding Python lists as query parameters converts every value one by one
    (~400 rows/s measured); scanning Arrow is ~190k rows/s on the same DuckLake.
    """
    return pa.Table.from_pydict(
        columns_from_batches(batches, processed_at), schema=BRONZE_ARROW_SCHEMA
    )


class DuckLakeBronzeSink(BatchingSink):
    """Writes each Quix checkpoint to DuckLake Bronze as one transaction."""

    def __init__(self, settings: DuckLakeSettings | None = None) -> None:
        super().__init__()
        self._settings = settings or DuckLakeSettings()
        self._table = f"{self._settings.alias}.main.{BRONZE_TABLE}"
        self._conn: duckdb.DuckDBPyConnection | None = None

    def setup(self) -> None:
        """Attach the lake and create the Bronze table if needed (called once by Quix)."""
        self._connection().execute(duckdb_ddl(self._table))

    def _connection(self) -> duckdb.DuckDBPyConnection:
        """Current connection, reopened after a storage or catalog failure."""
        if self._conn is None:
            self._conn = connect(self._settings)
        return self._conn

    def write(self, batch: SinkBatch) -> None:
        """Write a single partition batch (flush() normally writes all of them at once)."""
        self._insert([batch])

    def flush(self) -> None:
        """Write every partition of the checkpoint in one INSERT, then drop the batches."""
        try:
            batches = [batch for batch in self._batches.values() if batch.size]
            if batches:
                self._insert(batches)
        finally:
            self._batches.clear()

    def _insert(self, batches: list[SinkBatch]) -> None:
        processed_at = datetime.now(UTC)
        rows_table = arrow_table_from_batches(batches, processed_at)
        started = time.perf_counter()
        try:
            conn = self._connection()
            conn.register("bronze_batch", rows_table)
            try:
                conn.execute(
                    f"INSERT INTO {self._table} ({', '.join(BRONZE_COLUMN_NAMES)}) "
                    f"SELECT {', '.join(BRONZE_COLUMN_NAMES)} FROM bronze_batch"
                )
            finally:
                conn.unregister("bronze_batch")
        except (duckdb.IOException, duckdb.HTTPException, duckdb.ConnectionException) as e:
            # Garage or Postgres unavailable: offsets are not committed, Quix pauses and
            # seeks back to the checkpoint start, then retries with a fresh connection
            logger.warning(
                f"DuckLake write failed ({e}), retrying in {BACKPRESSURE_RETRY_SECONDS}s"
            )
            self._close()
            raise SinkBackpressureError(retry_after=BACKPRESSURE_RETRY_SECONDS) from e
        elapsed_ms = (time.perf_counter() - started) * 1000
        logger.info(
            f"Bronze commit: {rows_table.num_rows} rows from {len(batches)} partitions "
            f"in {elapsed_ms:.0f} ms"
        )

    def _close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


def build_app() -> Application:
    """Quix application on `raw_events`, with the branch B consumer group."""
    return Application(
        broker_address=config.KAFKA_BOOTSTRAP_SERVERS,
        consumer_group=config.QUIX_CONSUMER_GROUP,
        auto_offset_reset=config.QUIX_AUTO_OFFSET_RESET,
        commit_interval=config.QUIX_COMMIT_INTERVAL_SECONDS,
        commit_every=config.QUIX_COMMIT_EVERY,
        consumer_extra_config=config.KAFKA_CLIENT_CONFIG,
        producer_extra_config=config.KAFKA_CLIENT_CONFIG,
        # raw_events is created and configured by the producer
        auto_create_topics=False,
    )


def main() -> None:
    """Entry point: raw_events → parse → DuckLake Bronze."""
    app = build_app()
    topic = app.topic(config.RAW_EVENTS_TOPIC, value_deserializer="json")
    sdf = app.dataframe(topic)
    sdf = sdf.apply(parse_bronze_event).filter(lambda event: event is not None)
    sdf.sink(DuckLakeBronzeSink())
    app.run()


if __name__ == "__main__":
    main()
