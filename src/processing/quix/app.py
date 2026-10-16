"""
DuckDB branch streaming application.
Consumes `raw_events` and writes micro-batches to the DuckLake Bronze table on Garage (S3).

Delivery is at-least-once: Quix commits Kafka offsets only after the sink flush of a
checkpoint succeeds. All partitions of a checkpoint go in a single INSERT, so each
checkpoint (every QUIX_COMMIT_INTERVAL_SECONDS) is exactly one DuckLake snapshot, the
same cadence as Spark's 5 s trigger in the Spark branch.

Live checkpoints are inlined in the Postgres catalog and flushed to Parquet every few
minutes by the sink itself (decision D15). Rows still inlined when the app stops are
flushed at the next start or by the nightly maintenance (phase 3). Bronze has its own
DuckLake catalog: dbt commits Silver and Gold in another one, so they never compete with
the sink for snapshot ids. Every write holds the Bronze write lock shared: while the
nightly maintenance holds it, the sink pauses and catches up afterwards.

Started by the supervisor (decision D28), the app works on one benchmark day: it starts
from the day's start offsets, drops what is past its end offsets, records when it caught
up, and marks the day done once its commits reach the end (src.alternation.engine).
Started with BENCH_RUN instead (controlled test, D36), it works on one run the same way:
the replay topic, its own consumer group and a separate DuckLake catalog
(src.benchmark.runs). Run by hand without either, it consumes without bounds (local
development).
"""

import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import duckdb
import psycopg2
import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger
from quixstreams import Application, message_context
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

from src import config
from src.alternation.engine import EngineDay
from src.benchmark.runs import BenchRun
from src.processing.bronze import (
    BRONZE_COLUMN_NAMES,
    BRONZE_COLUMNS,
    BRONZE_TABLE,
    BronzeEvent,
    duckdb_ddl,
    duckdb_partition_ddl,
    parse_bronze_event,
)
from src.resources import redpanda
from src.resources.bronze_lock import SharedBronzeLock
from src.resources.ducklake import (
    DuckLakeSettings,
    bench_settings,
    connect,
    set_write_options,
    sql_literal,
)

# Pause consumption this long when Garage or the Postgres catalog is unreachable
BACKPRESSURE_RETRY_SECONDS = 10.0
# How often the app checks whether its committed offsets reached the day's end
COMPLETION_CHECK_SECONDS = 10.0

# Errors a fresh connection recovers from (the INSERT is atomic, so replaying the
# checkpoint is safe): Garage or Postgres down; DuckLake's commit retries exhausted
# while another writer of the Bronze catalog (the nightly CHECKPOINT) takes the snapshot
# id; DuckLake's internal error when this flush races the flush of a concurrent
# CHECKPOINT ("index 0 within vector of size 0"), which invalidates the DuckDB instance
RECOVERABLE_ERRORS = (
    duckdb.IOException,
    duckdb.HTTPException,
    duckdb.ConnectionException,
    duckdb.TransactionException,
    duckdb.InternalException,
    duckdb.FatalException,
    # The Bronze write lock's Postgres session
    psycopg2.Error,
)

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
    """Writes each Quix checkpoint to DuckLake Bronze as one transaction.

    Live checkpoints stay below the inlining limit and land in the Postgres catalog;
    every `inlined_flush_interval` seconds the sink moves them to Parquet (decision D15).
    """

    def __init__(
        self,
        settings: DuckLakeSettings | None = None,
        inlined_flush_interval: float = config.DUCKLAKE_INLINED_FLUSH_INTERVAL_SECONDS,
        lock: SharedBronzeLock | None = None,
        on_commit: Callable[[pa.Table], None] | None = None,
    ) -> None:
        super().__init__()
        self._settings = settings or DuckLakeSettings()
        self._lock = lock or SharedBronzeLock(self._settings)
        # Called with the rows of each committed checkpoint (caught-up detection, D28)
        self._on_commit = on_commit
        self._table = f"{self._settings.alias}.main.{BRONZE_TABLE}"
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._inlined_flush_interval = inlined_flush_interval
        self._last_inlined_flush = time.monotonic()

    def setup(self) -> None:
        """Attach the lake, set write options, create and partition Bronze, flush leftovers.

        Options and partitioning only apply to files written afterwards, so they must be
        in place before the first insert (decision D14).
        """
        conn = self._connection()
        set_write_options(conn, self._settings.alias)
        conn.execute(duckdb_ddl(self._table))
        conn.execute(duckdb_partition_ddl(self._table))
        # Rows left inlined by the previous run (stopped before its next flush)
        self._flush_inlined()

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
        if time.monotonic() - self._last_inlined_flush >= self._inlined_flush_interval:
            self._flush_inlined()

    def _flush_inlined(self) -> None:
        """Move inlined rows from the catalog to Parquet files (one per day partition).

        Runs after the checkpoint insert succeeded, so a failure here must not raise
        SinkBackpressureError: Quix would replay an already written checkpoint. The rows
        are safe in Postgres and the next checkpoint retries the flush.
        """
        self._last_inlined_flush = time.monotonic()
        alias, schema, table = self._table.split(".")
        started = time.perf_counter()
        try:
            if not self._lock.try_acquire():
                logger.info("Bronze maintenance running, inlined flush postponed")
                self._last_inlined_flush = float("-inf")
                return
            try:
                flushed = (
                    self._connection()
                    .execute(
                        f"CALL ducklake_flush_inlined_data({sql_literal(alias)}, "
                        f"schema_name => {sql_literal(schema)}, "
                        f"table_name => {sql_literal(table)})"
                    )
                    .fetchall()
                )
            finally:
                self._lock.release()
        except RECOVERABLE_ERRORS as e:
            logger.warning(f"Inlined data flush failed ({e}), retrying at the next checkpoint")
            self._close()
            # -inf, not 0: monotonic() counts from boot, so 0 is less than one interval
            # ago on a host that started recently and the retry would wait
            self._last_inlined_flush = float("-inf")
            return
        elapsed_ms = (time.perf_counter() - started) * 1000
        rows = sum(row[-1] for row in flushed)
        logger.info(f"Inlined flush: {rows} rows moved to Parquet in {elapsed_ms:.0f} ms")

    def _insert(self, batches: list[SinkBatch]) -> None:
        processed_at = datetime.now(UTC)
        rows_table = arrow_table_from_batches(batches, processed_at)
        started = time.perf_counter()
        try:
            if not self._lock.try_acquire():
                # The nightly maintenance holds Bronze: offsets stay uncommitted and Quix
                # replays the checkpoint once it is done (not an error)
                logger.info(
                    f"Bronze maintenance running, pausing {config.QUIX_LOCKED_RETRY_SECONDS:.0f}s"
                )
                raise SinkBackpressureError(retry_after=config.QUIX_LOCKED_RETRY_SECONDS)
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
            finally:
                self._lock.release()
        except RECOVERABLE_ERRORS as e:
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
        if self._on_commit is not None:
            self._on_commit(rows_table)

    def _close(self) -> None:
        """Drop the connections; never raises, the instance may already be invalidated."""
        self._lock.close()
        if self._conn is not None:
            try:
                self._conn.close()
            except duckdb.Error as e:
                logger.warning(f"Closing the DuckDB connection failed ({e})")
            finally:
                self._conn = None


def build_app(consumer_group: str = config.QUIX_CONSUMER_GROUP) -> Application:
    """Quix application with the DuckDB branch consumer group (its bench group in the
    controlled test)."""
    return Application(
        broker_address=config.KAFKA_BOOTSTRAP_SERVERS,
        consumer_group=consumer_group,
        auto_offset_reset=config.QUIX_AUTO_OFFSET_RESET,
        commit_interval=config.QUIX_COMMIT_INTERVAL_SECONDS,
        commit_every=config.QUIX_COMMIT_EVERY,
        consumer_extra_config={
            **config.KAFKA_CLIENT_CONFIG,
            "session.timeout.ms": config.QUIX_SESSION_TIMEOUT_MS,
        },
        producer_extra_config=config.KAFKA_CLIENT_CONFIG,
        # raw_events is created and configured by the producer
        auto_create_topics=False,
    )


def newest_event_time(rows: pa.Table) -> datetime | None:
    """Most recent event_time of a committed checkpoint (None when empty)."""
    return pc.max(rows.column("event_time")).as_py()


def in_day(day: EngineDay | BenchRun) -> Callable[[Any], bool]:
    """Filter keeping the messages before the day's end offsets (known from 19:00)."""

    def keep(_value: Any) -> bool:
        context = message_context()
        return day.in_bounds(context.partition, context.offset)

    return keep


def watch_completion(
    day: EngineDay | BenchRun,
    stop: threading.Event,
    group: str = config.QUIX_CONSUMER_GROUP,
    topic: str = config.RAW_EVENTS_TOPIC,
) -> None:
    """Mark the day (or run) done once the group's committed offsets reach its end. Quix
    commits only after the sink flushed, so every message before the end is in Bronze."""
    while not stop.wait(COMPLETION_CHECK_SECONDS):
        try:
            position = redpanda.committed_offsets(group, topic)
        except Exception as e:  # noqa: BLE001 - checked again at the next interval
            logger.warning(f"Reading the committed offsets failed ({e})")
            continue
        if day.check_complete(position):
            return


def main() -> None:
    """Entry point: raw_events → (day bounds) → parse → DuckLake Bronze; in bench mode,
    the replay topic → (run bounds) → parse → the bench catalog's Bronze."""
    bench = BenchRun.from_env()
    day = bench or EngineDay.from_env()
    group = config.BENCH_QUIX_CONSUMER_GROUP if bench else config.QUIX_CONSUMER_GROUP
    topic_name = config.BENCH_TOPIC if bench else config.RAW_EVENTS_TOPIC
    settings = bench_settings(DuckLakeSettings()) if bench else DuckLakeSettings()
    if day is not None:
        # Before the app joins the group: committing for an empty group is safe
        day.prepare(lambda offsets: redpanda.commit_offsets(group, offsets, topic_name))
    app = build_app(group)
    topic = app.topic(topic_name, value_deserializer="json")
    sdf = app.dataframe(topic)
    if day is not None:
        sdf = sdf.filter(in_day(day))
    sdf = sdf.apply(parse_bronze_event).filter(lambda event: event is not None)
    on_commit = None if day is None else lambda rows: day.record_commit(newest_event_time(rows))
    sdf.sink(DuckLakeBronzeSink(settings=settings, on_commit=on_commit))
    stop = threading.Event()
    if day is not None:
        threading.Thread(
            target=watch_completion, args=(day, stop, group, topic_name), daemon=True
        ).start()
    try:
        app.run()
    finally:
        stop.set()


if __name__ == "__main__":
    main()
