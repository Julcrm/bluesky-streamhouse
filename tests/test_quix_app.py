"""Unit tests for src.processing.quix.app, plus a sink round trip on the local stack."""

import uuid
from datetime import UTC, datetime

import duckdb
import pytest
from quixstreams.sinks import SinkBackpressureError, SinkBatch

from src import config
from src.processing.bronze import BRONZE_COLUMN_NAMES, parse_bronze_event
from src.processing.quix.app import (
    BRONZE_ARROW_SCHEMA,
    DuckLakeBronzeSink,
    arrow_table_from_batches,
    columns_from_batches,
)
from src.resources.ducklake import DuckLakeSettings, connect
from tests.test_bronze import _message
from tests.test_resources_ducklake import _local_stack_up

PROCESSED_AT = datetime(2026, 9, 26, 1, 0, tzinfo=UTC)


def _batch(partition: int, offsets: list[int], time: str = "2026-09-25T23:09:22Z") -> SinkBatch:
    """A sink batch holding one parsed event per offset (seq = offset)."""
    batch = SinkBatch(topic="raw_events", partition=partition)
    for offset in offsets:
        event = parse_bronze_event(_message(seq=offset, time=time))
        batch.append(value=event, key=None, timestamp=0, headers=[], offset=offset)
    return batch


def test_columns_carry_kafka_metadata_and_processed_at() -> None:
    """Each row gets its partition, offset and the single checkpoint write time."""
    columns = columns_from_batches([_batch(0, [10, 11]), _batch(2, [7])], PROCESSED_AT)
    assert list(columns) == list(BRONZE_COLUMN_NAMES)
    assert columns["kafka_partition"] == [0, 0, 2]
    assert columns["kafka_offset"] == [10, 11, 7]
    assert columns["seq"] == [10, 11, 7]
    assert set(columns["processed_at"]) == {PROCESSED_AT}


def test_arrow_table_matches_bronze_schema() -> None:
    """The Arrow table has exactly the contract columns and types."""
    table = arrow_table_from_batches([_batch(1, [1, 2, 3])], PROCESSED_AT)
    assert table.schema == BRONZE_ARROW_SCHEMA
    assert table.num_rows == 3


class _FakeConnection:
    """Records executed SQL; raises the given error on flush calls if set."""

    def __init__(self, error: Exception | None = None) -> None:
        self.statements: list[str] = []
        self.error = error

    def execute(self, sql: str) -> "_FakeConnection":
        self.statements.append(sql)
        if self.error is not None and "ducklake_flush_inlined_data" in sql:
            raise self.error
        return self

    def fetchall(self) -> list[tuple]:
        return [("main", "bronze_events", 0)]

    def close(self) -> None:
        pass


class _FakeLock:
    """Bronze write lock that is free, or held by the maintenance if `held`."""

    def __init__(self, held: bool = False) -> None:
        self.held = held
        self.holds = 0

    def try_acquire(self) -> bool:
        if self.held:
            return False
        self.holds += 1
        return True

    def release(self) -> None:
        self.holds -= 1

    def close(self) -> None:
        pass


def _sink_with(
    conn: _FakeConnection, interval: float, lock: _FakeLock | None = None
) -> DuckLakeBronzeSink:
    sink = DuckLakeBronzeSink(
        DuckLakeSettings(),
        inlined_flush_interval=interval,
        lock=lock or _FakeLock(),  # type: ignore[arg-type]
    )
    sink._conn = conn  # type: ignore[assignment]
    return sink


def test_inlined_flush_waits_for_the_interval() -> None:
    """No flush before the interval, one flush of the Bronze table once it elapsed."""
    conn = _FakeConnection()
    sink = _sink_with(conn, interval=3600)
    sink.flush()
    assert conn.statements == []
    sink._last_inlined_flush -= 3600
    sink.flush()
    assert conn.statements == [
        "CALL ducklake_flush_inlined_data('bronze', "
        "schema_name => 'main', table_name => 'bronze_events')"
    ]


@pytest.mark.parametrize(
    "error",
    [
        duckdb.IOException("garage down"),
        # Flush racing a concurrent CHECKPOINT, then the invalidated instance
        duckdb.InternalException("Attempted to access index 0 within vector of size 0"),
        duckdb.FatalException("database has been invalidated"),
    ],
)
def test_inlined_flush_failure_does_not_block_the_checkpoint(error: Exception) -> None:
    """A failed flush is logged, not raised (no replay), and retried at the next one."""
    sink = _sink_with(_FakeConnection(error), interval=3600)
    sink._last_inlined_flush -= 3600
    sink.flush()  # must not raise SinkBackpressureError
    assert sink._conn is None  # reconnects on the next write
    sink._conn = _FakeConnection()  # type: ignore[assignment]
    sink.flush()
    assert sink._conn.statements  # retried without waiting another interval


class _FailingInsertConnection(_FakeConnection):
    """Fails the INSERT like an invalidated DuckDB instance; close() fails too."""

    def register(self, name: str, table: object) -> None:
        pass

    def unregister(self, name: str) -> None:
        pass

    def execute(self, sql: str) -> "_FakeConnection":
        if sql.startswith("INSERT"):
            raise duckdb.FatalException("database has been invalidated")
        return super().execute(sql)

    def close(self) -> None:
        raise duckdb.FatalException("database has been invalidated")


def test_insert_on_invalidated_instance_replays_the_checkpoint() -> None:
    """Backpressure (Quix replays the checkpoint) and a fresh connection, not a crash."""
    sink = _sink_with(_FailingInsertConnection(), interval=3600)
    with pytest.raises(SinkBackpressureError):
        sink._insert([_batch(0, [1, 2])])
    assert sink._conn is None


class _RecordingInsertConnection(_FakeConnection):
    """Accepts the INSERT and its Arrow registration."""

    def register(self, name: str, table: object) -> None:
        pass

    def unregister(self, name: str) -> None:
        pass


def test_insert_pauses_while_the_maintenance_holds_bronze() -> None:
    """Maintenance running: no INSERT, backpressure for the lock's retry delay."""
    conn = _RecordingInsertConnection()
    sink = _sink_with(conn, interval=3600, lock=_FakeLock(held=True))
    with pytest.raises(SinkBackpressureError) as raised:
        sink._insert([_batch(0, [1, 2])])
    assert raised.value.retry_after == config.QUIX_LOCKED_RETRY_SECONDS
    assert conn.statements == []
    assert sink._conn is conn  # not a failure: the connection is kept


def test_insert_holds_the_lock_only_during_the_commit() -> None:
    """The shared hold is released after the INSERT, whatever its outcome."""
    lock = _FakeLock()
    sink = _sink_with(_RecordingInsertConnection(), interval=3600, lock=lock)
    sink._insert([_batch(0, [1, 2])])
    assert lock.holds == 0
    sink = _sink_with(_FailingInsertConnection(), interval=3600, lock=lock)
    with pytest.raises(SinkBackpressureError):
        sink._insert([_batch(0, [1])])
    assert lock.holds == 0


def test_inlined_flush_postponed_while_the_maintenance_holds_bronze() -> None:
    """No flush during the maintenance, retried at the next checkpoint without waiting."""
    conn = _FakeConnection()
    lock = _FakeLock(held=True)
    sink = _sink_with(conn, interval=3600, lock=lock)
    sink._last_inlined_flush -= 3600
    sink.flush()
    assert conn.statements == []
    lock.held = False
    sink.flush()
    assert len(conn.statements) == 1


@pytest.mark.skipif(not _local_stack_up(), reason="local stack not running (make up)")
def test_sink_writes_all_partitions_in_one_snapshot() -> None:
    """flush() writes every partition of a checkpoint as a single DuckLake snapshot."""
    table = f"test_bronze_{uuid.uuid4().hex[:8]}"
    sink = DuckLakeBronzeSink(DuckLakeSettings())
    sink._table = f"bronze.main.{table}"  # isolate from the real Bronze table
    sink.setup()
    conn = connect()
    try:
        before = conn.execute(
            "SELECT max(snapshot_id) FROM ducklake_snapshots('bronze')"
        ).fetchone()
        for partition, offsets in ((0, [1, 2]), (1, [3]), (2, [4, 5, 6])):
            for item in _batch(partition, offsets):
                sink.add(item.value, None, 0, [], "raw_events", partition, item.offset)
        sink.flush()
        after = conn.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('bronze')").fetchone()
        rows = conn.execute(
            f"SELECT count(*), count(DISTINCT kafka_partition) FROM bronze.main.{table}"
        ).fetchone()
        assert rows == (6, 3)
        assert after[0] == before[0] + 1
    finally:
        conn.execute(f"DROP TABLE IF EXISTS bronze.main.{table}")
        conn.close()
        sink._close()


@pytest.mark.skipif(not _local_stack_up(), reason="local stack not running (make up)")
def test_sink_inlines_then_flushes_zstd_files_split_by_utc_day() -> None:
    """A live checkpoint stays in the catalog until the inlined flush, which writes one
    zstd file per UTC day (events on both sides of midnight UTC)."""
    table = f"test_bronze_{uuid.uuid4().hex[:8]}"
    sink = DuckLakeBronzeSink(DuckLakeSettings(), inlined_flush_interval=3600)
    sink._table = f"bronze.main.{table}"
    sink.setup()
    sink.setup()  # restart: options and partitioning are re-applied without error
    conn = connect()
    try:
        # 23:30 UTC is already the next day in Europe/Paris: UTC must win
        for partition, time in ((0, "2026-09-25T23:30:00Z"), (1, "2026-09-26T00:30:00Z")):
            for item in _batch(partition, list(range(20)), time=time):
                sink.add(item.value, None, 0, [], "raw_events", partition, item.offset)
        sink.flush()
        list_files = f"SELECT data_file FROM ducklake_list_files('bronze', '{table}')"
        assert conn.execute(list_files).fetchall() == []  # inlined, readable already
        assert conn.execute(f"SELECT count(*) FROM bronze.main.{table}").fetchone() == (40,)
        sink._flush_inlined()
        files = [row[0] for row in conn.execute(list_files).fetchall()]
        assert conn.execute(f"SELECT count(*) FROM bronze.main.{table}").fetchone() == (40,)
        assert sorted(f.split(f"/{table}/")[1].rsplit("/", 1)[0] for f in files) == [
            "year=2026/month=9/day=25",
            "year=2026/month=9/day=26",
        ]
        codecs = conn.execute(
            "SELECT DISTINCT compression FROM parquet_metadata($files)", {"files": files}
        ).fetchall()
        assert codecs == [("ZSTD",)]
    finally:
        conn.execute(f"DROP TABLE IF EXISTS bronze.main.{table}")
        conn.close()
        sink._close()
