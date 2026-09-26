"""Unit tests for src.processing.quix.app, plus a sink round trip on the local stack."""

import uuid
from datetime import UTC, datetime

import pytest
from quixstreams.sinks import SinkBatch

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


def _batch(partition: int, offsets: list[int]) -> SinkBatch:
    """A sink batch holding one parsed event per offset (seq = offset)."""
    batch = SinkBatch(topic="raw_events", partition=partition)
    for offset in offsets:
        event = parse_bronze_event(_message(seq=offset))
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


@pytest.mark.skipif(not _local_stack_up(), reason="local stack not running (make up)")
def test_sink_writes_all_partitions_in_one_snapshot() -> None:
    """flush() writes every partition of a checkpoint as a single DuckLake snapshot."""
    table = f"test_bronze_{uuid.uuid4().hex[:8]}"
    sink = DuckLakeBronzeSink(DuckLakeSettings())
    sink._table = f"lake.main.{table}"  # isolate from the real Bronze table
    sink.setup()
    conn = connect()
    try:
        before = conn.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()
        for partition, offsets in ((0, [1, 2]), (1, [3]), (2, [4, 5, 6])):
            for item in _batch(partition, offsets):
                sink.add(item.value, None, 0, [], "raw_events", partition, item.offset)
        sink.flush()
        after = conn.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()
        rows = conn.execute(
            f"SELECT count(*), count(DISTINCT kafka_partition) FROM lake.main.{table}"
        ).fetchone()
        assert rows == (6, 3)
        assert after[0] == before[0] + 1
    finally:
        conn.execute(f"DROP TABLE IF EXISTS lake.main.{table}")
        conn.close()
        sink._close()
