"""Unit tests for src.maintenance.iceberg: the SQL sent to Spark, with a fake session
(no JVM). The procedures themselves were run against Lakekeeper and Garage locally."""

from datetime import UTC, datetime

import pytest

pytest.importorskip("pyspark")

from src import config  # noqa: E402
from src.maintenance import iceberg  # noqa: E402

TABLE = "lakekeeper.bronze.bronze_events"
NOW = datetime(2026, 10, 10, 2, 30, tzinfo=UTC)


class _Row(dict):
    def asDict(self) -> dict:  # noqa: N802 (Spark Row API)
        return dict(self)


class _Result:
    def __init__(self, rows: list) -> None:
        self.rows = rows

    def collect(self) -> list:
        return self.rows


class FakeSpark:
    """Records every statement; answers counts and procedure rows."""

    def __init__(self) -> None:
        self.sql_log: list[str] = []

    def sql(self, statement: str) -> _Result:
        self.sql_log.append(statement)
        if statement.startswith("SELECT"):
            return _Result([(42, 1000)])
        if statement.startswith("CALL") and "remove_orphan_files" in statement:
            return _Result([("s3://a",), ("s3://b",)])
        if statement.startswith("CALL"):
            return _Result([_Row(count=3)])
        return _Result([])


def test_cutoff_is_a_utc_midnight() -> None:
    assert iceberg.retention_cutoff(7, NOW) == datetime(2026, 10, 3, tzinfo=UTC)


def test_retention_deletes_whole_days_and_counts_them_first() -> None:
    spark = FakeSpark()
    assert iceberg.delete_expired_rows(spark, TABLE, "event_time", 7, NOW) == 42
    count, delete = spark.sql_log
    assert "partition.event_time_day < DATE '2026-10-03'" in count
    assert delete == f"DELETE FROM {TABLE} WHERE event_time < TIMESTAMP '2026-10-03 00:00:00'"


def test_expire_keeps_24_hours_of_time_travel() -> None:
    """Same time travel window as DuckLake (D21)."""
    spark = FakeSpark()
    assert iceberg.expire_snapshots(spark, TABLE, NOW) == {"count": 3}
    (call,) = spark.sql_log
    assert "expire_snapshots(" in call
    assert "older_than => TIMESTAMP '2026-10-09 02:30:00'" in call
    assert "retain_last => 1" in call


def test_orphans_listed_by_file_io_and_older_than_a_day() -> None:
    """No Hadoop S3 filesystem: prefix listing through S3FileIO; Iceberg refuses < 24 h."""
    spark = FakeSpark()
    assert iceberg.remove_orphan_files(spark, TABLE, NOW) == 2
    (call,) = spark.sql_log
    assert "prefix_listing => true" in call
    assert "older_than => TIMESTAMP '2026-10-09 01:30:00'" in call
    assert config.ICEBERG_ORPHAN_MIN_AGE_HOURS > 24


def test_rewrite_targets_duckdb_file_size_one_group_at_a_time() -> None:
    spark = FakeSpark()
    iceberg.rewrite_data_files(spark, TABLE)
    (call,) = spark.sql_log
    assert f"'target-file-size-bytes', '{512 * 1024 * 1024}'" in call
    assert "'partial-progress.enabled', 'true'" in call
    assert "'max-concurrent-file-group-rewrites', '1'" in call


def test_table_stats_from_metadata_tables() -> None:
    spark = FakeSpark()
    stats = iceberg.table_stats(spark, TABLE)
    assert stats.data_files == 42 and stats.data_bytes == 1000
    assert any(f"FROM {TABLE}.snapshots" in s for s in spark.sql_log)
