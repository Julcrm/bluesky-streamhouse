"""Unit tests for src.maintenance.iceberg: the SQL sent to the Thrift server, with a fake
runner (no JVM). The procedures themselves were run against Lakekeeper and Garage locally."""

from datetime import UTC, datetime
from typing import Any

from src import config
from src.maintenance import iceberg

TABLE = "lakekeeper.bronze.bronze_events"
SILVER = "lakekeeper.silver.silver_posts"
NOW = datetime(2026, 10, 10, 2, 30, tzinfo=UTC)


class FakeSql:
    """Records every statement; answers from (substring, rows) rules, first match wins."""

    def __init__(self, rules: list[tuple[str, list[dict[str, Any]]]] | None = None) -> None:
        self.log: list[str] = []
        self.rules = rules or []

    def __call__(self, statement: str) -> list[dict[str, Any]]:
        self.log.append(statement)
        for needle, rows in self.rules:
            if needle in statement:
                return rows
        return []


def test_cutoff_is_a_utc_midnight() -> None:
    assert iceberg.retention_cutoff(7, NOW) == datetime(2026, 10, 3, tzinfo=UTC)


def test_retention_drops_whole_days_and_counts_from_snapshot_summaries() -> None:
    sql = FakeSql(
        [
            ("max(committed_at)", [{"c": "2026-10-09 20:00:00"}]),
            ("deleted-records", [{"c": 42}]),
        ]
    )
    assert iceberg.delete_expired_rows(sql, TABLE, "event_time", 7, NOW) == 42
    last, delete, count = sql.log
    assert delete == f"DELETE FROM {TABLE} WHERE event_time < TIMESTAMP '2026-10-03 00:00:00'"
    # Only the commits of this DELETE are counted, net of rewritten rows
    assert "committed_at > TIMESTAMP '2026-10-09 20:00:00'" in count
    assert "operation IN ('delete', 'overwrite')" in count
    assert "added-records" in count


def test_retention_of_a_table_without_snapshot_counts_every_commit() -> None:
    sql = FakeSql([("max(committed_at)", [{"c": None}]), ("deleted-records", [{"c": 0}])])
    assert iceberg.delete_expired_rows(sql, TABLE, "event_time", 7, NOW) == 0
    assert "committed_at >" not in sql.log[-1]


def test_trim_keeps_the_last_done_position_of_each_reader() -> None:
    """Iceberg snapshot ids are random: the last position is the latest recorded."""
    sql = FakeSql([("deleted-records", [{"c": 5}])])
    table = iceberg.GOLD_PROGRESS
    assert iceberg.trim_progress(sql, table, ("model", "silver_model"), NOW) == 5
    delete = sql.log[1]
    assert "recorded_at < TIMESTAMP '2026-10-03 02:30:00'" in delete
    assert (
        "(model, silver_model, recorded_at) NOT IN (SELECT model, silver_model, "
        "max(recorded_at)" in delete
    )
    assert "WHERE done GROUP BY model, silver_model" in delete


def test_time_column_by_order_of_preference() -> None:
    sql = FakeSql([("DESCRIBE", [{"col_name": "hour"}, {"col_name": "collection"}])])
    assert iceberg.time_column(sql, "lakekeeper.gold.gold_langs_hour") == "hour"


def test_tables_found_in_the_catalog() -> None:
    sql = FakeSql([("SHOW TABLES", [{"tableName": "silver_posts"}, {"tableName": "silver_likes"}])])
    assert iceberg.list_tables(sql, "silver") == [
        "lakekeeper.silver.silver_likes",
        "lakekeeper.silver.silver_posts",
    ]


def test_positions_of_silver_on_bronze_and_gold_on_silver() -> None:
    sql = FakeSql(
        [
            ("FROM lakekeeper.meta.silver_progress", [{"model": "silver_posts", "position": 7}]),
            (
                "FROM lakekeeper.meta.gold_progress",
                [{"model": "gold_langs_hour", "silver_model": "silver_posts", "position": 9}],
            ),
        ]
    )
    positions = iceberg.read_positions(sql, {iceberg.SILVER_PROGRESS, iceberg.GOLD_PROGRESS})
    assert positions == {
        "silver_posts": (TABLE, 7),
        "gold_langs_hour <- silver_posts": (SILVER, 9),
    }
    assert all("max_by(" in s for s in sql.log)


def test_no_position_table_no_reader() -> None:
    sql = FakeSql()
    assert iceberg.read_positions(sql, set()) == {}
    assert sql.log == []


def test_guard_flags_old_unread_snapshots_and_expired_positions() -> None:
    sql = FakeSql(
        [
            ("snapshot_id = 1", [{"oldest": "2026-10-08 19:00:00", "position_kept": 1}]),
            ("committed_at > TIMESTAMP '2026-10-08 19:00:00'", [{"c": "2026-10-08 19:05:00"}]),
            ("snapshot_id = 2", [{"oldest": None, "position_kept": 0}]),
            ("snapshot_id = 3", [{"oldest": "2026-10-09 18:00:00", "position_kept": 1}]),
            ("committed_at > TIMESTAMP '2026-10-09 18:00:00'", [{"c": "2026-10-09 19:00:00"}]),
            ("snapshot_id = 4", [{"oldest": "2026-10-09 20:00:00", "position_kept": 1}]),
        ]
    )
    positions = {
        "silver_posts": (TABLE, 1),
        "silver_likes": (TABLE, 2),
        "silver_follows": (TABLE, 3),
        "silver_deletes": (TABLE, 4),
        "gold_langs_hour <- silver_posts": (SILVER, 1),
    }
    assert iceberg.stale_readers(sql, positions, {TABLE}, NOW) == {
        "silver_posts": "2026-10-08 19:05:00",
        "silver_likes": "position 2 already expired",
    }


def test_expire_keeps_24_hours_of_time_travel() -> None:
    """Same time travel window as DuckLake (D21)."""
    sql = FakeSql([("expire_snapshots", [{"deleted_data_files_count": 3}])])
    assert iceberg.expire_snapshots(sql, TABLE, NOW) == {"deleted_data_files_count": 3}
    (call,) = sql.log
    assert call.startswith("CALL lakekeeper.system.expire_snapshots(")
    assert "older_than => TIMESTAMP '2026-10-09 02:30:00'" in call
    assert "retain_last => 1" in call


def test_orphans_listed_by_file_io_and_older_than_a_day() -> None:
    """No Hadoop S3 filesystem: prefix listing through S3FileIO; Iceberg refuses < 24 h."""
    sql = FakeSql([("remove_orphan_files", [{"orphan_file_location": "s3://a"}] * 2)])
    assert iceberg.remove_orphan_files(sql, TABLE, NOW) == 2
    (call,) = sql.log
    assert "prefix_listing => true" in call
    assert "older_than => TIMESTAMP '2026-10-09 01:30:00'" in call
    assert config.ICEBERG_ORPHAN_MIN_AGE_HOURS > 24


def test_rewrite_targets_duckdb_file_size_one_group_at_a_time() -> None:
    sql = FakeSql()
    iceberg.rewrite_data_files(sql, TABLE)
    (call,) = sql.log
    assert f"'target-file-size-bytes', '{512 * 1024 * 1024}'" in call
    assert "'partial-progress.enabled', 'true'" in call
    assert "'max-concurrent-file-group-rewrites', '1'" in call


def test_table_stats_from_metadata_tables() -> None:
    sql = FakeSql(
        [
            (".files", [{"files": 42, "bytes": 1000}]),
            (".snapshots", [{"c": 7}]),
            (".manifests", [{"c": 3}]),
        ]
    )
    stats = iceberg.table_stats(sql, TABLE)
    assert stats == iceberg.TableStats(42, 1000, 7, 3)
    assert stats + stats == iceberg.TableStats(84, 2000, 14, 6)


def test_hourly_compaction_rewrites_today_only() -> None:
    """The streaming job writes into today's partition: older days are already compacted."""
    sql = FakeSql()
    iceberg.rewrite_data_files(sql, TABLE, iceberg.today_filter("event_time", NOW))
    (call,) = sql.log
    assert call.endswith(", where => \"event_time >= TIMESTAMP '2026-10-10 00:00:00'\")")
