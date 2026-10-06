"""Unit tests for src.maintenance.ducklake, on a local DuckLake (file catalog, no stack)."""

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from src import config
from src.maintenance import ducklake as maintenance
from src.resources.ducklake import DuckLakeSettings

ALIAS = config.DUCKLAKE_TRANSFORM_ALIAS
NOW = datetime(2026, 10, 10, 2, 0, tzinfo=UTC)


@pytest.fixture
def lake(tmp_path) -> duckdb.DuckDBPyConnection:
    """A DuckLake catalog under the transform alias, with Silver, Gold and meta tables."""
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake; SET TimeZone = 'UTC'")
    conn.execute(
        f"ATTACH 'ducklake:{tmp_path}/lake.ducklake' AS {ALIAS} "
        f"(DATA_PATH '{tmp_path}/data/', DATA_INLINING_ROW_LIMIT 0)"
    )
    for schema in ("silver", "gold", "meta"):
        conn.execute(f"CREATE SCHEMA {ALIAS}.{schema}")
    # One row per day from 2026-09-01 to 2026-10-09
    conn.execute(
        f"CREATE TABLE {ALIAS}.silver.silver_likes AS SELECT TIMESTAMPTZ '2026-09-01 12:00:00+00' "
        "+ INTERVAL (d) DAY AS event_time FROM range(39) AS t(d)"
    )
    conn.execute(
        f"CREATE TABLE {ALIAS}.gold.gold_langs_hour AS SELECT TIMESTAMPTZ '2026-09-01 12:00:00+00' "
        "+ INTERVAL (d) DAY AS hour FROM range(39) AS t(d)"
    )
    conn.execute(
        f"CREATE TABLE {ALIAS}.gold.gold_activity_minute AS SELECT TIMESTAMPTZ "
        "'2026-09-01 12:00:00+00' + INTERVAL (d) DAY AS minute FROM range(39) AS t(d)"
    )
    yield conn
    conn.close()


def test_cutoff_is_a_utc_midnight() -> None:
    """Whole days only, so a DELETE drops whole day files."""
    assert maintenance.retention_cutoff(7, NOW) == datetime(2026, 10, 3, tzinfo=UTC)


def test_time_column_by_table(lake) -> None:
    """Silver uses event_time, Gold its hour or minute key."""
    assert maintenance.time_column(lake, ALIAS, "silver", "silver_likes") == "event_time"
    assert maintenance.time_column(lake, ALIAS, "gold", "gold_langs_hour") == "hour"
    assert maintenance.time_column(lake, ALIAS, "gold", "gold_activity_minute") == "minute"


def test_delete_keeps_each_schema_retention(lake) -> None:
    """Silver keeps 7 whole days before today, Gold 30, every table of the schema."""
    deleted = maintenance.delete_expired_rows(lake, ALIAS, {"silver": 7, "gold": 30}, NOW)
    assert deleted == {
        "silver.silver_likes": 32,
        "gold.gold_activity_minute": 9,
        "gold.gold_langs_hour": 9,
    }
    oldest = lake.execute(f"SELECT min(event_time) FROM {ALIAS}.silver.silver_likes").fetchone()[0]
    assert oldest == datetime(2026, 10, 3, 12, 0, tzinfo=UTC)


def test_trim_progress_keeps_last_done_position(lake) -> None:
    """Old positions go, but never a model's last done one, however old."""
    old, recent = NOW - timedelta(days=20), NOW - timedelta(hours=1)
    lake.execute(
        f"CREATE TABLE {ALIAS}.meta.silver_progress AS SELECT * FROM (VALUES "
        "('a', 'silver_likes', 10, true, ?::TIMESTAMPTZ), "
        "('b', 'silver_likes', 20, true, ?::TIMESTAMPTZ), "
        "('c', 'silver_deletes', 5, true, ?::TIMESTAMPTZ), "
        "('d', 'silver_deletes', 6, false, ?::TIMESTAMPTZ)"
        ") AS t(invocation_id, model, bronze_snapshot_id, done, recorded_at)",
        [old, recent, old, old],
    )
    assert maintenance.trim_silver_progress(lake, NOW) == 2
    kept = lake.execute(
        f"SELECT model, bronze_snapshot_id FROM {ALIAS}.meta.silver_progress ORDER BY 1"
    ).fetchall()
    assert kept == [("silver_deletes", 5), ("silver_likes", 20)]


def test_catalog_stats_reads_metadata(lake) -> None:
    """Active files and bytes, snapshots, files waiting for deletion."""
    settings = DuckLakeSettings(alias=ALIAS, metadata_schema="main")
    stats = maintenance.catalog_stats(lake, settings)
    assert stats.active_files == 3
    assert stats.active_bytes > 0
    assert stats.snapshots >= 4
    assert stats.files_pending_deletion == 0


def test_stale_positions_only_with_old_pending_snapshots(lake) -> None:
    """A reader behind old snapshots is stale; an up-to-date or never-run one is not,
    however old its position (a day when the branch is off)."""
    current = lake.execute(f"SELECT id FROM ducklake_current_snapshot('{ALIAS}')").fetchone()[0]
    positions = {"behind": current - 2, "up_to_date": current, "never_ran": None}
    later = datetime.now(UTC) + timedelta(days=2)
    assert set(maintenance.stale_positions(lake, ALIAS, positions, 24, later)) == {"behind"}
    assert maintenance.stale_positions(lake, ALIAS, positions, 24, datetime.now(UTC)) == {}


class _ConflictingDeletes:
    """Real catalog, but the first `failures` DELETEs lose to a concurrent insert."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, failures: int) -> None:
        self.conn = conn
        self.failures = failures

    def execute(self, sql: str, params: list | None = None) -> duckdb.DuckDBPyConnection:
        if sql.startswith("DELETE") and self.failures:
            self.failures -= 1
            raise duckdb.TransactionException("another transaction has inserted into it")
        return self.conn.execute(sql, params)


def test_delete_retries_conflicts_with_quix_inserts(lake) -> None:
    """Quix inserting during the DELETE (prod, 2026-10-05/06): retried, rows still go."""
    waits = []
    conn = _ConflictingDeletes(lake, failures=2)
    deleted = maintenance.delete_expired_rows(conn, ALIAS, {"silver": 7}, NOW, 3, 10, waits.append)
    assert deleted == {"silver.silver_likes": 32}
    assert waits == [10, 20]


def test_delete_gives_up_after_retries(lake) -> None:
    """Still conflicting after every retry: the error reaches Dagster (alert)."""
    conn = _ConflictingDeletes(lake, failures=10)
    with pytest.raises(duckdb.TransactionException):
        maintenance.delete_expired_rows(conn, ALIAS, {"silver": 7}, NOW, 3, 0, lambda _: None)
    assert conn.failures == 6


class _ConflictingConnection:
    """Fails the first `failures` CHECKPOINTs with a commit conflict."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def execute(self, sql: str) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise duckdb.TransactionException("conflict on snapshot id")


def test_checkpoint_retries_conflicts_with_growing_backoff() -> None:
    """Quix may win the snapshot id: retried, with a longer wait each time."""
    waits = []
    conn = _ConflictingConnection(failures=2)
    assert maintenance.checkpoint(conn, ALIAS, 3, 10, waits.append) == 3
    assert waits == [10, 20]


def test_checkpoint_gives_up_after_retries() -> None:
    """Still conflicting after every retry: the error reaches Dagster (alert)."""
    conn = _ConflictingConnection(failures=10)
    with pytest.raises(duckdb.TransactionException):
        maintenance.checkpoint(conn, ALIAS, 3, 0, lambda _: None)
    assert conn.calls == 4
