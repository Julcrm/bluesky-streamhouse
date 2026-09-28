"""Unit tests for src.processing.silver_backlog, on a local DuckLake (file catalog, no stack)."""

import duckdb
import pytest

from src import config
from src.processing.silver_backlog import (
    bronze_rows_after,
    silver_max_rows_per_run,
    silver_read_snapshot,
)


@pytest.fixture
def bronze(tmp_path) -> duckdb.DuckDBPyConnection:
    """A DuckLake catalog attached under the Bronze alias, with an empty Bronze table."""
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake")
    alias = config.DUCKLAKE_BRONZE_ALIAS
    conn.execute(
        f"ATTACH 'ducklake:{tmp_path}/bronze.ducklake' AS {alias} "
        f"(DATA_PATH '{tmp_path}/data/', DATA_INLINING_ROW_LIMIT 0)"
    )
    conn.execute(f"CREATE TABLE {alias}.main.bronze_events (seq BIGINT)")
    yield conn
    conn.close()


def _insert(conn: duckdb.DuckDBPyConnection, rows: int) -> int:
    """Insert `rows` Bronze rows in one snapshot and return that snapshot id."""
    alias = config.DUCKLAKE_BRONZE_ALIAS
    conn.execute(f"INSERT INTO {alias}.main.bronze_events SELECT range FROM range({rows})")
    return conn.execute(f"SELECT id FROM ducklake_current_snapshot('{alias}')").fetchone()[0]


def test_max_rows_per_run_comes_from_dbt_project() -> None:
    """The cap is the dbt var, not a second copy in Python."""
    assert silver_max_rows_per_run() == 500_000


def test_rows_after_counts_inserts_since_snapshot(bronze) -> None:
    """Only rows inserted after the given snapshot are pending."""
    first = _insert(bronze, 30)
    _insert(bronze, 20)
    _insert(bronze, 5)
    assert bronze_rows_after(bronze, first) == 25


def test_rows_after_whole_table_before_first_silver_run(bronze) -> None:
    """Without a Silver snapshot, every Bronze row is pending."""
    _insert(bronze, 30)
    _insert(bronze, 20)
    assert bronze_rows_after(bronze, None) == 50


def test_rows_after_zero_when_up_to_date(bronze) -> None:
    """A Silver run that read the current snapshot leaves no backlog."""
    current = _insert(bronze, 10)
    assert bronze_rows_after(bronze, current) == 0


def test_rows_after_ignores_snapshots_without_inserts(bronze) -> None:
    """Snapshots that insert nothing (flush, CHECKPOINT, DDL) add no backlog."""
    current = _insert(bronze, 10)
    bronze.execute(
        f"ALTER TABLE {config.DUCKLAKE_BRONZE_ALIAS}.main.bronze_events ADD COLUMN x INT"
    )
    assert bronze_rows_after(bronze, current) == 0


@pytest.fixture
def transform() -> duckdb.DuckDBPyConnection:
    """An in-memory database attached under the transform alias, with a silver schema."""
    conn = duckdb.connect()
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    conn.execute(f"ATTACH ':memory:' AS {alias}")
    conn.execute(f"CREATE SCHEMA {alias}.silver")
    yield conn
    conn.close()


def test_read_snapshot_none_without_silver_tables(transform) -> None:
    """Before the first dbt run there is nothing to resume from."""
    assert silver_read_snapshot(transform) is None


def test_read_snapshot_is_the_lowest_across_models(transform) -> None:
    """A model left behind by a failed run sets where the next run starts."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    transform.execute(
        f"CREATE TABLE {alias}.silver.silver_posts AS SELECT 12 AS bronze_snapshot_id"
    )
    transform.execute(f"CREATE TABLE {alias}.silver.silver_likes AS SELECT 9 AS bronze_snapshot_id")
    assert silver_read_snapshot(transform) == 9


def test_read_snapshot_none_when_a_model_is_empty(transform) -> None:
    """An empty Silver table (created, never filled) means a full first read."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    transform.execute(
        f"CREATE TABLE {alias}.silver.silver_posts AS SELECT 12 AS bronze_snapshot_id"
    )
    transform.execute(f"CREATE TABLE {alias}.silver.silver_likes (bronze_snapshot_id BIGINT)")
    assert silver_read_snapshot(transform) is None
