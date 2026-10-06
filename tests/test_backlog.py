"""Unit tests for src.processing.backlog, on a local DuckLake (file catalog, no stack)."""

import json

import duckdb
import pytest
import yaml

from src import config
from src.processing.backlog import (
    Backlog,
    bronze_rows_after,
    gold_hours_behind,
    gold_inputs,
    gold_max_hours_per_run,
    gold_positions,
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


def test_caps_come_from_dbt_project() -> None:
    """The caps are dbt vars, not a second copy in Python."""
    assert silver_max_rows_per_run() == 500_000
    assert gold_max_hours_per_run() == 6


def test_dbt_writes_the_bronze_codec() -> None:
    """Silver and Gold use the Parquet codec of Bronze (D14): Snappy slipped into prod."""
    with open(config.DBT_DUCKDB_PROJECT_DIR / "dbt_project.yml") as f:
        codec = yaml.safe_load(f)["vars"]["parquet_compression"]
    assert codec == config.DUCKLAKE_PARQUET_COMPRESSION


def test_backlog_progress_is_a_moved_position() -> None:
    """A pass progresses if the Silver backlog shrinks or any read position moves, even
    when Gold has as many hours left (Silver touched them again)."""
    before = Backlog(20, 30, (100, 50, 60))
    assert Backlog(10, 30, (100, 50, 60)).progressed_from(before)
    assert Backlog(20, 30, (100, 55, 60)).progressed_from(before)
    assert (
        Backlog(20, 30, (100, 50, None)).progressed_from(Backlog(20, 30, (100, 50, None))) is False
    )
    assert Backlog(20, 30, (100, 50, 7)).progressed_from(Backlog(20, 30, (100, 50, None)))
    assert not Backlog(21, 25, (100, 50, 60)).progressed_from(before)


def test_rows_after_counts_inserts_since_snapshot(bronze) -> None:
    """Only rows inserted after the given snapshot are pending."""
    first = _insert(bronze, 30)
    _insert(bronze, 20)
    _insert(bronze, 5)
    assert bronze_rows_after(bronze, first) == 25


def test_rows_after_stops_at_limit(bronze) -> None:
    """The catch-up only asks whether the backlog exceeds one run."""
    first = _insert(bronze, 30)
    _insert(bronze, 20)
    _insert(bronze, 5)
    assert bronze_rows_after(bronze, first, limit=11) == 11
    assert bronze_rows_after(bronze, first, limit=100) == 25


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


def test_read_snapshot_prefers_the_progress_table(transform) -> None:
    """A model whose last batches held none of its rows still moves on (prod stall of
    2026-09-28): its position comes from the progress table, not from its rows."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    transform.execute(
        f"CREATE TABLE {alias}.silver.silver_posts AS SELECT 40 AS bronze_snapshot_id"
    )
    transform.execute(
        f"CREATE TABLE {alias}.silver.silver_deletes AS SELECT 9 AS bronze_snapshot_id"
    )
    transform.execute(f"CREATE SCHEMA {alias}.meta")
    transform.execute(
        f"CREATE TABLE {alias}.meta.silver_progress AS SELECT * FROM (VALUES "
        "('a', 'silver_posts', 40, true), ('a', 'silver_deletes', 40, true), "
        "('b', 'silver_deletes', 55, false)"
        ") AS t(invocation_id, model, bronze_snapshot_id, done)"
    )
    # The pending range of invocation b does not count until its post-hook ran
    assert silver_read_snapshot(transform) == 40


def test_read_snapshot_falls_back_to_rows_without_progress(transform) -> None:
    """Models with no progress row yet (before the switch) keep their rows' position."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    transform.execute(
        f"CREATE TABLE {alias}.silver.silver_posts AS SELECT 40 AS bronze_snapshot_id"
    )
    transform.execute(
        f"CREATE TABLE {alias}.silver.silver_deletes AS SELECT 9 AS bronze_snapshot_id"
    )
    transform.execute(f"CREATE SCHEMA {alias}.meta")
    transform.execute(
        f"CREATE TABLE {alias}.meta.silver_progress AS "
        "SELECT 'a' AS invocation_id, 'silver_posts' AS model, 41 AS bronze_snapshot_id, "
        "true AS done"
    )
    assert silver_read_snapshot(transform) == 9


def test_gold_inputs_from_manifest(tmp_path) -> None:
    """Each Gold model lists only the Silver models it depends on."""
    manifest = tmp_path / "manifest.json"
    nodes = {
        "model.p.silver_posts": {
            "name": "silver_posts",
            "resource_type": "model",
            "depends_on": {"nodes": ["source.p.bronze.bronze_events"]},
        },
        "model.p.silver_likes": {
            "name": "silver_likes",
            "resource_type": "model",
            "depends_on": {"nodes": []},
        },
        "model.p.gold_langs_hour": {
            "name": "gold_langs_hour",
            "resource_type": "model",
            "depends_on": {"nodes": ["model.p.silver_posts"]},
        },
        "model.p.gold_engagement_hour": {
            "name": "gold_engagement_hour",
            "resource_type": "model",
            "depends_on": {"nodes": ["model.p.silver_posts", "model.p.silver_likes"]},
        },
        "test.p.assert_gold": {
            "name": "gold_test",
            "resource_type": "test",
            "depends_on": {"nodes": ["model.p.silver_posts"]},
        },
    }
    manifest.write_text(json.dumps({"nodes": nodes}))
    assert gold_inputs(manifest) == {
        "gold_langs_hour": ["silver_posts"],
        "gold_engagement_hour": ["silver_likes", "silver_posts"],
    }


@pytest.fixture
def lake(tmp_path) -> duckdb.DuckDBPyConnection:
    """A DuckLake catalog under the transform alias, with silver and gold schemas."""
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake; SET TimeZone = 'UTC'")
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    conn.execute(
        f"ATTACH 'ducklake:{tmp_path}/transform.ducklake' AS {alias} "
        f"(DATA_PATH '{tmp_path}/transform/', DATA_INLINING_ROW_LIMIT 0)"
    )
    conn.execute(f"CREATE SCHEMA {alias}.silver")
    conn.execute(f"CREATE SCHEMA {alias}.gold")
    for model in ("silver_posts", "silver_likes"):
        conn.execute(f"CREATE TABLE {alias}.silver.{model} (event_time TIMESTAMPTZ)")
    yield conn
    conn.close()


def _add_hours(conn: duckdb.DuckDBPyConnection, model: str, first: int, count: int) -> int:
    """Insert one Silver row in each of `count` hours from hour `first` of 2026-10-01."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    conn.execute(
        f"INSERT INTO {alias}.silver.{model} SELECT TIMESTAMPTZ '2026-10-01 00:00:00+00' "
        f"+ INTERVAL (h) HOUR FROM range({first}, {first + count}) AS t(h)"
    )
    return conn.execute(f"SELECT id FROM ducklake_current_snapshot('{alias}')").fetchone()[0]


def _gold(conn: duckdb.DuckDBPyConnection, model: str, position: int) -> None:
    """A Gold table whose rows were built from Silver read at `position`."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    conn.execute(
        f"CREATE OR REPLACE TABLE {alias}.gold.{model} AS "
        f"SELECT {position}::BIGINT AS silver_snapshot_id"
    )


def test_gold_hours_behind_per_model_inputs(lake) -> None:
    """A Gold model left at an old position because its own inputs did not change does
    not count the hours another model has to rebuild."""
    start = _add_hours(lake, "silver_posts", 0, 2)
    _gold(lake, "gold_langs_hour", start)
    _gold(lake, "gold_engagement_hour", start)
    _add_hours(lake, "silver_likes", 2, 10)
    inputs = {
        "gold_langs_hour": ["silver_posts"],
        "gold_engagement_hour": ["silver_likes", "silver_posts"],
    }
    positions = gold_positions(lake, list(inputs))
    assert positions == {"gold_engagement_hour": start, "gold_langs_hour": start}
    assert gold_hours_behind(lake, inputs, positions) == 10


def test_gold_hours_behind_zero_before_gold_exists(lake) -> None:
    """Before Gold's first run there is no position to measure from."""
    _add_hours(lake, "silver_posts", 0, 30)
    inputs = {"gold_langs_hour": ["silver_posts"]}
    positions = gold_positions(lake, list(inputs))
    assert positions == {"gold_langs_hour": None}
    assert gold_hours_behind(lake, inputs, positions) == 0
