"""
Backlog of the DuckDB branch: Bronze rows Silver has not read, Silver hours Gold has not rebuilt.

A Silver run reads at most `silver_max_rows_per_run` Bronze rows and a Gold model
rebuilds at most `gold_max_hours_per_run` hours (dbt vars), so after a pause (the 07:00
catch-up, decision D10) the Dagster job repeats dbt until the rest fits in one run.
Same counting as the dbt macros: inserted rows of the DuckLake change feed (decision
D16), which ignores flush and CHECKPOINT snapshots.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb

from src import config
from src.processing.bronze import BRONZE_TABLE
from src.resources.ducklake import DuckLakeSettings, connect, transform_settings

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
# Silver read positions, written by dbt (macros/bronze_changes.sql)
PROGRESS_SCHEMA = "meta"
PROGRESS_TABLE = "silver_progress"


@dataclass(frozen=True)
class Backlog:
    """What one more dbt run would still have to read."""

    silver_rows: int
    gold_hours: int
    # Read positions: Silver's lowest, then each Gold model's (None before a first run)
    positions: tuple[int | None, ...] = ()

    def progressed_from(self, previous: "Backlog") -> bool:
        """True when a pass moved a read position or shrank the Silver backlog.

        Gold hours left are no measure of progress: a later Silver snapshot can touch
        again the hours a pass just rebuilt (each Silver model commits on its own).
        """
        return self.silver_rows < previous.silver_rows or any(
            new is not None and (old is None or new > old)
            for new, old in zip(self.positions, previous.positions, strict=True)
        )


def gold_inputs(manifest_path: Path) -> dict[str, list[str]]:
    """Silver models each Gold model reads, from the compiled dbt manifest."""
    with open(manifest_path) as f:
        nodes = json.load(f)["nodes"]
    return {
        node["name"]: sorted(
            nodes[parent]["name"]
            for parent in node["depends_on"]["nodes"]
            if parent in nodes and nodes[parent]["name"].startswith("silver_")
        )
        for node in nodes.values()
        if node["resource_type"] == "model" and node["name"].startswith("gold_")
    }


def _tables(conn: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    """Tables of a schema in the transform catalog."""
    return [
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?",
            [config.DUCKLAKE_TRANSFORM_ALIAS, schema],
        ).fetchall()
    ]


def silver_positions(transform: duckdb.DuckDBPyConnection) -> dict[str, int | None]:
    """Bronze snapshot each Silver model has read up to, None before its first run.

    A model's position is its last completed range in the progress table, else (before
    that table existed) the max `bronze_snapshot_id` of its rows.
    """
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    progress = {}
    if PROGRESS_TABLE in _tables(transform, PROGRESS_SCHEMA):
        progress = dict(
            transform.execute(
                f"SELECT model, max(bronze_snapshot_id) "
                f"FROM {alias}.{PROGRESS_SCHEMA}.{PROGRESS_TABLE} WHERE done GROUP BY model"
            ).fetchall()
        )
    return {
        table: progress[table]
        if progress.get(table) is not None
        else transform.execute(
            f"SELECT max(bronze_snapshot_id) FROM {alias}.{SILVER_SCHEMA}.{table}"
        ).fetchone()[0]
        for table in _tables(transform, SILVER_SCHEMA)
    }


def silver_read_snapshot(transform: duckdb.DuckDBPyConnection) -> int | None:
    """Bronze snapshot every Silver model has read up to, None before the first run.

    The lowest position is what the next run starts from if a model was left behind
    (failed run).
    """
    snapshots = list(silver_positions(transform).values())
    if not snapshots or any(snapshot is None for snapshot in snapshots):
        return None
    return min(snapshots)


def bronze_rows_after(
    bronze: duckdb.DuckDBPyConnection, snapshot: int | None, limit: int | None = None
) -> int:
    """Bronze rows inserted after `snapshot` (all of them, from the oldest kept snapshot,
    when Silver has never run), counted up to `limit` at most.

    The catch-up only needs to know whether the backlog exceeds one run: counting 82 M
    rows took ~65 s before every pass of the 2026-10-01 catch-up, the limit stops early.
    """
    alias = config.DUCKLAKE_BRONZE_ALIAS
    current = bronze.execute(f"SELECT id FROM ducklake_current_snapshot('{alias}')").fetchone()[0]
    if snapshot is None:
        snapshot = (
            bronze.execute(
                f"SELECT min(snapshot_id) FROM ducklake_snapshots('{alias}')"
            ).fetchone()[0]
            - 1
        )
    if snapshot >= current:
        return 0
    limit_clause = "" if limit is None else f" LIMIT {int(limit)}"
    return bronze.execute(
        "SELECT count(*) FROM (SELECT 1 FROM "
        f"ducklake_table_insertions('{alias}', 'main', '{BRONZE_TABLE}', ?, ?){limit_clause})",
        [snapshot + 1, current],
    ).fetchone()[0]


def gold_positions(
    transform: duckdb.DuckDBPyConnection, models: list[str]
) -> dict[str, int | None]:
    """Silver snapshot each Gold model last read up to (max `silver_snapshot_id`), None
    before its first run."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    existing = set(_tables(transform, GOLD_SCHEMA))
    return {
        model: transform.execute(
            f"SELECT max(silver_snapshot_id) FROM {alias}.{GOLD_SCHEMA}.{model}"
        ).fetchone()[0]
        if model in existing
        else None
        for model in sorted(models)
    }


def gold_hours_behind(
    transform: duckdb.DuckDBPyConnection,
    inputs: dict[str, list[str]],
    positions: dict[str, int | None],
) -> int:
    """Most Silver hours a Gold model still has to rebuild, 0 before Gold's first run.

    Each Gold model is measured on its own Silver inputs from its own position: a model
    with no new input rows writes nothing and keeps an old position, which must not
    count the hours other models have to rebuild.
    """
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    current = transform.execute(f"SELECT id FROM ducklake_current_snapshot('{alias}')").fetchone()[
        0
    ]
    behind = 0
    for model, silver_models in inputs.items():
        position = positions.get(model)
        if position is None or position >= current or not silver_models:
            continue
        selects = " UNION ALL ".join(
            f"SELECT date_trunc('hour', event_time) AS hour FROM ducklake_table_insertions("
            f"'{alias}', '{SILVER_SCHEMA}', '{silver_model}', {position + 1}, {current})"
            for silver_model in silver_models
        )
        hours = transform.execute(f"SELECT count(DISTINCT hour) FROM ({selects})").fetchone()[0]
        behind = max(behind, hours)
    return behind


def current_backlog(
    manifest_path: Path,
    silver_limit: int | None = None,
    bronze_settings: DuckLakeSettings | None = None,
    transform: DuckLakeSettings | None = None,
) -> Backlog:
    """Silver and Gold backlog, Silver rows counted up to `silver_limit`. Both catalogs
    are attached read-only."""
    bronze_conn = connect(bronze_settings or DuckLakeSettings(), read_only=True)
    try:
        transform_conn = connect(transform or transform_settings(), read_only=True)
    except duckdb.InvalidInputException as e:
        # First deployment: dbt has not created the transform catalog yet (a read-only
        # ATTACH does not create it). Any other error (Postgres, Garage) is raised
        if "does not exist" not in str(e):
            raise
        snapshot, gold_hours, positions = None, 0, {}
    else:
        try:
            snapshot = silver_read_snapshot(transform_conn)
            inputs = gold_inputs(manifest_path)
            positions = gold_positions(transform_conn, list(inputs))
            gold_hours = gold_hours_behind(transform_conn, inputs, positions)
        finally:
            transform_conn.close()
    try:
        return Backlog(
            bronze_rows_after(bronze_conn, snapshot, silver_limit),
            gold_hours,
            (snapshot, *positions.values()),
        )
    finally:
        bronze_conn.close()
