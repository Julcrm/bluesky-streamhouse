"""
Silver backlog of branch B: Bronze rows written since the snapshot Silver last read.

A Silver run reads at most `silver_max_rows_per_run` Bronze rows (dbt var), so after a
pause (the 07:00 catch-up, decision D10) the Dagster job repeats Silver until the rest
fits in one run. Same counting as the dbt macro `bronze_snapshot_range`: inserted rows
of the Bronze change feed (decision D16), which ignores flush and CHECKPOINT snapshots.
"""

from pathlib import Path

import duckdb
import yaml

from src import config
from src.processing.bronze import BRONZE_TABLE
from src.resources.ducklake import DuckLakeSettings, connect, transform_settings

SILVER_SCHEMA = "silver"


def silver_max_rows_per_run(project_dir: Path = config.DBT_DUCKDB_PROJECT_DIR) -> int:
    """Row cap of one Silver run, read from dbt_project.yml (single source of truth)."""
    with open(project_dir / "dbt_project.yml") as f:
        return int(yaml.safe_load(f)["vars"]["silver_max_rows_per_run"])


def silver_read_snapshot(transform: duckdb.DuckDBPyConnection) -> int | None:
    """Bronze snapshot every Silver model has read up to, None before the first run.

    All Silver models read the same snapshot range, but the lowest one is what the next
    run starts from if a model was left behind (failed run).
    """
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    tables = [
        row[0]
        for row in transform.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?",
            [alias, SILVER_SCHEMA],
        ).fetchall()
    ]
    if not tables:
        return None
    snapshots = [
        transform.execute(
            f"SELECT max(bronze_snapshot_id) FROM {alias}.{SILVER_SCHEMA}.{table}"
        ).fetchone()[0]
        for table in tables
    ]
    if any(snapshot is None for snapshot in snapshots):
        return None
    return min(snapshots)


def bronze_rows_after(bronze: duckdb.DuckDBPyConnection, snapshot: int | None) -> int:
    """Bronze rows inserted after `snapshot` (all of them, from the oldest kept snapshot,
    when Silver has never run)."""
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
    return bronze.execute(
        f"SELECT count(*) FROM ducklake_table_changes('{alias}', 'main', '{BRONZE_TABLE}', ?, ?) "
        "WHERE change_type = 'insert'",
        [snapshot + 1, current],
    ).fetchone()[0]


def silver_backlog_rows(
    bronze_settings: DuckLakeSettings | None = None,
    transform: DuckLakeSettings | None = None,
) -> int:
    """Bronze rows Silver has not read yet. Both catalogs are attached read-only."""
    bronze_conn = connect(bronze_settings or DuckLakeSettings(), read_only=True)
    try:
        transform_conn = connect(transform or transform_settings(), read_only=True)
    except duckdb.InvalidInputException as e:
        # First deployment: dbt has not created the transform catalog yet (a read-only
        # ATTACH does not create it). Any other error (Postgres, Garage) is raised
        if "does not exist" not in str(e):
            raise
        snapshot = None
    else:
        try:
            snapshot = silver_read_snapshot(transform_conn)
        finally:
            transform_conn.close()
    try:
        return bronze_rows_after(bronze_conn, snapshot)
    finally:
        bronze_conn.close()
