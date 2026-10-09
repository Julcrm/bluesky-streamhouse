"""
Nightly maintenance of the DuckDB branch (decisions D14, D21), as assets so that every run keeps
its before/after figures in Dagster: files, bytes and snapshots over time are benchmark
data (drift, H8). No business logic here: everything is in `src.maintenance.ducklake`.

Order (one step at a time): guard, retention DELETE and CHECKPOINT of each catalog (one
does not wait for the other), storage checks; the housekeeping (old Dagster runs, dbt
run folders) runs whatever they give.
"""

import time
from contextlib import nullcontext

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    Failure,
    MaterializeResult,
    asset,
)

from src import config
from src.dagster.assets import dbt_project
from src.dagster.housekeeping import housekeeping_asset
from src.dagster.runs import wait_for_runs
from src.maintenance import ducklake as maintenance
from src.processing.backlog import GOLD_SCHEMA, gold_positions, silver_positions
from src.resources.bronze_lock import exclusive_bronze_lock
from src.resources.ducklake import DuckLakeSettings, connect, transform_settings

GROUP = "maintenance"
KEY_PREFIX = [config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_DUCKDB, GROUP]
LAKE_STORAGE_KEY = AssetKey([*KEY_PREFIX, "lake_storage"])
SILVER_GOLD_JOB = "bluesky_silver_gold"


def _wait_for_silver_gold(context: AssetExecutionContext) -> None:
    """Wait for a Silver/Gold run in progress (dbt writes the transform catalog), up to
    MAINTENANCE_WAIT_FOR_RUN_SECONDS. The schedule skips its ticks while this runs.
    Not for the nightly checks: they only read, and they wait for the maintenance."""
    wait_for_runs(
        context.instance,
        context.log,
        context.run_id,
        {SILVER_GOLD_JOB, "__ASSET_JOB"},
        config.MAINTENANCE_WAIT_FOR_RUN_SECONDS,
    )


def _guard(context: AssetExecutionContext, stale: dict[str, str], catalog: str) -> None:
    """Stop before any DELETE or CHECKPOINT if a reader is too far behind (D16, D21)."""
    if stale:
        raise Failure(
            f"Maintenance of `{catalog}` stopped: unread snapshots older than "
            f"{config.READ_POSITION_MAX_AGE_HOURS} h would be expired",
            metadata={"stale_readers": stale},
            allow_retries=False,
        )
    context.log.info(f"Guard passed for `{catalog}`: every reader is within the window")


def _maintain(
    context: AssetExecutionContext,
    settings: DuckLakeSettings,
    retention_days: dict[str, int],
    stale: dict[str, str],
) -> MaterializeResult:
    """Guard, retention DELETE, options, CHECKPOINT, scheduled files deleted; figures
    before and after."""
    _guard(context, stale, settings.alias)
    started = time.monotonic()
    # Quix writes Bronze 24/7 until the alternation (D28): it pauses while the
    # maintenance holds the Bronze write lock. dbt never runs during the maintenance
    is_bronze = settings.alias == config.DUCKLAKE_BRONZE_ALIAS
    conn = maintenance.maintenance_connection(settings)
    try:
        before = maintenance.catalog_stats(conn, settings)
        lock_started = time.monotonic()
        with exclusive_bronze_lock(settings) if is_bronze else nullcontext():
            lock_wait_seconds = time.monotonic() - lock_started
            delete_started = time.monotonic()
            deleted = maintenance.delete_expired_rows(conn, settings.alias, retention_days)
            delete_seconds = time.monotonic() - delete_started
            if settings.alias == config.DUCKLAKE_TRANSFORM_ALIAS:
                deleted["meta.silver_progress"] = maintenance.trim_silver_progress(conn)
            maintenance.set_options(conn, settings.alias)
            checkpoint_started = time.monotonic()
            attempts = maintenance.checkpoint(conn, settings.alias)
            checkpoint_seconds = time.monotonic() - checkpoint_started
            files_deleted = maintenance.delete_scheduled_files(conn, settings.alias)
            writers_paused_seconds = time.monotonic() - lock_started if is_bronze else 0.0
        after = maintenance.catalog_stats(conn, settings)
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "rows_deleted": deleted,
            "delete_seconds": round(delete_seconds, 1),
            "lock_wait_seconds": round(lock_wait_seconds, 1),
            # How long Quix stayed paused (Bronze only): it catches up afterwards
            "writers_paused_seconds": round(writers_paused_seconds, 1),
            "checkpoint_attempts": attempts,
            "checkpoint_seconds": round(checkpoint_seconds, 1),
            "files_deleted": files_deleted,
            "duration_seconds": round(time.monotonic() - started, 1),
            **{f"before_{k}": v for k, v in before.as_dict().items()},
            **{f"after_{k}": v for k, v in after.as_dict().items()},
        }
    )


@asset(
    group_name=GROUP,
    key_prefix=KEY_PREFIX,
    description="Bronze retention (7 days) and CHECKPOINT (D14, D21).",
)
def bronze_maintenance(context: AssetExecutionContext) -> MaterializeResult:
    _wait_for_silver_gold(context)
    bronze_settings = DuckLakeSettings()
    readers = connect(transform_settings(), read_only=True)
    bronze = connect(bronze_settings, read_only=True)
    try:
        # Silver reads Bronze's change feed from each model's position
        stale = maintenance.stale_positions(
            bronze, bronze_settings.alias, silver_positions(readers)
        )
    finally:
        readers.close()
        bronze.close()
    return _maintain(context, bronze_settings, {"main": config.BRONZE_RETENTION_DAYS}, stale)


@asset(
    group_name=GROUP,
    key_prefix=KEY_PREFIX,
    description="Silver (7 days) and Gold (30 days) retention, CHECKPOINT of the transform "
    "catalog (D14, D21).",
)
def transform_maintenance(context: AssetExecutionContext) -> MaterializeResult:
    _wait_for_silver_gold(context)
    settings = transform_settings()
    transform = connect(settings, read_only=True)
    try:
        # Gold reads the Silver change feed of the same catalog from each model's position
        models = maintenance.list_tables(transform, settings.alias, GOLD_SCHEMA)
        stale = maintenance.stale_positions(
            transform, settings.alias, gold_positions(transform, models)
        )
    finally:
        transform.close()
    return _maintain(
        context,
        settings,
        {"silver": config.SILVER_RETENTION_DAYS, "gold": config.GOLD_RETENTION_DAYS},
        stale,
    )


@asset(
    group_name=GROUP,
    key_prefix=KEY_PREFIX,
    # Not chained to each other: a Bronze failure must not stop the transform catalog
    deps=[bronze_maintenance, transform_maintenance],
    description="Bucket and catalog database sizes after maintenance, with alert thresholds.",
    check_specs=[
        AssetCheckSpec("bucket_under_alert", asset=LAKE_STORAGE_KEY, blocking=True),
        AssetCheckSpec("catalog_under_alert", asset=LAKE_STORAGE_KEY, blocking=True),
    ],
)
def lake_storage(context: AssetExecutionContext) -> MaterializeResult:
    bucket = maintenance.bucket_bytes()
    conn = connect(read_only=True)
    try:
        catalog = maintenance.catalog_database_bytes(conn)
    finally:
        conn.close()
    gb = 10**9
    return MaterializeResult(
        metadata={"bucket_bytes": bucket, "catalog_database_bytes": catalog},
        check_results=[
            AssetCheckResult(
                check_name="bucket_under_alert",
                passed=bucket < config.BUCKET_ALERT_BYTES,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "bucket_gb": round(bucket / gb, 2),
                    "alert_gb": config.BUCKET_ALERT_BYTES / gb,
                },
            ),
            AssetCheckResult(
                check_name="catalog_under_alert",
                passed=catalog < config.CATALOG_ALERT_BYTES,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "catalog_gb": round(catalog / gb, 3),
                    "alert_gb": config.CATALOG_ALERT_BYTES / gb,
                },
            ),
        ],
    )


housekeeping = housekeeping_asset(
    KEY_PREFIX, GROUP, dbt_project.project_dir / dbt_project.target_path
)


maintenance_assets = [bronze_maintenance, transform_maintenance, lake_storage, housekeeping]
