"""
Nightly maintenance of branch B (decisions D14, D21), as assets so that every run keeps
its before/after figures in Dagster: files, bytes and snapshots over time are benchmark
data (drift, H8). No business logic here: everything is in `src.maintenance.ducklake`.

Order (one step at a time): guard, retention DELETE and CHECKPOINT of each catalog (one
does not wait for the other), storage checks, then the purge of old Dagster runs.
"""

import time
from datetime import UTC, datetime, timedelta

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DagsterRunStatus,
    Failure,
    MaterializeResult,
    RunsFilter,
    asset,
)

from src import config
from src.dagster.runs import active_location_runs, in_location
from src.maintenance import ducklake as maintenance
from src.processing.backlog import GOLD_SCHEMA, gold_positions, silver_positions
from src.resources.ducklake import DuckLakeSettings, connect, transform_settings

GROUP = "maintenance"
SILVER_GOLD_JOB = "bluesky_silver_gold"
FINISHED_RUN_STATUSES = [
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
]


def _wait_for_silver_gold(context: AssetExecutionContext) -> None:
    """Wait for a Silver/Gold run in progress (dbt writes the transform catalog), up to
    MAINTENANCE_WAIT_FOR_RUN_SECONDS. The schedule skips its ticks while this runs."""
    deadline = time.monotonic() + config.MAINTENANCE_WAIT_FOR_RUN_SECONDS
    while active := active_location_runs(context.instance, SILVER_GOLD_JOB, context.run_id):
        if time.monotonic() > deadline:
            raise Failure(
                f"Run {active[0].run_id} ({active[0].job_name}) still in progress after "
                f"{config.MAINTENANCE_WAIT_FOR_RUN_SECONDS // 60} min: maintenance skipped",
                allow_retries=False,
            )
        context.log.info(f"Waiting for run {active[0].run_id} ({active[0].job_name})")
        time.sleep(30)


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
    """Guard, retention DELETE, options, CHECKPOINT; figures before and after."""
    _guard(context, stale, settings.alias)
    started = time.monotonic()
    conn = maintenance.maintenance_connection(settings)
    try:
        before = maintenance.catalog_stats(conn, settings)
        deleted = maintenance.delete_expired_rows(conn, settings.alias, retention_days)
        if settings.alias == config.DUCKLAKE_TRANSFORM_ALIAS:
            deleted["meta.silver_progress"] = maintenance.trim_silver_progress(conn)
        maintenance.set_options(conn, settings.alias)
        checkpoint_started = time.monotonic()
        attempts = maintenance.checkpoint(conn, settings.alias)
        checkpoint_seconds = time.monotonic() - checkpoint_started
        after = maintenance.catalog_stats(conn, settings)
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "rows_deleted": deleted,
            "checkpoint_attempts": attempts,
            "checkpoint_seconds": round(checkpoint_seconds, 1),
            "duration_seconds": round(time.monotonic() - started, 1),
            **{f"before_{k}": v for k, v in before.as_dict().items()},
            **{f"after_{k}": v for k, v in after.as_dict().items()},
        }
    )


@asset(group_name=GROUP, description="Bronze retention (7 days) and CHECKPOINT (D14, D21).")
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
    # Not chained to each other: a Bronze failure must not stop the transform catalog
    deps=[bronze_maintenance, transform_maintenance],
    description="Bucket and catalog database sizes after maintenance, with alert thresholds.",
    check_specs=[
        AssetCheckSpec("bucket_under_alert", asset="lake_storage", blocking=True),
        AssetCheckSpec("catalog_under_alert", asset="lake_storage", blocking=True),
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


@asset(
    group_name=GROUP,
    deps=[lake_storage],
    description="Delete finished Dagster runs of this code location older than 30 days (D20).",
)
def dagster_run_purge(context: AssetExecutionContext) -> MaterializeResult:
    cutoff = datetime.now(UTC) - timedelta(days=config.DAGSTER_RUN_RETENTION_DAYS)
    records = context.instance.get_run_records(
        RunsFilter(statuses=FINISHED_RUN_STATUSES, created_before=cutoff)
    )
    # Never another project's runs: the instance is shared with velib
    ours = [r.dagster_run.run_id for r in records if in_location(r.dagster_run)]
    for run_id in ours:
        context.instance.delete_run(run_id)
    return MaterializeResult(metadata={"runs_deleted": len(ours), "cutoff": cutoff.isoformat()})


maintenance_assets = [bronze_maintenance, transform_maintenance, lake_storage, dagster_run_purge]
