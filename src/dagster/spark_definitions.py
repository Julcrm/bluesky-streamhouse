"""
Dagster Definitions of branch A, served by the gRPC code server `bluesky_spark`
(decision D20: one code location per branch) to the shared dagster-workspace.

Runs execute in this container (DefaultRunLauncher), so Spark's memory counts against
its own limit (1.5 GB, like branch B's code server). Phase 4: Iceberg maintenance of the
Bronze table; phase 6: completeness check of A's days (src/dagster/spark_alternation.py);
phase 5 adds the dbt-spark Silver/Gold models here.
"""

import time

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
)

from src import config
from src.dagster.alerts import failure_alert_sensor
from src.dagster.runs import active_location_runs
from src.dagster.spark_alternation import (
    iceberg_completeness_job,
    iceberg_completeness_sensor,
    iceberg_day_completeness,
)
from src.maintenance import iceberg
from src.processing.bronze import BRONZE_TABLE
from src.resources.spark import build_session

GROUP = "maintenance"
BRONZE = f"{config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"


@asset(
    name="iceberg_bronze_maintenance",
    key_prefix=[config.DAGSTER_ASSET_PREFIX, GROUP],
    group_name=GROUP,
    description="Iceberg Bronze: retention (7 days), file and manifest rewrites, snapshot "
    "expiration (24 h), orphan files (D14, D21). The counterpart of DuckLake's CHECKPOINT.",
)
def iceberg_bronze_maintenance(context: AssetExecutionContext) -> MaterializeResult:
    """One Iceberg procedure per step, each timed: their number and cost against
    DuckLake's single command is a benchmark result."""
    spark = build_session("bluesky-iceberg-maintenance", config.SPARK_MAINTENANCE_DRIVER_MEMORY)
    spark.sparkContext.setLogLevel("WARN")
    durations: dict[str, float] = {}
    results: dict[str, object] = {}

    def timed(step: str, fn, *args):
        started = time.monotonic()
        value = fn(spark, *args)
        durations[f"{step}_seconds"] = round(time.monotonic() - started, 1)
        context.log.info(f"{step}: {value} in {durations[f'{step}_seconds']} s")
        results[step] = value
        return value

    try:
        before = iceberg.table_stats(spark, BRONZE)
        timed(
            "delete_expired_rows",
            iceberg.delete_expired_rows,
            BRONZE,
            "event_time",
            config.BRONZE_RETENTION_DAYS,
        )
        timed("rewrite_data_files", iceberg.rewrite_data_files, BRONZE)
        timed("rewrite_manifests", iceberg.rewrite_manifests, BRONZE)
        timed("expire_snapshots", iceberg.expire_snapshots, BRONZE)
        timed("remove_orphan_files", iceberg.remove_orphan_files, BRONZE)
        after = iceberg.table_stats(spark, BRONZE)
    finally:
        spark.stop()
    return MaterializeResult(
        metadata={
            **{f"before_{k}": v for k, v in before.as_dict().items()},
            **{f"after_{k}": v for k, v in after.as_dict().items()},
            **durations,
            "rows_deleted": results["delete_expired_rows"],
            "orphan_files_deleted": results["remove_orphan_files"],
            "rewrite_data_files": results["rewrite_data_files"],
            "expire_snapshots": results["expire_snapshots"],
        }
    )


iceberg_maintenance_job = define_asset_job(
    name="bluesky_iceberg_maintenance",
    selection=AssetSelection.assets(iceberg_bronze_maintenance),
    description="Nightly Iceberg maintenance of branch A's Bronze table (D21).",
)


@schedule(
    job=iceberg_maintenance_job,
    cron_schedule=config.ICEBERG_MAINTENANCE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def iceberg_maintenance_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every night at 02:30 (Europe/Paris), after branch B's maintenance. Iceberg commits
    concurrently with the streaming appends (optimistic concurrency): no wait needed."""
    previous = [
        run
        for run in active_location_runs(context.instance, iceberg_maintenance_job.name)
        if run.job_name == iceberg_maintenance_job.name
    ]
    if previous:
        return SkipReason(f"Iceberg maintenance run {previous[0].run_id} still in progress")
    return RunRequest()


defs = Definitions(
    assets=[iceberg_bronze_maintenance, iceberg_day_completeness],
    jobs=[iceberg_maintenance_job, iceberg_completeness_job],
    schedules=[iceberg_maintenance_schedule],
    sensors=[
        failure_alert_sensor(
            "spark_failure_alert_sensor", [iceberg_maintenance_job, iceberg_completeness_job]
        ),
        iceberg_completeness_sensor,
    ],
)
