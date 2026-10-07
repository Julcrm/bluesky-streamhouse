"""
Dagster Definitions of branch A, served by the gRPC code server `bluesky_spark`
(decision D20: one code location per branch) to the shared dagster-workspace.

Runs execute in this container (DefaultRunLauncher); dbt-spark's queries run in the
Spark Thrift server (D6 revised), both within branch A's 2 GB transform budget (D31).
Silver/Gold every 15 min on A's days (src/dagster/spark_assets.py), nightly tests after
a day of A (D30), Iceberg maintenance of the Bronze table, completeness check of A's
days (src/dagster/spark_alternation.py). One run at a time in this location (D30): the
Thrift server is shared and its memory close to its limit.
"""

import time
from datetime import UTC, datetime

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
from dagster_dbt import DbtCliResource

from src import config
from src.alternation import calendar as cal
from src.alternation import monitor
from src.dagster.alerts import failure_alert_sensor
from src.dagster.runs import active_location_runs, wait_for_runs
from src.dagster.spark_alternation import (
    iceberg_completeness_job,
    iceberg_completeness_sensor,
    iceberg_day_completeness,
)
from src.dagster.spark_assets import dbt_project, spark_bronze, spark_dbt_models
from src.maintenance import iceberg
from src.processing.bronze import BRONZE_TABLE
from src.resources.spark import build_session

GROUP = "maintenance"
BRONZE = f"{config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"


@asset(
    name="iceberg_bronze_maintenance",
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_A, GROUP],
    group_name=GROUP,
    description="Iceberg Bronze: retention (7 days), file and manifest rewrites, snapshot "
    "expiration (24 h), orphan files (D14, D21). The counterpart of DuckLake's CHECKPOINT.",
)
def iceberg_bronze_maintenance(context: AssetExecutionContext) -> MaterializeResult:
    """One Iceberg procedure per step, each timed: their number and cost against
    DuckLake's single command is a benchmark result. Alone in this location (D30)."""
    wait_for_runs(
        context.instance, context.log, context.run_id, None, config.SPARK_RUN_WAIT_SECONDS
    )
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
    concurrently with the streaming appends (optimistic concurrency); the run waits for
    the other runs of this location (D30)."""
    previous = [
        run
        for run in active_location_runs(context.instance, iceberg_maintenance_job.name)
        if run.job_name == iceberg_maintenance_job.name
    ]
    if previous:
        return SkipReason(f"Iceberg maintenance run {previous[0].run_id} still in progress")
    return RunRequest()


spark_silver_gold_job = define_asset_job(
    name="bluesky_spark_silver_gold",
    selection=AssetSelection.keys(spark_bronze.key) | AssetSelection.assets(spark_dbt_models),
    description="Observe Bronze, then Silver (with catch-up passes) and Gold, dbt tests as checks.",
)

spark_nightly_checks_job = define_asset_job(
    name="bluesky_spark_nightly_checks",
    # The dbt tests only, no model: rerun over a day instead of 2 hours (D25)
    selection=AssetSelection.checks_for_assets(spark_dbt_models),
    description="Every dbt test of Silver and Gold over the last day of branch A (D25, D30).",
    tags={config.NIGHTLY_CHECKS_TAG: "true"},
)


@schedule(
    job=spark_silver_gold_job,
    cron_schedule=config.DAGSTER_SCHEDULE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def spark_silver_gold_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every 15 min on branch A's days only (D28), as branch B's: its engine runs, or
    stopped less than an hour ago. Any run of this location blocks it (D30)."""
    days = monitor.recent_days()
    due = monitor.transform_due(days, cal.BRANCH_A, datetime.now(UTC)) if days else None
    if due is None:
        return SkipReason("No day of branch A running or just finished")
    active = active_location_runs(context.instance, spark_silver_gold_job.name)
    if active:
        return SkipReason(f"Run {active[0].run_id} ({active[0].job_name}) still in progress")
    return RunRequest(tags={"bluesky/transform_due": due})


@schedule(
    job=spark_nightly_checks_job,
    cron_schedule=config.NIGHTLY_CHECKS_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def spark_nightly_checks_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every night at 03:00 (Europe/Paris), only after a day of branch A (D30): the
    Thrift server is still up (it runs until B's next opening). The run waits for the
    other runs of this location."""
    days = monitor.recent_days()
    due = monitor.nightly_checks_due(days, cal.BRANCH_A, datetime.now(UTC)) if days else None
    if due is None:
        return SkipReason("The day that just ended was not a finished day of branch A")
    previous = [
        run
        for run in active_location_runs(context.instance, spark_nightly_checks_job.name)
        if run.job_name == spark_nightly_checks_job.name
    ]
    if previous:
        return SkipReason(f"Nightly checks run {previous[0].run_id} still in progress")
    return RunRequest(tags={"bluesky/nightly_checks_due": due})


defs = Definitions(
    assets=[spark_bronze, spark_dbt_models, iceberg_bronze_maintenance, iceberg_day_completeness],
    jobs=[
        spark_silver_gold_job,
        spark_nightly_checks_job,
        iceberg_maintenance_job,
        iceberg_completeness_job,
    ],
    schedules=[
        spark_silver_gold_schedule,
        spark_nightly_checks_schedule,
        iceberg_maintenance_schedule,
    ],
    sensors=[
        failure_alert_sensor(
            "spark_failure_alert_sensor",
            [
                spark_silver_gold_job,
                spark_nightly_checks_job,
                iceberg_maintenance_job,
                iceberg_completeness_job,
            ],
        ),
        iceberg_completeness_sensor,
    ],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
