"""
Dagster Definitions of the Spark branch, served by the gRPC code server `bluesky_spark`
(decision D20: one code location per branch) to the shared dagster-workspace.

Runs execute in this container (DefaultRunLauncher); dbt-spark's queries run in the
Spark Thrift server (D6 revised), both within the Spark branch's transform limits (D31).
Silver/Gold every 15 min on the Spark branch's days (src/dagster/spark_assets.py),
nightly tests after each of its days (D30), Iceberg maintenance of every table and
completeness check of its days
(src/dagster/spark_alternation.py), all through the Thrift server: no JVM here (5d).
One run at a time in this location (D30): the Thrift server is shared.
"""

import time
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    Failure,
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
from src.benchmark.cleanup import drop_iceberg_bench
from src.dagster.alerts import failure_alert_sensor
from src.dagster.bench import bench_night_definitions, spark_arrivals, spark_transform
from src.dagster.housekeeping import housekeeping_asset
from src.dagster.runs import active_location_runs, wait_for_runs
from src.dagster.spark_alternation import (
    iceberg_completeness_job,
    iceberg_completeness_sensor,
    iceberg_day_completeness,
    spark_benchmark_windows,
)
from src.dagster.spark_assets import dbt_project, logged_backlog, spark_bronze, spark_dbt_models
from src.maintenance import iceberg
from src.processing.bronze import BRONZE_TABLE
from src.resources import thrift

GROUP = "maintenance"
BRONZE = f"{config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"


Retention = tuple[str, int] | None  # (time column, days), None: no row retention


def _guard(context: AssetExecutionContext, stale: dict[str, str], scope: str) -> None:
    """Stop before any DELETE or expiration if a reader is too far behind (D16, D21)."""
    if stale:
        raise Failure(
            f"Iceberg maintenance of {scope} stopped: unread snapshots older than "
            f"{config.READ_POSITION_MAX_AGE_HOURS} h would be expired",
            metadata={"stale_readers": stale},
            allow_retries=False,
        )
    context.log.info(f"Guard passed for {scope}: every reader is within the window")


def _maintain(
    context: AssetExecutionContext, tables: dict[str, Retention], trims: dict[str, tuple]
) -> MaterializeResult:
    """Guard, then table by table: retention DELETE (or trim of the read positions),
    rewrite_data_files, rewrite_manifests, expire_snapshots, remove_orphan_files. Every
    statement runs on the Thrift server; each step is timed and summed over the tables:
    their number and cost against DuckLake's single CHECKPOINT is a benchmark result."""
    sql = thrift.records
    existing = set(iceberg.list_tables(sql, "meta"))
    _guard(
        context,
        iceberg.stale_readers(sql, iceberg.read_positions(sql, existing), set(tables)),
        ", ".join(sorted({t.split(".")[1] for t in tables})),
    )
    durations: dict[str, float] = {}
    rows_deleted: dict[str, int] = {}
    orphans = 0
    calls = 0
    before = after = iceberg.EMPTY_STATS

    def timed(step: str, fn, *args):
        nonlocal calls
        started = time.monotonic()
        value = fn(sql, *args)
        key = f"{step}_seconds"
        durations[key] = round(durations.get(key, 0.0) + time.monotonic() - started, 1)
        calls += 1
        return value

    for table, retention in tables.items():
        name = table.split(".", 1)[1]
        before += iceberg.table_stats(sql, table)
        if retention is not None:
            rows_deleted[name] = timed(
                "delete_expired_rows", iceberg.delete_expired_rows, table, *retention
            )
        elif table in trims:
            rows_deleted[name] = timed("trim_progress", iceberg.trim_progress, table, trims[table])
        timed("rewrite_data_files", iceberg.rewrite_data_files, table)
        timed("rewrite_manifests", iceberg.rewrite_manifests, table)
        timed("expire_snapshots", iceberg.expire_snapshots, table)
        orphans += timed("remove_orphan_files", iceberg.remove_orphan_files, table)
        after += iceberg.table_stats(sql, table)
        context.log.info(f"{name}: maintained, {rows_deleted.get(name, 0)} rows deleted")
    return MaterializeResult(
        metadata={
            "tables": len(tables),
            "procedure_calls": calls,
            "rows_deleted": rows_deleted,
            "orphan_files_deleted": orphans,
            **durations,
            **{f"before_{k}": v for k, v in before.as_dict().items()},
            **{f"after_{k}": v for k, v in after.as_dict().items()},
        }
    )


@asset(
    name="iceberg_bronze_maintenance",
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_SPARK, GROUP],
    group_name=GROUP,
    description="Iceberg Bronze: retention (7 days), file and manifest rewrites, snapshot "
    "expiration (24 h), orphan files (D14, D21), through the Thrift server. The "
    "counterpart of DuckLake's CHECKPOINT.",
)
def iceberg_bronze_maintenance(context: AssetExecutionContext) -> MaterializeResult:
    """Alone in this location (D30): waits for Silver/Gold, which reads Bronze."""
    wait_for_runs(
        context.instance, context.log, context.run_id, None, config.SPARK_RUN_WAIT_SECONDS
    )
    return _maintain(context, {BRONZE: ("event_time", config.BRONZE_RETENTION_DAYS)}, {})


@asset(
    name="iceberg_transform_maintenance",
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_SPARK, GROUP],
    group_name=GROUP,
    # Not chained to Bronze: a Bronze failure must not stop Silver and Gold (as the DuckDB branch)
    description="Iceberg Silver (7 days), Gold (30 days) and read positions (7 days, the "
    "last of each reader kept): same steps as Bronze (D14, D21).",
)
def iceberg_transform_maintenance(context: AssetExecutionContext) -> MaterializeResult:
    wait_for_runs(
        context.instance, context.log, context.run_id, None, config.SPARK_RUN_WAIT_SECONDS
    )
    sql = thrift.records
    tables: dict[str, Retention] = {}
    for namespace, days in (
        ("silver", config.SILVER_RETENTION_DAYS),
        ("gold", config.GOLD_RETENTION_DAYS),
    ):
        for table in iceberg.list_tables(sql, namespace):
            tables[table] = (iceberg.time_column(sql, table), days)
    meta = iceberg.list_tables(sql, "meta")
    for table in meta:
        tables[table] = None
    trims = {
        iceberg.SILVER_PROGRESS: ("model",),
        iceberg.GOLD_PROGRESS: ("model", "silver_model"),
    }
    return _maintain(context, tables, {t: k for t, k in trims.items() if t in meta})


housekeeping = housekeeping_asset(
    [config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_SPARK, GROUP],
    GROUP,
    dbt_project.project_dir / dbt_project.target_path,
)


iceberg_maintenance_job = define_asset_job(
    name="bluesky_iceberg_maintenance",
    selection=AssetSelection.assets(
        iceberg_bronze_maintenance, iceberg_transform_maintenance, housekeeping
    ),
    description="Nightly Iceberg maintenance of the Spark branch: Bronze, Silver, Gold, "
    "positions (D21), "
    "and the housekeeping of this code location.",
    # One step at a time, as the DuckDB branch's: both send their procedures to the one
    # Thrift server
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 1}}}},
)


@schedule(
    job=iceberg_maintenance_job,
    cron_schedule=config.ICEBERG_MAINTENANCE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def iceberg_maintenance_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every night at 02:30 (Europe/Paris), after the DuckDB branch's maintenance, only
    while the Spark branch owns the latest opened day: the Thrift server runs then, from
    the opening of a Spark day to the opening of the next DuckDB day (D6 revised). So it
    runs the night after each Spark day; Bronze is then at most 8 days old. The run waits
    for the other runs of this location (D30)."""
    try:
        conn = cal.connect_benchmark()
        try:
            owner = cal.CalendarStore(conn).owner()
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - calendar not created yet, or Postgres down
        return SkipReason(f"Calendar unreachable: {e}")
    if owner != cal.BRANCH_SPARK:
        return SkipReason(f"Branch {owner} owns the latest day: the Thrift server is stopped")
    previous = [
        run
        for run in active_location_runs(context.instance, iceberg_maintenance_job.name)
        if run.job_name == iceberg_maintenance_job.name
    ]
    if previous:
        return SkipReason(f"Iceberg maintenance run {previous[0].run_id} still in progress")
    return RunRequest()


@asset(
    name="iceberg_hourly_compaction",
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_SPARK, GROUP],
    group_name=GROUP,
    description="Hourly compaction of the Bronze table on Spark days (D33): today's small "
    "files of the 5-second commits, so that incremental Silver reads stay fast. Manifests, "
    "snapshot expiration and retention stay nightly.",
)
def iceberg_hourly_compaction(context: AssetExecutionContext) -> MaterializeResult:
    """rewrite_data_files on today's partition, through the Thrift server. No
    rewrite_manifests: the streaming appends already merge manifests (Iceberg's merge on
    commit, from 100 manifests) and one of those merges made it fail on 2026-10-08
    (`Deleted manifest ... could not be found in the latest snapshot`); it stays in the
    nightly maintenance, when the engine is stopped. Waits for a Silver/Gold run in
    progress (D30); its cost is a benchmark result (H6), the slowdown without it was one
    (H8)."""
    wait_for_runs(
        context.instance, context.log, context.run_id, None, config.SPARK_RUN_WAIT_SECONDS
    )
    sql = thrift.records
    before = iceberg.table_stats(sql, BRONZE)
    started = time.monotonic()
    rewritten = iceberg.rewrite_data_files(sql, BRONZE, iceberg.today_filter("event_time"))
    seconds = round(time.monotonic() - started, 1)
    after = iceberg.table_stats(sql, BRONZE)
    context.log.info(f"Bronze: {before.data_files} -> {after.data_files} files in {seconds} s")
    return MaterializeResult(
        metadata={
            **{f"before_{k}": v for k, v in before.as_dict().items()},
            **{f"after_{k}": v for k, v in after.as_dict().items()},
            "rewrite_data_files_seconds": seconds,
            "rewrite_data_files": rewritten,
        }
    )


iceberg_compaction_job = define_asset_job(
    name="bluesky_iceberg_hourly_compaction",
    selection=AssetSelection.assets(iceberg_hourly_compaction),
    description="Hourly compaction of the Spark branch's Bronze table on its days (D33).",
)


@schedule(
    job=iceberg_compaction_job,
    cron_schedule=config.ICEBERG_HOURLY_COMPACTION_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def iceberg_compaction_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every hour at :50 while the Spark branch's Silver/Gold is due (its engine runs, or
    stopped less than an hour ago): the Thrift server is up then. One at a time. Skipped
    on the dates of ICEBERG_HOURLY_COMPACTION_OFF_DAYS (measured without it, D33)."""
    today = datetime.now(ZoneInfo(config.DAGSTER_TIMEZONE)).date().isoformat()
    if today in config.ICEBERG_HOURLY_COMPACTION_OFF_DAYS:
        return SkipReason(f"No hourly compaction on {today}: measured without it (D33)")
    days = monitor.recent_days()
    due = monitor.transform_due(days, cal.BRANCH_SPARK, datetime.now(UTC)) if days else None
    if due is None:
        return SkipReason("No day of the Spark branch running or just finished")
    previous = [
        run
        for run in active_location_runs(context.instance, iceberg_compaction_job.name)
        if run.job_name == iceberg_compaction_job.name
    ]
    if previous:
        return SkipReason(f"Compaction run {previous[0].run_id} still in progress")
    return RunRequest(tags={"bluesky/transform_due": due})


spark_silver_gold_job = define_asset_job(
    name="bluesky_spark_silver_gold",
    selection=AssetSelection.keys(spark_bronze.key) | AssetSelection.assets(spark_dbt_models),
    description="Observe Bronze, then Silver (with catch-up passes) and Gold, dbt tests as checks.",
)

spark_nightly_checks_job = define_asset_job(
    name="bluesky_spark_nightly_checks",
    # The dbt tests only, no model: rerun over a day instead of 2 hours (D25)
    selection=AssetSelection.checks_for_assets(spark_dbt_models),
    description="Every dbt test of Silver and Gold over the last Spark day (D25, D30).",
    tags={config.NIGHTLY_CHECKS_TAG: "true"},
)


@schedule(
    job=spark_silver_gold_job,
    cron_schedule=config.DAGSTER_SCHEDULE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def spark_silver_gold_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every 15 min on the Spark branch's days only (D28), as the DuckDB branch's: its
    engine runs, or
    stopped less than an hour ago. Any run of this location blocks it (D30)."""
    days = monitor.recent_days()
    due = monitor.transform_due(days, cal.BRANCH_SPARK, datetime.now(UTC)) if days else None
    if due is None:
        return SkipReason("No day of the Spark branch running or just finished")
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
    """Every night at 03:00 (Europe/Paris), only after a day of the Spark branch (D30): the
    Thrift server is still up (it runs until the next DuckDB day opens). The run waits for the
    other runs of this location."""
    days = monitor.recent_days()
    due = monitor.nightly_checks_due(days, cal.BRANCH_SPARK, datetime.now(UTC)) if days else None
    if due is None:
        return SkipReason("The day that just ended was not a finished day of the Spark branch")
    previous = [
        run
        for run in active_location_runs(context.instance, spark_nightly_checks_job.name)
        if run.job_name == spark_nightly_checks_job.name
    ]
    if previous:
        return SkipReason(f"Nightly checks run {previous[0].run_id} still in progress")
    return RunRequest(tags={"bluesky/nightly_checks_due": due})


# Controlled test (D36): the night after a Spark day, on the bench namespaces
spark_bench_job, spark_bench_schedule = bench_night_definitions(
    cal.BRANCH_SPARK, drop_iceberg_bench, spark_transform(logged_backlog), spark_arrivals
)


defs = Definitions(
    assets=[
        spark_bronze,
        spark_dbt_models,
        iceberg_bronze_maintenance,
        iceberg_transform_maintenance,
        housekeeping,
        iceberg_hourly_compaction,
        iceberg_day_completeness,
        spark_benchmark_windows,
    ],
    jobs=[
        spark_silver_gold_job,
        spark_nightly_checks_job,
        iceberg_maintenance_job,
        iceberg_compaction_job,
        iceberg_completeness_job,
        spark_bench_job,
    ],
    schedules=[
        spark_silver_gold_schedule,
        spark_nightly_checks_schedule,
        iceberg_maintenance_schedule,
        iceberg_compaction_schedule,
        spark_bench_schedule,
    ],
    sensors=[
        failure_alert_sensor(
            "spark_failure_alert_sensor",
            [
                spark_silver_gold_job,
                spark_nightly_checks_job,
                iceberg_maintenance_job,
                iceberg_compaction_job,
                iceberg_completeness_job,
            ],
        ),
        iceberg_completeness_sensor,
    ],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
