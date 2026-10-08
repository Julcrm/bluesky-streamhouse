"""
Dagster Definitions of the DuckDB branch, served by the gRPC code server `bluesky_duckdb`
(decision D20) to the shared dagster-workspace.

Runs execute in this container (DefaultRunLauncher), so dbt and DuckDB memory count
against its own limit: the cost of the DuckDB branch's transformations stays attributable.
"""

from datetime import UTC, datetime

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    schedule,
)
from dagster_dbt import DbtCliResource

from src import config
from src.alternation import calendar as cal
from src.alternation import monitor
from src.dagster.alternation import (
    branch_calendar,
    calendar_job,
    close_day_schedule,
    completeness_job,
    ducklake_day_completeness,
    open_day_schedule,
)
from src.dagster.assets import bluesky_dbt_models, dbt_project, quix_bronze
from src.dagster.jobs import maintenance_job, nightly_checks_job, silver_gold_job
from src.dagster.maintenance import maintenance_assets
from src.dagster.runs import active_location_runs, blocks_schedule
from src.dagster.sensors import calendar_alert_sensor, failure_alert_sensor

__all__ = [
    "blocks_schedule",
    "defs",
    "maintenance_job",
    "maintenance_schedule",
    "nightly_checks_job",
    "nightly_checks_schedule",
    "silver_gold_job",
    "silver_gold_schedule",
]


@schedule(
    job=silver_gold_job,
    cron_schedule=config.DAGSTER_SCHEDULE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def silver_gold_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every 15 min on the DuckDB branch's days only (D28), one run at a time: a catch-up run can
    outlast the interval, and a second dbt would wait on the first one's DuckDB file
    lock and fail."""
    due = transform_due()
    if due is None:
        return SkipReason("No day of the DuckDB branch running or just finished")
    # Any run of this code location blocks it, maintenance included (same catalogs)
    active = active_location_runs(context.instance, silver_gold_job.name)
    if active:
        return SkipReason(f"Run {active[0].run_id} ({active[0].job_name}) still in progress")
    return RunRequest(tags={"bluesky/transform_due": due})


def transform_due() -> str | None:
    """Why the DuckDB branch's Silver/Gold runs now (its engine runs, or stopped less than an
    hour ago), None otherwise. CPU spent on dbt while the Spark branch is measured would bias
    the benchmark. Calendar unreachable: not run (the engine does not run either)."""
    days = monitor.recent_days()
    return monitor.transform_due(days, cal.BRANCH_DUCKDB, datetime.now(UTC)) if days else None


def nightly_checks_due() -> str | None:
    """Why the DuckDB branch's nightly tests run tonight (the day that just ended was
    its own, D30),
    None otherwise. Calendar unreachable: not run."""
    days = monitor.recent_days()
    return monitor.nightly_checks_due(days, cal.BRANCH_DUCKDB, datetime.now(UTC)) if days else None


@schedule(
    job=maintenance_job,
    cron_schedule=config.MAINTENANCE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def maintenance_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every night at 02:00 (Europe/Paris), outside the 07:00-19:00 window (D10). A
    Silver/Gold run in progress is waited for by the maintenance itself."""
    previous = [
        run
        for run in active_location_runs(context.instance, maintenance_job.name)
        if run.job_name == maintenance_job.name
    ]
    if previous:
        return SkipReason(f"Maintenance run {previous[0].run_id} still in progress")
    return RunRequest()


@schedule(
    job=nightly_checks_job,
    cron_schedule=config.NIGHTLY_CHECKS_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def nightly_checks_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every night at 03:00 (Europe/Paris), after the maintenance: every dbt test over a
    day (D25), only after a day of the DuckDB branch (D30). The run itself waits for the other
    runs of the location."""
    due = nightly_checks_due()
    if due is None:
        return SkipReason("The day that just ended was not a finished day of the DuckDB branch")
    previous = [
        run
        for run in active_location_runs(context.instance, nightly_checks_job.name)
        if run.job_name == nightly_checks_job.name
    ]
    if previous:
        return SkipReason(f"Nightly checks run {previous[0].run_id} still in progress")
    return RunRequest(tags={"bluesky/nightly_checks_due": due})


defs = Definitions(
    assets=[
        quix_bronze,
        bluesky_dbt_models,
        *maintenance_assets,
        branch_calendar,
        ducklake_day_completeness,
    ],
    jobs=[silver_gold_job, maintenance_job, nightly_checks_job, calendar_job, completeness_job],
    schedules=[
        silver_gold_schedule,
        maintenance_schedule,
        nightly_checks_schedule,
        open_day_schedule,
        close_day_schedule,
    ],
    sensors=[failure_alert_sensor, calendar_alert_sensor],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
