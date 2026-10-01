"""
Dagster Definitions of branch B, served by the gRPC code server `bluesky_duckdb`
(decision D20) to the shared dagster-workspace.

Runs execute in this container (DefaultRunLauncher), so dbt and DuckDB memory count
against its own limit: the cost of branch B's transformations stays attributable.
"""

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
from src.dagster.assets import bluesky_dbt_models, dbt_project, quix_bronze
from src.dagster.jobs import maintenance_job, nightly_checks_job, silver_gold_job
from src.dagster.maintenance import maintenance_assets
from src.dagster.runs import active_location_runs, blocks_schedule
from src.dagster.sensors import failure_alert_sensor

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
    """Every 15 min, one run at a time: a catch-up run can outlast the interval, and a
    second dbt would wait on the first one's DuckDB file lock and fail."""
    # Any run of this code location blocks it, maintenance included (same catalogs)
    active = active_location_runs(context.instance, silver_gold_job.name)
    if active:
        return SkipReason(f"Run {active[0].run_id} ({active[0].job_name}) still in progress")
    return RunRequest()


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
    day (D25). The run itself waits for the other runs of the location."""
    previous = [
        run
        for run in active_location_runs(context.instance, nightly_checks_job.name)
        if run.job_name == nightly_checks_job.name
    ]
    if previous:
        return SkipReason(f"Nightly checks run {previous[0].run_id} still in progress")
    return RunRequest()


defs = Definitions(
    assets=[quix_bronze, bluesky_dbt_models, *maintenance_assets],
    jobs=[silver_gold_job, maintenance_job, nightly_checks_job],
    schedules=[silver_gold_schedule, maintenance_schedule, nightly_checks_schedule],
    sensors=[failure_alert_sensor],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
