"""
Dagster Definitions of branch B, served by the gRPC code server `bluesky_duckdb`
(decision D20) to the shared dagster-workspace.

Runs execute in this container (DefaultRunLauncher), so dbt and DuckDB memory count
against its own limit: the cost of branch B's transformations stays attributable.
"""

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    RunsFilter,
    ScheduleEvaluationContext,
    SkipReason,
    define_asset_job,
    schedule,
)
from dagster_dbt import DbtCliResource

from src import config
from src.dagster.assets import bluesky_dbt_models, dbt_project, quix_bronze

# Statuses of a run that is not finished yet
ACTIVE_RUN_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]

silver_gold_job = define_asset_job(
    name="bluesky_silver_gold",
    selection=AssetSelection.keys(quix_bronze.key) | AssetSelection.assets(bluesky_dbt_models),
    description="Observe Bronze, then Silver (with catch-up passes) and Gold, dbt tests as checks.",
)


@schedule(
    job=silver_gold_job,
    cron_schedule=config.DAGSTER_SCHEDULE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def silver_gold_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Every 15 min, one run at a time: a catch-up run can outlast the interval."""
    active = context.instance.get_run_records(
        RunsFilter(job_name=silver_gold_job.name, statuses=ACTIVE_RUN_STATUSES), limit=1
    )
    if active:
        return SkipReason(f"Run {active[0].dagster_run.run_id} still in progress")
    return RunRequest()


defs = Definitions(
    assets=[quix_bronze, bluesky_dbt_models],
    jobs=[silver_gold_job],
    schedules=[silver_gold_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
