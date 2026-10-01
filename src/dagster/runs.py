"""
Runs of this code location in the shared Dagster instance (velib runs there too, D20).

Used by the Silver/Gold schedule (one run at a time) and by the maintenance, which must
not write the transform catalog while dbt does, nor purge another project's runs.
"""

from dagster import DagsterInstance, DagsterRun, DagsterRunStatus, RunsFilter

from src import config

# Statuses of a run that is not finished yet
ACTIVE_RUN_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]


def in_location(run: DagsterRun) -> bool:
    """True for a run launched from this code location (never a velib run)."""
    origin = run.remote_job_origin
    return origin is not None and (
        origin.repository_origin.code_location_origin.location_name == config.DAGSTER_CODE_LOCATION
    )


def blocks_schedule(run: DagsterRun, job_name: str = "bluesky_silver_gold") -> bool:
    """True for a run of this code location: the scheduled job, a manual materialization
    from the UI (`__ASSET_JOB`), which writes the same tables, or the maintenance."""
    if run.remote_job_origin is None:  # created outside a code location (tests, in process)
        return run.job_name == job_name
    return in_location(run)


def active_location_runs(
    instance: DagsterInstance, job_name: str, exclude_run_id: str | None = None
) -> list[DagsterRun]:
    """Unfinished runs of this code location, other than `exclude_run_id`."""
    return [
        record.dagster_run
        for record in instance.get_run_records(RunsFilter(statuses=ACTIVE_RUN_STATUSES))
        if record.dagster_run.run_id != exclude_run_id
        and blocks_schedule(record.dagster_run, job_name)
    ]
