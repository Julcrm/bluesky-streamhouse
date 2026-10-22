"""
Run status sensors recording each finished Silver/Gold pass (src/benchmark/passes.py):
one pair (success, failure) per code location.
"""

from datetime import UTC, datetime

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from src import config
from src.alternation import calendar as cal
from src.benchmark import passes


def _record(context: RunStatusSensorContext, branch: str) -> None:
    run = context.dagster_run
    record = context.instance.get_run_record_by_id(run.run_id)
    when = lambda ts: datetime.fromtimestamp(ts, UTC) if ts else None  # noqa: E731
    row = passes.pass_row(
        run.run_id,
        branch,
        run.job_name,
        run.status.value,
        run.tags.get(config.NIGHTLY_CHECKS_TAG) == "true",
        when(record.start_time) if record else None,
        when(record.end_time) if record else None,
    )
    if row is None:
        return
    conn = cal.connect_benchmark()
    try:
        passes.record(conn, row)
    finally:
        conn.close()


def pass_sensors(prefix: str, branch: str, jobs: list) -> list:
    """Two sensors writing the passes of `jobs`, finished in success or failure."""

    @run_status_sensor(
        name=f"{prefix}_pass_success_sensor",
        run_status=DagsterRunStatus.SUCCESS,
        monitored_jobs=jobs,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def on_success(context: RunStatusSensorContext) -> None:
        _record(context, branch)

    @run_status_sensor(
        name=f"{prefix}_pass_failure_sensor",
        run_status=DagsterRunStatus.FAILURE,
        monitored_jobs=jobs,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def on_failure(context: RunStatusSensorContext) -> None:
        _record(context, branch)

    return [on_success, on_failure]
