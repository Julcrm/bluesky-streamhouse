"""
One-off runs: a test or a catch-up launched once at a set time, without anyone at the
keyboard (decision of Julien, 2026-10-11). Each code location gets one sensor over its
entries of `config.ONE_OFF_RUNS`; the entry's key is the run key, so Dagster launches it
once even if the sensor sees it again. An entry is dropped from the list once it has
served; a stale one (past its grace period) is never launched, even after a redeploy.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from dagster import (
    DefaultSensorStatus,
    JobDefinition,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)


@dataclass(frozen=True)
class OneOffRun:
    """`job` launched once at `at` (ISO with offset), with this run config."""

    key: str
    job: str
    at: str
    run_config: dict[str, Any] = field(default_factory=dict)


def due_runs(runs: Sequence[OneOffRun], now: datetime, grace: timedelta) -> list[OneOffRun]:
    """Entries whose time has come and is less than `grace` old."""
    due = []
    for run in runs:
        at = datetime.fromisoformat(run.at)
        if at <= now < at + grace:
            due.append(run)
    return due


def one_off_sensor(
    name: str,
    jobs: Sequence[JobDefinition | UnresolvedAssetJobDefinition],
    runs: Sequence[OneOffRun],
    grace: timedelta,
    interval_seconds: int,
) -> SensorDefinition:
    """Sensor of one code location launching its due one-off runs, once each."""
    by_name = {job.name: job for job in jobs}
    unknown = sorted({run.job for run in runs} - by_name.keys())
    if unknown:
        raise ValueError(f"One-off runs of jobs not in this location: {unknown}")

    @sensor(
        name=name,
        jobs=list(jobs),
        minimum_interval_seconds=interval_seconds,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def _sensor(context: SensorEvaluationContext):
        due = due_runs(runs, datetime.now().astimezone(), grace)
        if not due:
            return SkipReason("No one-off run due")
        return [
            RunRequest(
                run_key=run.key,
                job_name=run.job,
                run_config=run.run_config,
                tags={"one_off": run.key},
            )
            for run in due
        ]

    return _sensor
