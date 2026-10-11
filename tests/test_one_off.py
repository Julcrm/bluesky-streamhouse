"""Tests for src.dagster.one_off (one-off runs launched once at a set time)."""

from datetime import datetime, timedelta

import pytest
from dagster import RunRequest, build_sensor_context, job, op

from src import config
from src.dagster.one_off import OneOffRun, due_runs, one_off_sensor

GRACE = timedelta(hours=12)
RUN = OneOffRun("windows-0810", "some_job", "2026-10-12T19:20:00+02:00", {"ops": {}})


def _at(text: str) -> datetime:
    return datetime.fromisoformat(text)


def test_due_only_between_its_time_and_the_grace_period() -> None:
    assert due_runs([RUN], _at("2026-10-12T19:19:59+02:00"), GRACE) == []
    assert due_runs([RUN], _at("2026-10-12T19:20:00+02:00"), GRACE) == [RUN]
    # Same instant in UTC
    assert due_runs([RUN], _at("2026-10-12T17:30:00+00:00"), GRACE) == [RUN]
    # A stale entry left in the list never fires after a later redeploy
    assert due_runs([RUN], _at("2026-10-13T07:20:00+02:00"), GRACE) == []


def test_sensor_requests_with_the_key_as_run_key(monkeypatch) -> None:
    @op
    def noop() -> None:
        pass

    @job(name="some_job")
    def some_job() -> None:
        noop()

    sensor = one_off_sensor("test_one_off", [some_job], [RUN], GRACE, 60)
    monkeypatch.setattr("src.dagster.one_off.due_runs", lambda runs, now, grace: list(runs))
    requests = sensor.evaluate_tick(build_sensor_context()).run_requests
    assert len(requests) == 1
    request: RunRequest = requests[0]
    assert (request.run_key, request.job_name, request.run_config) == (
        "windows-0810",
        "some_job",
        {"ops": {}},
    )
    monkeypatch.setattr("src.dagster.one_off.due_runs", lambda runs, now, grace: [])
    result = sensor.evaluate_tick(build_sensor_context())
    assert result.run_requests == [] and result.skip_message


def test_unknown_job_refused_at_load() -> None:
    with pytest.raises(ValueError, match="not in this location"):
        one_off_sensor("test_one_off", [], [RUN], GRACE, 60)


def test_configured_entries_have_unique_keys_and_valid_times() -> None:
    runs = [*config.ONE_OFF_RUNS_SPARK, *config.ONE_OFF_RUNS_DUCKDB]
    keys = [run[0] for run in runs]
    assert len(keys) == len(set(keys))
    for run in runs:
        assert datetime.fromisoformat(run[2]).tzinfo is not None
