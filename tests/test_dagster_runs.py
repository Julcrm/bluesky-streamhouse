"""Tests for src.dagster.runs: waiting for the other runs of the code location."""

from types import SimpleNamespace

import pytest
from dagster import Failure

from src import config
from src.dagster.runs import wait_for_runs


def _origin(location: str) -> SimpleNamespace:
    return SimpleNamespace(
        repository_origin=SimpleNamespace(
            code_location_origin=SimpleNamespace(location_name=location)
        )
    )


class FakeInstance:
    """Run records in insertion order (storage id), all unfinished."""

    def __init__(self, *runs: tuple[str, str] | tuple[str, str, str]) -> None:
        self.records = [
            SimpleNamespace(
                storage_id=storage_id,
                dagster_run=SimpleNamespace(
                    run_id=run[0],
                    job_name=run[1],
                    remote_job_origin=_origin(
                        run[2] if len(run) > 2 else config.DAGSTER_CODE_LOCATION
                    ),
                ),
            )
            for storage_id, run in enumerate(runs, start=1)
        ]

    def get_run_records(self, filters):
        return list(self.records)

    def get_run_record_by_id(self, run_id):
        return next((r for r in self.records if r.dagster_run.run_id == run_id), None)

    def finish(self, run_id: str) -> None:
        self.records = [r for r in self.records if r.dagster_run.run_id != run_id]


class Log:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def info(self, line: str) -> None:
        self.lines.append(line)


def test_later_waiting_run_does_not_hold_an_earlier_one() -> None:
    """20/10: the 18:50 compaction and the 19:01 completeness check both waited for any
    run, held each other for 2 h; the earlier run now goes first."""
    instance = FakeInstance(
        ("compaction", "bluesky_iceberg_hourly_compaction"),
        ("completeness", "bluesky_iceberg_day_completeness"),
    )
    log = Log()
    wait_for_runs(instance, log, "compaction", None, timeout_seconds=0, poll_seconds=0)
    assert log.lines == []


def test_later_run_waits_for_an_earlier_one() -> None:
    instance = FakeInstance(
        ("compaction", "bluesky_iceberg_hourly_compaction"),
        ("completeness", "bluesky_iceberg_day_completeness"),
    )
    with pytest.raises(Failure, match="compaction"):
        wait_for_runs(instance, Log(), "completeness", None, 0, poll_seconds=0)
    instance.finish("compaction")
    wait_for_runs(instance, Log(), "completeness", None, 0, poll_seconds=0)


def test_only_the_named_jobs_and_this_location() -> None:
    instance = FakeInstance(
        ("velib", "velib_pipeline_job", "velib"),
        ("checks", "bluesky_spark_nightly_checks"),
        ("gold", "bluesky_spark_silver_gold"),
        ("maintenance", "bluesky_iceberg_maintenance"),
    )
    with pytest.raises(Failure, match="gold"):
        wait_for_runs(instance, Log(), "maintenance", {"bluesky_spark_silver_gold"}, 0, 0)
    instance.finish("gold")
    wait_for_runs(instance, Log(), "maintenance", {"bluesky_spark_silver_gold"}, 0, 0)
