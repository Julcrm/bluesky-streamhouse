"""Unit tests for src.dagster.housekeeping: dbt run folders and old Dagster runs."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dagster import DagsterRunStatus, instance_for_test, job, op

from src.dagster import housekeeping

NOW = datetime(2026, 10, 10, 2, 30, tzinfo=UTC)


def _folder(target: Path, name: str, age_days: float) -> Path:
    path = target / name
    path.mkdir()
    (path / "manifest.json").write_bytes(b"x" * 1000)
    moment = (NOW - timedelta(days=age_days)).timestamp()
    os.utime(path, (moment, moment))
    return path


def test_old_run_folders_go_the_image_manifest_stays(tmp_path: Path) -> None:
    old = _folder(tmp_path, "spark_dbt_models-c975c12-5fe3d6e", 3)
    loose = _folder(tmp_path, "1d99461", 3)
    recent = _folder(tmp_path, "bluesky_dbt_models-afd1545-93ff1b6", 1)
    compiled = _folder(tmp_path, "compiled", 10)  # `dbt build` run by hand
    (tmp_path / "manifest.json").write_text("{}")  # built with the image
    (tmp_path / "partial_parse.msgpack").write_bytes(b"\0")

    cutoff = NOW - timedelta(days=2)
    assert housekeeping.purge_dbt_targets(tmp_path, cutoff) == (2, 2000)
    assert not old.exists() and not loose.exists()
    assert recent.exists() and compiled.exists()
    assert (tmp_path / "manifest.json").exists() and (tmp_path / "partial_parse.msgpack").exists()


def test_missing_target_folder_is_not_an_error(tmp_path: Path) -> None:
    assert housekeeping.purge_dbt_targets(tmp_path / "target", NOW) == (0, 0)


@op
def _noop() -> None:
    pass


@job
def _sample() -> None:
    _noop()


def test_only_this_locations_finished_runs_are_purged(monkeypatch) -> None:
    """The instance is shared with velib: a run of another location is never deleted."""
    with instance_for_test() as instance:
        finished = _sample.execute_in_process(instance=instance).run_id
        mine = {finished}
        monkeypatch.setattr(housekeeping, "in_location", lambda run: run.run_id in mine)
        future = datetime.now(UTC) + timedelta(days=1)
        assert instance.get_run_by_id(finished).status == DagsterRunStatus.SUCCESS
        assert housekeeping.purge_finished_runs(instance, future) == 1
        assert instance.get_run_by_id(finished) is None
        other = _sample.execute_in_process(instance=instance).run_id
        assert housekeeping.purge_finished_runs(instance, future) == 0
        assert instance.get_run_by_id(other) is not None
