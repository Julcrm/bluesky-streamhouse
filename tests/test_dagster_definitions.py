"""Tests for the Dagster code location of branch B (needs the dbt manifest: `make dbt-parse`)."""

import subprocess
import sys
from types import SimpleNamespace

import pytest
from dagster import (
    DagsterRunStatus,
    RunRequest,
    SkipReason,
    build_schedule_context,
    instance_for_test,
)

from src.dagster.assets import dbt_project

pytestmark = pytest.mark.skipif(
    not dbt_project.manifest_path.exists(), reason="dbt manifest missing (make dbt-parse)"
)


def test_definitions_load() -> None:
    """One job over Bronze and every dbt model, one schedule, dbt tests as checks."""
    from src.dagster.definitions import defs, silver_gold_job

    job = defs.get_job_def(silver_gold_job.name)
    keys = {key.to_user_string() for key in job.asset_layer.executable_asset_keys}
    assert "bluesky/bronze/quix_bronze" in keys
    assert {"bluesky/silver/silver_posts", "bluesky/gold/gold_hashtags_hour"} <= keys
    assert len(defs.get_repository_def().asset_graph.asset_check_keys) > 0


@pytest.fixture
def b_day_running(monkeypatch) -> None:
    """The calendar gives a running day to branch B."""
    monkeypatch.setattr("src.dagster.definitions.transform_due", lambda: "day 2026-10-08 is open")


def test_schedule_runs_when_idle(b_day_running) -> None:
    """A day of B and no run in progress: the schedule requests a run."""
    from src.dagster.definitions import silver_gold_schedule

    with instance_for_test() as instance:
        result = silver_gold_schedule(build_schedule_context(instance=instance))
    assert isinstance(result, RunRequest)


def test_schedule_skips_outside_branch_b_days(monkeypatch) -> None:
    """A's day (or no calendar): no dbt in branch B, its CPU would bias A's measures."""
    from src.dagster.definitions import silver_gold_schedule

    monkeypatch.setattr("src.dagster.definitions.transform_due", lambda: None)
    with instance_for_test() as instance:
        result = silver_gold_schedule(build_schedule_context(instance=instance))
    assert isinstance(result, SkipReason)


def test_schedule_skips_while_a_run_is_active(b_day_running) -> None:
    """A catch-up run still going: the next tick is skipped, never two runs at once."""
    from src.dagster.definitions import defs, silver_gold_job, silver_gold_schedule

    with instance_for_test() as instance:
        instance.create_run_for_job(
            job_def=defs.get_job_def(silver_gold_job.name), status=DagsterRunStatus.STARTED
        )
        result = silver_gold_schedule(build_schedule_context(instance=instance))
    assert isinstance(result, SkipReason)


def test_stop_terminates_a_running_dbt_process() -> None:
    """A dbt subprocess left running by a failed op is stopped before the retry."""
    from src.dagster.assets import _stop

    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    _stop(process, timeout=5)
    assert process.poll() is not None


def _run_from(location: str, job_name: str = "__ASSET_JOB") -> SimpleNamespace:
    """A run as launched by the dagster-workspace from a given code location."""
    origin = SimpleNamespace(
        repository_origin=SimpleNamespace(
            code_location_origin=SimpleNamespace(location_name=location)
        )
    )
    return SimpleNamespace(remote_job_origin=origin, job_name=job_name)


def test_manual_materialization_of_this_location_blocks_the_schedule() -> None:
    """A "Materialize all" from the UI runs as __ASSET_JOB: it must block the schedule."""
    from src.dagster.definitions import blocks_schedule

    assert blocks_schedule(_run_from("bluesky_duckdb"))


def test_runs_of_other_locations_do_not_block() -> None:
    """velib runs share the Dagster instance but never block branch B."""
    from src.dagster.definitions import blocks_schedule

    assert not blocks_schedule(_run_from("velib_lakehouse", "velib_pipeline_job"))


def test_maintenance_job_loads() -> None:
    """Both catalogs, storage with its two blocking checks, run purge; one alert sensor."""
    from src.dagster.definitions import defs, maintenance_job

    job = defs.get_job_def(maintenance_job.name)
    keys = {key.to_user_string() for key in job.asset_layer.executable_asset_keys}
    assert keys == {
        "bluesky/maintenance/bronze_maintenance",
        "bluesky/maintenance/transform_maintenance",
        "bluesky/maintenance/lake_storage",
        "bluesky/maintenance/dagster_run_purge",
    }
    checks = {key.name for key in defs.get_repository_def().asset_graph.asset_check_keys}
    assert {"bucket_under_alert", "catalog_under_alert"} <= checks
    assert defs.get_sensor_def("failure_alert_sensor") is not None
    assert defs.get_sensor_def("calendar_alert_sensor") is not None


def test_maintenance_schedule_skips_while_previous_maintenance_runs() -> None:
    """Two maintenance runs would CHECKPOINT the same catalogs at once."""
    from src.dagster.definitions import defs, maintenance_job, maintenance_schedule

    with instance_for_test() as instance:
        assert isinstance(
            maintenance_schedule(build_schedule_context(instance=instance)), RunRequest
        )
        instance.create_run_for_job(
            job_def=defs.get_job_def(maintenance_job.name), status=DagsterRunStatus.STARTED
        )
        result = maintenance_schedule(build_schedule_context(instance=instance))
    assert isinstance(result, SkipReason)


def test_purge_only_counts_runs_of_this_location() -> None:
    """The instance is shared with velib: its runs are never purged."""
    from src.dagster.runs import in_location

    assert in_location(_run_from("bluesky_duckdb"))
    assert not in_location(_run_from("velib_lakehouse", "velib_pipeline_job"))
    assert not in_location(SimpleNamespace(remote_job_origin=None, job_name="x"))


def test_failure_email_skipped_without_configuration(monkeypatch) -> None:
    """No Resend key or recipient: nothing is sent, the sensor only logs."""
    from src import config
    from src.dagster.sensors import send_failure_email

    monkeypatch.setattr(config, "RESEND_API_KEY", "")
    assert send_failure_email("run", "job", "error") is False


def test_every_asset_sits_in_the_project_folder() -> None:
    """The Dagster catalog is shared with velib: every key starts with `bluesky/<layer>`,
    and each layer is an asset group."""
    from src.dagster.definitions import defs

    graph = defs.get_repository_def().asset_graph
    layers = {"bronze", "silver", "gold", "maintenance", "alternation"}
    for key in graph.get_all_asset_keys():
        assert key.path[0] == "bluesky" and key.path[1] in layers, key
        assert graph.get(key).group_name == key.path[1], key


def test_reaper_closes_unfinished_runs_of_this_location_only() -> None:
    """At code server startup, runs left STARTED or CANCELING by the previous container
    are closed; finished runs and other projects' runs are left alone."""
    from src.dagster.definitions import defs, silver_gold_job
    from src.dagster.reap_runs import reap_orphan_runs

    with instance_for_test() as instance:
        job = defs.get_job_def(silver_gold_job.name)
        started = instance.create_run_for_job(job_def=job, status=DagsterRunStatus.STARTED)
        canceling = instance.create_run_for_job(job_def=job, status=DagsterRunStatus.CANCELING)
        other = instance.create_run_for_job(job_def=job, status=DagsterRunStatus.STARTED)
        done = instance.create_run_for_job(job_def=job, status=DagsterRunStatus.SUCCESS)
        reaped = reap_orphan_runs(instance, is_ours=lambda run: run.run_id != other.run_id)
        status = {r: instance.get_run_by_id(r).status for r in (started.run_id, canceling.run_id)}

        assert set(reaped) == {started.run_id, canceling.run_id}
        assert status[started.run_id] == DagsterRunStatus.FAILURE
        assert status[canceling.run_id] == DagsterRunStatus.CANCELED
        assert instance.get_run_by_id(other.run_id).status == DagsterRunStatus.STARTED
        assert instance.get_run_by_id(done.run_id).status == DagsterRunStatus.SUCCESS


def test_spark_code_location_loads_without_branch_b() -> None:
    """Branch A's code location: Iceberg maintenance under bluesky/maintenance, its own
    schedule and alert sensor, and no import of branch B's dbt project."""
    from src.dagster.spark_definitions import defs, iceberg_maintenance_job

    job = defs.get_job_def(iceberg_maintenance_job.name)
    keys = {key.to_user_string() for key in job.asset_layer.executable_asset_keys}
    assert keys == {"bluesky/maintenance/iceberg_bronze_maintenance"}
    assert defs.get_sensor_def("spark_failure_alert_sensor") is not None
    assert defs.get_sensor_def("iceberg_completeness_sensor") is not None
    assert defs.get_schedule_def("iceberg_maintenance_schedule") is not None
    # In a fresh interpreter: loading branch A must not load branch B's modules
    loaded = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, src.dagster.spark_definitions; "
            "print([m for m in sys.modules if m in ('src.dagster.assets', 'dagster_dbt')])",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert loaded == "[]"
