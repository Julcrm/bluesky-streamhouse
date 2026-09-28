"""Tests for the Dagster code location of branch B (needs the dbt manifest: `make dbt-parse`)."""

import subprocess
import sys

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
    assert "quix_bronze" in keys
    assert {"silver/silver_posts", "gold/gold_hashtags_hour"} <= keys
    assert len(defs.get_repository_def().asset_graph.asset_check_keys) > 0


def test_schedule_runs_when_idle() -> None:
    """No run in progress: the schedule requests a run."""
    from src.dagster.definitions import silver_gold_schedule

    with instance_for_test() as instance:
        result = silver_gold_schedule(build_schedule_context(instance=instance))
    assert isinstance(result, RunRequest)


def test_schedule_skips_while_a_run_is_active() -> None:
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
