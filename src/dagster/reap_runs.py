"""
Close the runs of this code location left unfinished by the previous container.

Runs execute inside the code server container (DefaultRunLauncher, D20), so when a
redeploy replaces the container, its runs die with it but stay STARTED in the shared
instance: the Silver/Gold schedule then skips every tick until someone cancels them by
hand (twice on 2026-09-28, again on 2026-10-01). Dagster's run monitoring cannot see it:
the DefaultRunLauncher does not support run worker health checks. So the new container,
before serving, marks them: STARTING/STARTED as failed (the failure sensor emails it),
CANCELING as canceled. No run of this location can be alive yet: the gRPC server that
would run it is not listening.

Usage (container entrypoint): python -m src.dagster.reap_runs
"""

from collections.abc import Callable

from dagster import DagsterInstance, DagsterRun, DagsterRunStatus, RunsFilter

from src.dagster.runs import in_location

DEAD_STATUSES = [DagsterRunStatus.STARTING, DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]
REASON = "Run worker gone: the code server container was replaced (redeploy or restart)"


def reap_orphan_runs(
    instance: DagsterInstance, is_ours: Callable[[DagsterRun], bool] = in_location
) -> list[str]:
    """Close this location's unfinished runs; returns their ids."""
    reaped = []
    for record in instance.get_run_records(RunsFilter(statuses=DEAD_STATUSES)):
        run = record.dagster_run
        if not is_ours(run):
            continue
        if run.status == DagsterRunStatus.CANCELING:
            instance.report_run_canceled(run, message=REASON)
        else:
            instance.report_run_failed(run, message=REASON)
        reaped.append(run.run_id)
    return reaped


def main() -> None:
    # Never block the code server: a storage error is logged, serving goes on
    try:
        with DagsterInstance.get() as instance:
            reaped = reap_orphan_runs(instance)
        print(f"Orphan runs closed: {reaped or 'none'}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"Orphan run check skipped: {e!r}", flush=True)


if __name__ == "__main__":
    main()
