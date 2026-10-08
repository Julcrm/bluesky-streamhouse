"""
Nightly housekeeping of one code location, the same in both branches: finished Dagster
runs older than 30 days (D20) and the per-run dbt target folders of dagster-dbt.

Each dbt command of a run writes its own folder under the project's target/ (~3 MB:
manifest, run results, compiled SQL); nothing reads it once the run has logged its
events. Kept 2 days for debugging, then deleted: over a frozen measurement period with
no redeploy, they would add up to several GB per container.

No engine import here: branch A's code location must not load DuckDB, nor B's a JVM.
"""

import re
import shutil
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    DagsterInstance,
    DagsterRunStatus,
    MaterializeResult,
    RunsFilter,
    asset,
)

from src import config
from src.dagster.runs import in_location

FINISHED_RUN_STATUSES = [
    DagsterRunStatus.SUCCESS,
    DagsterRunStatus.FAILURE,
    DagsterRunStatus.CANCELED,
]
# dagster-dbt names a run's folder `<op>-<run id[:7]>-<uuid[:7]>`, or `<uuid[:7]>` out of
# a run; the manifest and partial parse file built with the image sit in target/ itself
RUN_TARGET_DIR = re.compile(r"^(?:.+-[0-9a-f]{7}-)?[0-9a-f]{7}$")


def purge_finished_runs(instance: DagsterInstance, cutoff: datetime) -> int:
    """Delete the finished runs of this code location created before `cutoff`."""
    records = instance.get_run_records(
        RunsFilter(statuses=FINISHED_RUN_STATUSES, created_before=cutoff)
    )
    # Never another project's runs: the instance is shared with velib
    ours = [r.dagster_run.run_id for r in records if in_location(r.dagster_run)]
    for run_id in ours:
        instance.delete_run(run_id)
    return len(ours)


def purge_dbt_targets(target_dir: Path, cutoff: datetime) -> tuple[int, int]:
    """Delete the per-run folders of `target_dir` last modified before `cutoff`;
    returns (folders, bytes) deleted."""
    if not target_dir.is_dir():
        return 0, 0
    folders = size = 0
    for path in target_dir.iterdir():
        if not (path.is_dir() and RUN_TARGET_DIR.match(path.name)):
            continue
        if datetime.fromtimestamp(path.stat().st_mtime, UTC) >= cutoff:
            continue
        size += sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
        shutil.rmtree(path, ignore_errors=True)
        folders += 1
    return folders, size


def housekeeping_asset(
    key_prefix: Sequence[str], group_name: str, target_dir: Path
) -> AssetsDefinition:
    """The housekeeping asset of one code location, in its nightly maintenance job.
    Independent of the other maintenance assets: a failed storage check or guard must
    not stop it."""

    @asset(
        name="housekeeping",
        key_prefix=list(key_prefix),
        group_name=group_name,
        description=f"Delete finished Dagster runs of this code location older than "
        f"{config.DAGSTER_RUN_RETENTION_DAYS} days (D20) and dbt run folders older than "
        f"{config.DBT_TARGET_RETENTION_DAYS} days.",
    )
    def housekeeping(context: AssetExecutionContext) -> MaterializeResult:
        now = datetime.now(UTC)
        run_cutoff = now - timedelta(days=config.DAGSTER_RUN_RETENTION_DAYS)
        target_cutoff = now - timedelta(days=config.DBT_TARGET_RETENTION_DAYS)
        runs = purge_finished_runs(context.instance, run_cutoff)
        folders, size = purge_dbt_targets(target_dir, target_cutoff)
        return MaterializeResult(
            metadata={
                "runs_deleted": runs,
                "runs_cutoff": run_cutoff.isoformat(),
                "dbt_target_folders_deleted": folders,
                "dbt_target_mb_deleted": round(size / 2**20, 1),
            }
        )

    return housekeeping
