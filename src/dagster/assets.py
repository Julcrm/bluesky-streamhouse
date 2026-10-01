"""
Dagster assets of branch B (code location `bluesky_duckdb`, decision D20).
Assets contain no business logic: they delegate to dbt and to the processing modules.

- `quix_bronze`: Bronze table written by the Quix sink, observed (current snapshot).
- dbt models: one asset per Silver and Gold model, dbt tests as asset checks. The
  project and its manifest are found from `src.config`, never an absolute path.

Keys are `bluesky/<layer>/<name>` and groups are the layers: the Dagster catalog is
shared with velib, each project keeps its own folder.
"""

import json
import subprocess
from collections.abc import Iterator, Mapping
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Failure,
    ObserveResult,
    Output,
    RetryPolicy,
    observable_source_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from src import config
from src.dagster.runs import wait_for_runs
from src.processing.backlog import (
    Backlog,
    current_backlog,
    gold_max_hours_per_run,
    silver_max_rows_per_run,
)
from src.resources.ducklake import connect

dbt_project = DbtProject(
    project_dir=config.DBT_DUCKDB_PROJECT_DIR,
    profiles_dir=config.DBT_DUCKDB_PROJECT_DIR,
)
# `dagster dev` compiles the manifest; the image ships the one built by `dbt parse`
dbt_project.prepare_if_dev()


@observable_source_asset(
    name="quix_bronze",
    key_prefix=[config.DAGSTER_ASSET_PREFIX, "bronze"],
    group_name="bronze",
    description="DuckLake Bronze table written by the Quix Streams sink (catalog `bronze`).",
)
def quix_bronze() -> ObserveResult:
    """Current snapshot of the Bronze catalog: a new version every Quix checkpoint."""
    conn = connect(read_only=True)
    try:
        snapshot_id, snapshot_time = conn.execute(
            "SELECT snapshot_id, CAST(snapshot_time AS VARCHAR) "
            f"FROM ducklake_snapshots('{config.DUCKLAKE_BRONZE_ALIAS}') "
            "ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    return ObserveResult(
        data_version=DataVersion(str(snapshot_id)),
        metadata={"snapshot_id": snapshot_id, "snapshot_time": snapshot_time},
    )


def _stop(process: subprocess.Popen, timeout: float = 30) -> None:
    """Terminate a dbt subprocess that is still running, then kill it if it hangs."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()


def _is_silver(key: AssetKey) -> bool:
    """Silver models are named silver_* (their asset key ends with the model name)."""
    return key.path[-1].startswith("silver_")


def _silver_selected(context: AssetExecutionContext) -> bool:
    """True when the run materializes at least one Silver model."""
    return any(_is_silver(key) for key in context.selected_asset_keys)


class BlueskyDbtTranslator(DagsterDbtTranslator):
    """dbt models as `bluesky/<layer>/<model>` in the group of their layer (silver, gold).

    The Bronze source keeps the key set in its dbt meta (the `quix_bronze` asset).
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props["resource_type"] != "model":
            return key
        return key.with_prefix(config.DAGSTER_ASSET_PREFIX)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if dbt_resource_props["resource_type"] != "model":
            return super().get_group_name(dbt_resource_props)
        # fqn = [project, layer folder, ..., model]
        return dbt_resource_props["fqn"][1]


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=BlueskyDbtTranslator(),
    retry_policy=RetryPolicy(
        max_retries=config.DAGSTER_RETRY_MAX, delay=config.DAGSTER_RETRY_DELAY_SECONDS
    ),
)
def bluesky_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
    """Silver then Gold, with dbt tests as asset checks.

    Catch-up (option (a), decision D20): while Silver or Gold is behind by more than one
    run's cap, dbt runs again in silent passes; the final `dbt build` then reads the rest
    and publishes materializations and checks. A pass that moves no read position fails
    the run: looping on it would hide a stalled model (prod, 2026-09-28 to 2026-10-01).
    """
    if context.run.tags.get(config.NIGHTLY_CHECKS_TAG) == "true":
        # Nightly checks (D25): the same tests over a day, alone. One dbt at a time in
        # this container (DuckDB file lock), and no test while the maintenance rewrites
        wait_for_runs(
            context.instance, context.log, context.run_id, None, config.NIGHTLY_CHECKS_WAIT_SECONDS
        )
        invocation = dbt.cli(
            ["build", "--vars", json.dumps(config.NIGHTLY_TEST_VARS)], context=context
        )
        try:
            yield from invocation.stream()
        finally:
            _stop(invocation.process)
        return

    passes = 0
    if _silver_selected(context):
        silver_cap, gold_cap = silver_max_rows_per_run(), gold_max_hours_per_run()

        def measure() -> Backlog:
            # Counting past the cap would not change the decision (and took ~65 s on 82 M
            # rows); progress is checked on read positions anyway
            return current_backlog(dbt_project.manifest_path, silver_limit=silver_cap + 1)

        def silver_left(backlog: Backlog) -> str:
            return (
                f"over {silver_cap}"
                if backlog.silver_rows > silver_cap
                else str(backlog.silver_rows)
            )

        backlog = measure()
        context.log.info(
            f"Backlog: {silver_left(backlog)} Bronze rows for Silver (cap {silver_cap} per run), "
            f"{backlog.gold_hours} hours for Gold (cap {gold_cap})"
        )
        while (
            backlog.silver_rows > silver_cap or backlog.gold_hours > gold_cap
        ) and passes < config.CATCHUP_MAX_PASSES:
            # Not streamed: Dagster accepts a single materialization per asset and run
            dbt.cli(["run"]).wait()
            passes += 1
            previous, backlog = backlog, measure()
            context.log.info(
                f"Catch-up pass {passes}: {silver_left(backlog)} Bronze rows, "
                f"{backlog.gold_hours} Gold hours left"
            )
            if not backlog.progressed_from(previous):
                # Deterministic: a retry would loop the same way
                raise Failure(
                    f"Catch-up pass {passes} moved no read position ({previous} -> {backlog}): "
                    "a model is not moving its read position",
                    allow_retries=False,
                )
    invocation = dbt.cli(["build"], context=context)
    try:
        for event in invocation.stream():
            # Benchmark metadata: how many extra passes this run needed
            if isinstance(event, Output) and _is_silver(
                context.asset_key_for_output(event.output_name)
            ):
                event = event.with_metadata({**event.metadata, "catchup_passes": passes})
            yield event
    finally:
        # If the op fails mid-stream, dbt would keep writing and hold the DuckDB file
        # lock while the retry starts a second dbt over the same tables
        _stop(invocation.process)
