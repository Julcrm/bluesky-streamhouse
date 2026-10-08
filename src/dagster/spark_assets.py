"""
Dagster assets of the Spark branch's Silver/Gold (code location `bluesky_spark`, decisions D20,
D29, D30). Assets contain no business logic: they delegate to dbt-spark, whose queries
run in the Spark Thrift server (D6 revised); this code server runs no JVM.

- `spark_bronze`: Iceberg Bronze table written by the Spark streaming job, observed
  (current snapshot, read through the Thrift server).
- dbt models: one asset per Silver and Gold model, dbt tests as asset checks, same
  models, tests, caps and catch-up loop as the DuckDB branch (src/dagster/assets.py).

Keys are `bluesky/spark/<layer>/<name>` (D30), groups are the layers.
"""

import json
from collections.abc import Iterator

from dagster import (
    AssetExecutionContext,
    DataVersion,
    Failure,
    ObserveResult,
    Output,
    observable_source_asset,
)
from dagster_dbt import DbtCliInvocation, DbtCliResource, DbtProject, dbt_assets

from src import config
from src.dagster.dbt_common import BlueskyDbtTranslator, is_silver, silver_selected, stop_process
from src.dagster.runs import wait_for_runs
from src.processing.bronze import BRONZE_TABLE
from src.processing.dbt_vars import gold_max_hours_per_run, silver_max_rows_per_run
from src.processing.spark.backlog import SparkBacklog, parse_backlog
from src.resources import thrift

# bluesky/spark/<layer>/<name> (D30)
KEY_PREFIX = [config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_SPARK]

dbt_project = DbtProject(
    project_dir=config.DBT_SPARK_PROJECT_DIR,
    profiles_dir=config.DBT_SPARK_PROJECT_DIR,
)
# `dagster dev` compiles the manifest; the image ships the one built by `dbt parse`
dbt_project.prepare_if_dev()


@observable_source_asset(
    name="spark_bronze",
    key_prefix=[*KEY_PREFIX, "bronze"],
    group_name="bronze",
    description="Iceberg Bronze table written by the Spark streaming job (namespace `bronze`).",
)
def spark_bronze() -> ObserveResult:
    """Current snapshot of the Bronze table: a new version every streaming micro-batch."""
    snapshot_id, committed_at = thrift.query(
        "SELECT snapshot_id, CAST(committed_at AS STRING) "
        f"FROM {config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    )[0]
    return ObserveResult(
        data_version=DataVersion(str(snapshot_id)),
        metadata={"snapshot_id": str(snapshot_id), "committed_at": committed_at},
    )


def _logged_backlog(invocation: DbtCliInvocation) -> SparkBacklog:
    """The backlog a dbt command logged (log_backlog macro), once it has finished."""
    messages = (
        event.raw_event.get("info", {}).get("msg", "") for event in invocation.stream_raw_events()
    )
    backlog = parse_backlog(messages)
    invocation.wait()  # raises if dbt failed
    if backlog is None:
        raise Failure(f"`{' '.join(invocation.dbt_command)}` logged no backlog")
    return backlog


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=BlueskyDbtTranslator(KEY_PREFIX),
)
def spark_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
    """Silver then Gold, with dbt tests as asset checks.

    Catch-up, as the DuckDB branch: while Silver or Gold is behind by more than one run's cap,
    dbt runs again in silent passes; the final `dbt build` then reads the rest and
    publishes materializations and checks. The backlog is measured by the Thrift server
    (`run-operation measure_backlog`, then each pass's own on-run-end line). A pass that
    moves no read position fails the run: looping on it would hide a stalled model.
    """
    if context.run.tags.get(config.NIGHTLY_CHECKS_TAG) == "true":
        # Nightly checks (D25): the same tests over a day, alone in this location (D30)
        wait_for_runs(
            context.instance, context.log, context.run_id, None, config.NIGHTLY_CHECKS_WAIT_SECONDS
        )
        invocation = dbt.cli(
            ["build", "--vars", json.dumps(config.NIGHTLY_TEST_VARS)], context=context
        )
        try:
            yield from invocation.stream()
        finally:
            stop_process(invocation.process)
        return

    passes = 0
    if silver_selected(context):
        silver_cap = silver_max_rows_per_run(config.DBT_SPARK_PROJECT_DIR)
        gold_cap = gold_max_hours_per_run(config.DBT_SPARK_PROJECT_DIR)
        backlog = _logged_backlog(dbt.cli(["run-operation", "measure_backlog"]))
        context.log.info(
            f"Backlog: {backlog.silver_rows} Bronze rows for Silver (cap {silver_cap} per "
            f"run), {backlog.gold_hours} hours for Gold (cap {gold_cap})"
        )
        while (
            backlog.silver_rows > silver_cap or backlog.gold_hours > gold_cap
        ) and passes < config.CATCHUP_MAX_PASSES:
            # Not streamed: Dagster accepts a single materialization per asset and run
            previous, backlog = backlog, _logged_backlog(dbt.cli(["run"]))
            passes += 1
            context.log.info(
                f"Catch-up pass {passes}: {backlog.silver_rows} Bronze rows, "
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
            if isinstance(event, Output) and is_silver(
                context.asset_key_for_output(event.output_name)
            ):
                event = event.with_metadata({**event.metadata, "catchup_passes": passes})
            yield event
    finally:
        # A dbt left running would keep a Thrift session busy while the next run starts
        stop_process(invocation.process)
