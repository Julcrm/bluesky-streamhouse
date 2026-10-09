"""
Frozen sample of the controlled test (decision D35): one run at the configured date,
copying an evening slice of `raw_events` into a topic kept forever. Served by the DuckDB
branch's code server, which already reaches Redpanda (alternation bounds); the sample
belongs to neither branch.
"""

from datetime import datetime

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    MaterializeResult,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
)

from src import config
from src.benchmark import sample

GROUP = "benchmark"


@asset(
    # Asset folder of the code location serving it (one folder per engine), like the
    # DuckDB branch's benchmark windows
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_DUCKDB, GROUP],
    group_name=GROUP,
    description="Frozen sample of the controlled test: ~20 min of raw_events copied once into "
    "a topic kept forever (D35).",
)
def benchmark_sample(context: AssetExecutionContext) -> MaterializeResult:
    """Copy the configured slice; fails if the sample topic already exists."""
    start = datetime.fromisoformat(config.BENCH_SAMPLE_START)
    figures = sample.freeze_sample(start, config.BENCH_SAMPLE_MINUTES)
    context.log.info(f"Sample {config.BENCH_SAMPLE_TOPIC}: {figures}")
    return MaterializeResult(metadata=figures)


sample_job = define_asset_job(
    name="bluesky_benchmark_sample",
    selection=[benchmark_sample],
    description="Frozen sample of the controlled test, copied once (D35).",
)


@schedule(
    job=sample_job,
    cron_schedule=config.BENCH_SAMPLE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def sample_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Once, at the configured date; never again once the sample exists."""
    try:
        if sample.sample_exists():
            return SkipReason(f"Sample {config.BENCH_SAMPLE_TOPIC} already frozen")
    except Exception as e:  # noqa: BLE001 - Redpanda unreachable: the run would fail too
        return SkipReason(f"Redpanda unreachable: {e}")
    return RunRequest()
