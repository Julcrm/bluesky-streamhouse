"""
Frozen sample of the controlled test (decision D35): an evening slice of `raw_events`
copied into a topic kept forever, by one scheduled run on 2026-10-09 (the schedule was
removed on 2026-10-23, once served). The job stays: it shows how the sample was made and
refuses to run again while the topic exists. Served by the DuckDB
branch's code server, which already reaches Redpanda (alternation bounds); the sample
belongs to neither branch.
"""

from datetime import datetime

from dagster import AssetExecutionContext, MaterializeResult, asset, define_asset_job

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
