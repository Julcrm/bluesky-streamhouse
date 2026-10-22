"""
Bucket usage per prefix, once a night (H5b): served by the DuckDB branch's code server,
which holds the S3 key; it measures both branches alike.
"""

from datetime import UTC, datetime

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    MaterializeResult,
    RunRequest,
    ScheduleEvaluationContext,
    asset,
    define_asset_job,
    schedule,
)

from src import config
from src.alternation import calendar as cal
from src.benchmark import storage

GROUP = "benchmark"


@asset(
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_DUCKDB, GROUP],
    group_name=GROUP,
    description="Bucket usage per prefix (DuckLake catalogs, Iceberg, controlled test) at "
    "the nightly peak, before the maintenances (H5b).",
)
def bucket_usage(context: AssetExecutionContext) -> MaterializeResult:
    import s3fs

    fs = s3fs.S3FileSystem(
        key=config.S3_ACCESS_KEY_ID,
        secret=config.S3_SECRET_ACCESS_KEY,
        endpoint_url=config.S3_ENDPOINT_URL,
    )
    figures = storage.usage(storage.list_bucket(fs, config.BUCKET))
    conn = cal.connect_benchmark()
    try:
        storage.record(conn, datetime.now(UTC).replace(microsecond=0), figures)
    finally:
        conn.close()
    context.log.info(f"Bucket usage: {figures}")
    return MaterializeResult(
        metadata={f"{prefix} GB": round(b / 1e9, 2) for prefix, (b, _) in figures.items()}
    )


bucket_usage_job = define_asset_job(
    name="bluesky_bucket_usage",
    selection=[bucket_usage],
    description="Bucket usage per prefix, once a night (H5b).",
)


@schedule(
    job=bucket_usage_job,
    cron_schedule=config.BUCKET_USAGE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def bucket_usage_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    """Every night, after the bench nights and before the maintenances."""
    return RunRequest()
