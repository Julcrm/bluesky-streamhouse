"""
Storage of both branches, once a night: bucket usage per prefix (H5b, 01:50, at the peak)
and table usage (H5, 04:00, after the maintenances). Served by the DuckDB branch's code
server, which holds the S3 key and reaches both catalogs; it measures both branches alike.
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


@asset(
    key_prefix=[config.DAGSTER_ASSET_PREFIX, config.DAGSTER_ENGINE_DUCKDB, GROUP],
    group_name=GROUP,
    description="Live rows, bytes and files of every table of both branches after the "
    "nightly maintenances, from the catalogs' metadata (H5).",
)
def table_usage(context: AssetExecutionContext) -> MaterializeResult:
    import psycopg2

    from src.benchmark.reset import ICEBERG_NAMESPACES
    from src.resources import iceberg_rest

    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.DUCKLAKE_CATALOG_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        connect_timeout=10,
        application_name="table_usage",
    )
    try:
        with conn.cursor() as cur:
            tables = [
                *storage.ducklake_tables(cur, config.DUCKLAKE_BRONZE_METADATA_SCHEMA),
                *storage.ducklake_tables(cur, config.DUCKLAKE_TRANSFORM_METADATA_SCHEMA),
            ]
    finally:
        conn.close()
    url, session = iceberg_rest.catalog()
    tables += storage.iceberg_tables(url, session, ICEBERG_NAMESPACES)
    bench = cal.connect_benchmark()
    try:
        storage.record_tables(bench, datetime.now(UTC).replace(microsecond=0), tables)
    finally:
        bench.close()
    context.log.info(f"Table usage: {len(tables)} tables")
    return MaterializeResult(
        metadata={
            f"{t.branch} {t.namespace}.{t.table_name} bytes/row": round(t.bytes / t.rows, 1)
            for t in tables
            if t.rows
        }
    )


table_usage_job = define_asset_job(
    name="bluesky_table_usage",
    selection=[table_usage],
    description="Table usage of both branches, once a night after the maintenances (H5).",
)


@schedule(
    job=table_usage_job,
    cron_schedule=config.TABLE_USAGE_CRON,
    execution_timezone=config.DAGSTER_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def table_usage_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    """Every night, after the maintenances and the nightly checks."""
    return RunRequest()
