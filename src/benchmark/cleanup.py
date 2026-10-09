"""
Emptying the bench tables before each run of the controlled test (decision D36), so
every repetition starts from the same, empty state. Never touches a production table:
only the bench catalogs (DuckDB) and the `bench_` namespaces (Spark) are named here.

- DuckDB branch: the bench catalogs are dropped whole, metadata and files: their
  Postgres metadata schemas (bench_bronze, bench_transform) and their data under
  `bench/` in the bucket. The next ATTACH (Quix, dbt) recreates them empty. Dropping
  tables one by one would leave the catalog's snapshots behind, a different start state
  for each repetition.
- Spark branch: every table of the bench namespaces is dropped with PURGE (its data
  and metadata files), through the Thrift server.

Kept free of Dagster imports.
"""

import psycopg2
import s3fs

from src import config

DUCKLAKE_BENCH_SCHEMAS = (
    config.DUCKLAKE_BENCH_BRONZE_METADATA_SCHEMA,
    config.DUCKLAKE_BENCH_TRANSFORM_METADATA_SCHEMA,
)
DUCKLAKE_BENCH_PATHS = (
    config.DUCKLAKE_BENCH_BRONZE_DATA_PATH,
    config.DUCKLAKE_BENCH_TRANSFORM_DATA_PATH,
)
ICEBERG_BENCH_NAMESPACES = tuple(
    f"{config.ICEBERG_BENCH_PREFIX}{name}" for name in ("bronze", "silver", "gold", "meta")
)


def _bench_only(names: tuple[str, ...]) -> None:
    """Guard: refuse anything that is not a bench schema, namespace or path."""
    for name in names:
        if "bench" not in name:
            raise ValueError(f"Not a bench object, refused: {name}")


def drop_ducklake_bench(
    schemas: tuple[str, ...] = DUCKLAKE_BENCH_SCHEMAS,
    paths: tuple[str, ...] = DUCKLAKE_BENCH_PATHS,
) -> dict[str, int]:
    """Drop the bench DuckLake catalogs: metadata schemas, then data files. Returns the
    number of files deleted per path."""
    _bench_only(schemas + paths)
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.DUCKLAKE_CATALOG_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        connect_timeout=10,
        application_name="bench_cleanup",
    )
    try:
        with conn, conn.cursor() as cur:
            for schema in schemas:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        conn.close()
    fs = s3fs.S3FileSystem(
        key=config.S3_ACCESS_KEY_ID,
        secret=config.S3_SECRET_ACCESS_KEY,
        endpoint_url=config.S3_ENDPOINT_URL,
    )
    deleted = {}
    for path in paths:
        prefix = path.removeprefix("s3://").rstrip("/")
        files = fs.find(prefix) if fs.exists(prefix) else []
        if files:
            fs.rm(files)
        deleted[path] = len(files)
    return deleted


def drop_iceberg_bench(namespaces: tuple[str, ...] = ICEBERG_BENCH_NAMESPACES) -> list[str]:
    """Drop, with PURGE, every table of the bench namespaces; the tables dropped."""
    from src.resources import thrift

    _bench_only(namespaces)
    dropped = []
    for namespace in namespaces:
        if not thrift.records(f"SHOW NAMESPACES IN {config.SPARK_CATALOG} LIKE '{namespace}'"):
            continue
        for row in thrift.records(f"SHOW TABLES IN {config.SPARK_CATALOG}.{namespace}"):
            table = f"{config.SPARK_CATALOG}.{namespace}.{row['tableName']}"
            thrift.query(f"DROP TABLE IF EXISTS {table} PURGE")
            dropped.append(table)
    return dropped
