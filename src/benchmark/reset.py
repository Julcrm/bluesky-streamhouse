"""
Reset before the measurement period (plan of the freeze, option B decided by Julien on
2026-10-16): both lakehouses and the measures start empty, so the two branches begin the
period from the same state (H8b measures what accumulates from there).

    python -m src.benchmark.reset duckdb    # in bluesky-duckdb: DuckLake catalogs + its runs
    python -m src.benchmark.reset spark     # in bluesky-spark: Iceberg tables + its runs
    python -m src.benchmark.reset measures  # in either: samples, windows, bench runs

Without `--confirm <part>` it only prints what it would delete (dry run). Destructive:
run by Julien on the VPS, never by an automated job.

- DuckDB branch: the two production DuckLake catalogs are dropped whole, metadata and
  files, as the controlled test drops its bench ones (src.benchmark.cleanup): their
  Postgres metadata schemas and their data under `ducklake/` in the bucket. The next
  ATTACH (Quix, dbt) recreates them empty; the Bronze table and the dbt models follow.
- Spark branch: every table of the production namespaces is dropped through
  Lakekeeper's REST API (no Thrift server needed: it only runs on Spark days), and
  Lakekeeper deletes the files itself (delete profile `hard`). The namespaces stay; the
  streaming job recreates Bronze, dbt the rest. Streaming checkpoints are per day, so a
  new day never reads an old one: left alone.
- Measures: the benchmark tables are emptied (TRUNCATE), not dropped: the collector
  keeps writing to them.
- Dagster: the finished runs of this code location (housekeeping's purge with a cutoff
  of now), never velib's: the instance is shared.

Never touched, refused by name: the frozen sample (a Redpanda topic, not named here),
`raw_events`, the alternation calendar (`branch_calendar`), the bench catalogs and
namespaces (the controlled test cleans its own), anything outside bluesky.
"""

import argparse
import sys
from datetime import UTC, datetime
from typing import Any

from src import config

PARTS = ("duckdb", "spark", "measures")

DUCKLAKE_SCHEMAS = (
    config.DUCKLAKE_BRONZE_METADATA_SCHEMA,
    config.DUCKLAKE_TRANSFORM_METADATA_SCHEMA,
)
DUCKLAKE_PATHS = (config.DUCKLAKE_BRONZE_DATA_PATH, config.DUCKLAKE_TRANSFORM_DATA_PATH)
ICEBERG_NAMESPACES = ("bronze", "silver", "gold", "meta")
MEASURE_TABLES = (
    "container_samples",
    "host_samples",
    "catalog_samples",
    "service_windows",
    "branch_windows",
    "bench_runs",
)
# Names a reset must never touch, whatever a caller passes
PROTECTED = ("branch_calendar", "raw_events", config.BENCH_SAMPLE_TOPIC, "default")


def _guard(names: tuple[str, ...]) -> None:
    """Refuse a protected name, an object of the controlled test (bench_*, bench/ in the
    bucket; its results table bench_runs is a measure) or anything of velib."""
    for name in names:
        leaf = name.rstrip("/").split("/")[-1]
        if name in PROTECTED or leaf in PROTECTED:
            raise ValueError(f"Protected from the reset, refused: {name}")
        if (leaf.startswith("bench_") and leaf != "bench_runs") or "/bench/" in name:
            raise ValueError(f"Object of the controlled test, refused: {name}")
        if "velib" in name:
            raise ValueError(f"Not a bluesky object, refused: {name}")


# --- DuckDB branch ---------------------------------------------------------------


def _ducklake_conn():
    import psycopg2

    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.DUCKLAKE_CATALOG_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        connect_timeout=10,
        application_name="benchmark_reset",
    )


def _s3():
    import s3fs

    return s3fs.S3FileSystem(
        key=config.S3_ACCESS_KEY_ID,
        secret=config.S3_SECRET_ACCESS_KEY,
        endpoint_url=config.S3_ENDPOINT_URL,
    )


def ducklake_plan(
    schemas: tuple[str, ...] = DUCKLAKE_SCHEMAS, paths: tuple[str, ...] = DUCKLAKE_PATHS
) -> dict[str, int]:
    """What the DuckDB reset deletes: tables per metadata schema, files per data path."""
    _guard(schemas + paths)
    plan = {}
    conn = _ducklake_conn()
    try:
        with conn.cursor() as cur:
            for schema in schemas:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s",
                    (schema,),
                )
                plan[f"schema {schema} (metadata tables)"] = cur.fetchone()[0]
    finally:
        conn.close()
    fs = _s3()
    for path in paths:
        prefix = path.removeprefix("s3://").rstrip("/")
        plan[f"files under {path}"] = len(fs.find(prefix)) if fs.exists(prefix) else 0
    return plan


def reset_ducklake(
    schemas: tuple[str, ...] = DUCKLAKE_SCHEMAS, paths: tuple[str, ...] = DUCKLAKE_PATHS
) -> dict[str, int]:
    """Drop the DuckLake catalogs: metadata schemas, then data files. Files deleted per path."""
    _guard(schemas + paths)
    conn = _ducklake_conn()
    try:
        with conn, conn.cursor() as cur:
            for schema in schemas:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        conn.close()
    fs = _s3()
    deleted = {}
    for path in paths:
        prefix = path.removeprefix("s3://").rstrip("/")
        files = fs.find(prefix) if fs.exists(prefix) else []
        if files:
            fs.rm(files)
        deleted[path] = len(files)
    return deleted


# --- Spark branch (Lakekeeper REST) ------------------------------------------------


def _catalog() -> tuple[str, Any]:
    """(base URL of the warehouse's REST paths, HTTP session)."""
    import requests

    session = requests.Session()
    base = config.ICEBERG_CATALOG_URI.rstrip("/")
    response = session.get(
        f"{base}/v1/config", params={"warehouse": config.ICEBERG_WAREHOUSE}, timeout=30
    )
    response.raise_for_status()
    body = response.json()
    # The warehouse prefix: in `defaults` for Lakekeeper, `overrides` win when set
    prefix = {**body.get("defaults", {}), **body.get("overrides", {})}["prefix"]
    return f"{base}/v1/{prefix}", session


def iceberg_tables(namespaces: tuple[str, ...] = ICEBERG_NAMESPACES) -> list[tuple[str, str]]:
    """(namespace, table) of every table in the namespaces (missing namespaces skipped)."""
    _guard(namespaces)
    url, session = _catalog()
    tables = []
    for namespace in namespaces:
        token = None
        while True:  # paged (rest-page-size)
            params = {"pageToken": token} if token else {}
            response = session.get(
                f"{url}/namespaces/{namespace}/tables", params=params, timeout=30
            )
            if response.status_code == 404:
                break
            response.raise_for_status()
            body = response.json()
            tables += [(namespace, ident["name"]) for ident in body.get("identifiers", [])]
            token = body.get("next-page-token")
            if not token:
                break
    return tables


def reset_iceberg(namespaces: tuple[str, ...] = ICEBERG_NAMESPACES) -> list[str]:
    """Drop every table of the namespaces; Lakekeeper deletes their files. Tables dropped."""
    tables = iceberg_tables(namespaces)
    url, session = _catalog()
    dropped = []
    for namespace, table in tables:
        response = session.delete(f"{url}/namespaces/{namespace}/tables/{table}", timeout=60)
        if response.status_code != 404:
            response.raise_for_status()
        dropped.append(f"{namespace}.{table}")
    return dropped


# --- Measures --------------------------------------------------------------------


def measures_plan(tables: tuple[str, ...] = MEASURE_TABLES) -> dict[str, int]:
    """Rows per benchmark table (missing tables skipped)."""
    from src.alternation import calendar as cal

    _guard(tables)
    conn = cal.connect_benchmark()
    try:
        with conn.cursor() as cur:
            plan = {}
            for table in tables:
                cur.execute("SELECT to_regclass(%s)", (table,))
                if cur.fetchone()[0] is None:
                    continue
                cur.execute(f'SELECT count(*) FROM "{table}"')
                plan[table] = cur.fetchone()[0]
            return plan
    finally:
        conn.close()


def reset_measures(tables: tuple[str, ...] = MEASURE_TABLES) -> list[str]:
    """Empty the benchmark tables (TRUNCATE, the collector keeps writing). Tables emptied."""
    from src.alternation import calendar as cal

    present = list(measures_plan(tables))
    if not present:
        return []
    conn = cal.connect_benchmark()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE " + ", ".join(f'"{t}"' for t in present))
    finally:
        conn.close()
    return present


# --- Dagster -----------------------------------------------------------------------


def dagster_runs(purge: bool) -> int:
    """Finished runs of this code location (count, or deleted when `purge`)."""
    from dagster import DagsterInstance, RunsFilter

    from src.dagster.housekeeping import FINISHED_RUN_STATUSES, purge_finished_runs
    from src.dagster.runs import in_location

    instance = DagsterInstance.get()
    now = datetime.now(UTC)
    if purge:
        return purge_finished_runs(instance, now)
    records = instance.get_run_records(
        RunsFilter(statuses=FINISHED_RUN_STATUSES, created_before=now)
    )
    return sum(1 for r in records if in_location(r.dagster_run))


# --- CLI -----------------------------------------------------------------------------


def run(part: str, confirm: str | None, with_dagster: bool = True) -> dict:
    """Dry run (plan) unless `confirm` equals `part`. Returns what was planned or done."""
    if part not in PARTS:
        raise ValueError(f"Unknown part {part!r}, expected one of {PARTS}")
    location = {"duckdb": "bluesky_duckdb", "spark": "bluesky_spark"}.get(part)
    if with_dagster and location and config.DAGSTER_CODE_LOCATION != location:
        raise SystemExit(f"Run the {part} part in the {location} code server (its Dagster runs)")
    execute = confirm == part
    result: dict = {"part": part, "executed": execute}
    if part == "duckdb":
        result["ducklake"] = reset_ducklake() if execute else ducklake_plan()
    elif part == "spark":
        result["iceberg"] = (
            reset_iceberg() if execute else [f"{n}.{t}" for n, t in iceberg_tables()]
        )
    else:
        result["measures"] = reset_measures() if execute else measures_plan()
    if with_dagster and part != "measures":
        result["dagster_runs"] = dagster_runs(purge=execute)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0].strip())
    parser.add_argument("part", choices=PARTS)
    parser.add_argument(
        "--confirm", help="repeat the part's name to really delete (dry run otherwise)"
    )
    parser.add_argument(
        "--no-dagster", action="store_true", help="keep this location's Dagster runs"
    )
    args = parser.parse_args(argv)
    result = run(args.part, args.confirm, with_dagster=not args.no_dagster)
    mode = (
        "DELETED" if result["executed"] else "DRY RUN (add --confirm " + args.part + " to delete)"
    )
    print(f"Benchmark reset, part {args.part}: {mode}")
    for key, value in result.items():
        if key in ("part", "executed"):
            continue
        if isinstance(value, dict):
            for name, count in value.items():
                print(f"  {key}: {name}: {count}")
        elif isinstance(value, list):
            print(f"  {key}: {len(value)}")
            for name in value:
                print(f"    {name}")
        else:
            print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
