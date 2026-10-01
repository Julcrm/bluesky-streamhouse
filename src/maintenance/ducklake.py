"""
DuckLake maintenance of branch B (decision D21), run nightly by Dagster: retention
DELETE (D14), then the native CHECKPOINT of each catalog (one per writer, D22), which
flushes inlined rows, merges small files, expires snapshots and removes old and orphan
files in one command (Iceberg needs 3-4 procedures for the same: a benchmark result).

Kept free of Dagster imports, like the resources: the assets only call these functions.
"""

import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

import duckdb
import s3fs

from src import config
from src.resources.ducklake import DuckLakeSettings, connect

# Column a table's retention is measured on, by order of preference
TIME_COLUMNS = ("event_time", "minute", "hour")
PROGRESS_TABLE = "meta.silver_progress"


@dataclass(frozen=True)
class CatalogStats:
    """State of one catalog, read from its metadata tables before and after maintenance."""

    active_files: int
    active_bytes: int
    snapshots: int
    files_pending_deletion: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def maintenance_connection(settings: DuckLakeSettings) -> duckdb.DuckDBPyConnection:
    """Writable connection to one catalog, with the code server's DuckDB budget."""
    conn = connect(settings)
    conn.execute(f"SET memory_limit = '{config.MAINTENANCE_DUCKDB_MEMORY_LIMIT}'")
    conn.execute(f"SET threads = {int(config.MAINTENANCE_DUCKDB_THREADS)}")
    conn.execute(f"SET temp_directory = '{config.MAINTENANCE_DUCKDB_TEMP_DIRECTORY}'")
    return conn


def retention_cutoff(days: int, now: datetime | None = None) -> datetime:
    """Start of the oldest UTC day kept: a whole number of days, so a DELETE removes
    whole day files instead of writing deletion files."""
    now = now or datetime.now(UTC)
    midnight = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight - timedelta(days=days)


def catalog_stats(conn: duckdb.DuckDBPyConnection, settings: DuckLakeSettings) -> CatalogStats:
    """Active files and bytes, snapshots and files waiting for deletion of one catalog."""
    meta = f"__ducklake_metadata_{settings.alias}.{settings.metadata_schema}"
    files, size = conn.execute(
        f"SELECT count(*), coalesce(sum(file_size_bytes), 0) FROM {meta}.ducklake_data_file "
        "WHERE end_snapshot IS NULL"
    ).fetchone()
    snapshots = conn.execute(f"SELECT count(*) FROM {meta}.ducklake_snapshot").fetchone()[0]
    pending = conn.execute(
        f"SELECT count(*) FROM {meta}.ducklake_files_scheduled_for_deletion"
    ).fetchone()[0]
    return CatalogStats(int(files), int(size), int(snapshots), int(pending))


def list_tables(conn: duckdb.DuckDBPyConnection, alias: str, schema: str) -> list[str]:
    """Tables of one schema of an attached catalog."""
    return [
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? ORDER BY 1",
            [alias, schema],
        ).fetchall()
    ]


def time_column(conn: duckdb.DuckDBPyConnection, alias: str, schema: str, table: str) -> str:
    """Time column a table's retention is measured on (event_time, minute or hour)."""
    columns = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
            [alias, schema, table],
        ).fetchall()
    }
    for column in TIME_COLUMNS:
        if column in columns:
            return column
    raise ValueError(f"{alias}.{schema}.{table} has none of the time columns {TIME_COLUMNS}")


def delete_expired_rows(
    conn: duckdb.DuckDBPyConnection,
    alias: str,
    retention_days: dict[str, int],
    now: datetime | None = None,
) -> dict[str, int]:
    """DELETE the rows older than each schema's retention; rows deleted per table.

    Tables are found in the catalog, not listed here: a new Silver or Gold model gets
    its schema's retention without a code change.
    """
    deleted = {}
    for schema, days in retention_days.items():
        cutoff = retention_cutoff(days, now)
        for table in list_tables(conn, alias, schema):
            column = time_column(conn, alias, schema, table)
            deleted[f"{schema}.{table}"] = conn.execute(
                f"DELETE FROM {alias}.{schema}.{table} WHERE {column} < ?", [cutoff]
            ).fetchone()[0]
    return deleted


def trim_silver_progress(conn: duckdb.DuckDBPyConnection, now: datetime | None = None) -> int:
    """Delete old Silver read positions, always keeping each model's last done one."""
    alias = config.DUCKLAKE_TRANSFORM_ALIAS
    if "silver_progress" not in list_tables(conn, alias, "meta"):
        return 0
    table = f"{alias}.{PROGRESS_TABLE}"
    cutoff = (now or datetime.now(UTC)) - timedelta(days=config.SILVER_PROGRESS_RETENTION_DAYS)
    return conn.execute(
        f"DELETE FROM {table} AS p WHERE recorded_at < ? AND NOT (done AND bronze_snapshot_id "
        f"= (SELECT max(bronze_snapshot_id) FROM {table} WHERE model = p.model AND done))",
        [cutoff],
    ).fetchone()[0]


def set_options(conn: duckdb.DuckDBPyConnection, alias: str) -> None:
    """Persist the options CHECKPOINT applies (idempotent)."""
    for option, value in (
        ("expire_older_than", config.DUCKLAKE_EXPIRE_OLDER_THAN),
        ("delete_older_than", config.DUCKLAKE_DELETE_OLDER_THAN),
        ("target_file_size", config.DUCKLAKE_TARGET_FILE_SIZE),
    ):
        conn.execute(f"CALL {alias}.set_option('{option}', '{value}')")


def checkpoint(
    conn: duckdb.DuckDBPyConnection,
    alias: str,
    retries: int = config.CHECKPOINT_RETRIES,
    delay_seconds: float = config.CHECKPOINT_RETRY_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """CHECKPOINT one catalog, retried on a commit conflict; returns the attempts made.

    Quix commits to Bronze every 5 s, so the CHECKPOINT can lose the race for the next
    snapshot id. Backoff grows with each attempt.
    """
    for attempt in range(1, retries + 2):
        try:
            conn.execute(f"CHECKPOINT {alias}")
            return attempt
        except duckdb.TransactionException:
            if attempt > retries:
                raise
            sleep(delay_seconds * attempt)
    raise AssertionError("unreachable")


def stale_positions(
    conn: duckdb.DuckDBPyConnection,
    alias: str,
    positions: dict[str, int | None],
    max_age_hours: int = config.READ_POSITION_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, str]:
    """Readers whose oldest unread snapshot of `alias` is older than the time travel
    window: expiring snapshots would remove what they have still to read (D16, D21).

    A reader with nothing pending is never stale, however old its position: on a day
    when the branch is off (D1) nothing is written and nothing is lost.
    """
    limit = (now or datetime.now(UTC)) - timedelta(hours=max_age_hours)
    stale = {}
    for reader, position in positions.items():
        if position is None:  # never ran: its first run reads from the oldest snapshot
            continue
        oldest_pending = conn.execute(
            f"SELECT min(snapshot_time) FROM ducklake_snapshots('{alias}') WHERE snapshot_id > ?",
            [position],
        ).fetchone()[0]
        if oldest_pending is not None and oldest_pending < limit:
            stale[reader] = oldest_pending.isoformat()
    return stale


def bucket_bytes(bucket: str = config.BUCKET) -> int:
    """Size of the whole bucket by listing it: orphan files and files waiting for
    deletion count too, unlike the catalog's own figures."""
    fs = s3fs.S3FileSystem(
        key=config.S3_ACCESS_KEY_ID,
        secret=config.S3_SECRET_ACCESS_KEY,
        endpoint_url=config.S3_ENDPOINT_URL,
        client_kwargs={"region_name": config.S3_REGION},
    )
    return int(fs.du(bucket))


def catalog_database_bytes(conn: duckdb.DuckDBPyConnection) -> int:
    """Size of the Postgres database holding both DuckLake catalogs (D12)."""
    conn.execute("ATTACH IF NOT EXISTS '' AS catalog_db (TYPE postgres, READ_ONLY)")
    return int(
        conn.execute(
            "SELECT * FROM postgres_query('catalog_db', "
            "'SELECT pg_database_size(current_database())')"
        ).fetchone()[0]
    )
