"""
Iceberg maintenance of the Spark branch, the counterpart of DuckLake's single CHECKPOINT
(decision D21): Iceberg needs one procedure per task. That difference is a benchmark
result (operational complexity).

Order, nightly, table by table: retention DELETE (whole partitions, D14),
rewrite_data_files (merge the small files of 5-second commits and 15-minute runs),
rewrite_manifests, expire_snapshots (24 h of time travel, like DuckLake),
remove_orphan_files (failed writes). Every file deletion goes through Lakekeeper's
remote signing (no S3 key in Spark).

Every statement runs on the Spark Thrift server (D6 revised, 5d), as SQL: the code
server of the Spark branch holds no JVM. Functions take the statement runner as their
first argument
(`src.resources.thrift.records` in production, a fake in the tests).

Kept free of Dagster imports: the assets only call these functions.
"""

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from src import config
from src.processing.bronze import BRONZE_TABLE

# One SQL statement in, its rows as {column: value} out
Sql = Callable[[str], list[dict[str, Any]]]

PROCEDURES = f"{config.SPARK_CATALOG}.system"
# Read positions of Silver (on Bronze) and Gold (on Silver), written by dbt
SILVER_PROGRESS = f"{config.SPARK_CATALOG}.meta.silver_progress"
GOLD_PROGRESS = f"{config.SPARK_CATALOG}.meta.gold_progress"


@dataclass(frozen=True)
class TableStats:
    """State of one Iceberg table, read from its metadata tables."""

    data_files: int
    data_bytes: int
    snapshots: int
    manifests: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)

    def __add__(self, other: "TableStats") -> "TableStats":
        return TableStats(
            self.data_files + other.data_files,
            self.data_bytes + other.data_bytes,
            self.snapshots + other.snapshots,
            self.manifests + other.manifests,
        )


EMPTY_STATS = TableStats(0, 0, 0, 0)


def _timestamp(moment: datetime) -> str:
    """TIMESTAMP literal (CALL arguments must be constants, not expressions)."""
    return f"TIMESTAMP '{moment.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')}'"


def _scalar(sql: Sql, statement: str) -> Any:
    """First column of the first row of one statement."""
    rows = sql(statement)
    return next(iter(rows[0].values())) if rows else None


def table_stats(sql: Sql, table: str) -> TableStats:
    files = sql(
        f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes "
        f"FROM {table}.files"
    )[0]
    snapshots = _scalar(sql, f"SELECT count(*) FROM {table}.snapshots")
    manifests = _scalar(sql, f"SELECT count(*) FROM {table}.manifests")
    return TableStats(int(files["files"]), int(files["bytes"]), int(snapshots), int(manifests))


def list_tables(sql: Sql, namespace: str) -> list[str]:
    """Tables of one namespace, fully qualified, found in the catalog: a new Silver or
    Gold model gets its namespace's retention without a code change (as the DuckDB branch)."""
    rows = sql(f"SHOW TABLES IN {config.SPARK_CATALOG}.{namespace}")
    return sorted(f"{config.SPARK_CATALOG}.{namespace}.{row['tableName']}" for row in rows)


def time_column(sql: Sql, table: str) -> str:
    """Time column a table's retention is measured on (event_time, minute or hour)."""
    columns = {row["col_name"] for row in sql(f"DESCRIBE TABLE {table}")}
    for column in config.RETENTION_TIME_COLUMNS:
        if column in columns:
            return column
    raise ValueError(f"{table} has none of the time columns {config.RETENTION_TIME_COLUMNS}")


def retention_cutoff(days: int, now: datetime | None = None) -> datetime:
    """Start of the oldest UTC day kept, as for DuckLake: whole day partitions go."""
    now = (now or datetime.now(UTC)).astimezone(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)


def _rows_removed_since(sql: Sql, table: str, since: str | None) -> int:
    """Net rows removed by the delete and overwrite commits after `since`, from the
    snapshot summaries (no data read)."""
    after = f" AND committed_at > TIMESTAMP '{since}'" if since else ""
    removed = _scalar(
        sql,
        "SELECT coalesce(sum(CAST(coalesce(summary['deleted-records'], '0') AS BIGINT)"
        " - CAST(coalesce(summary['added-records'], '0') AS BIGINT)), 0) "
        f"FROM {table}.snapshots WHERE operation IN ('delete', 'overwrite'){after}",
    )
    return int(removed or 0)


def _last_commit(sql: Sql, table: str) -> str | None:
    return _scalar(sql, f"SELECT CAST(max(committed_at) AS STRING) FROM {table}.snapshots")


def delete_expired_rows(
    sql: Sql, table: str, column: str, days: int, now: datetime | None = None
) -> int:
    """DELETE the rows older than the retention; returns how many. Tables are
    partitioned by day or hour on `column` and the cutoff is a midnight, so the DELETE
    drops whole partitions (metadata only)."""
    since = _last_commit(sql, table)
    sql(f"DELETE FROM {table} WHERE {column} < {_timestamp(retention_cutoff(days, now))}")
    return _rows_removed_since(sql, table, since)


def trim_progress(sql: Sql, table: str, keys: tuple[str, ...], now: datetime | None = None) -> int:
    """Delete old read positions, always keeping the last done one of each reader
    (`keys`). Iceberg snapshot ids are random: the last one is the latest recorded."""
    cutoff = (now or datetime.now(UTC)) - timedelta(days=config.SILVER_PROGRESS_RETENTION_DAYS)
    columns = ", ".join(keys)
    since = _last_commit(sql, table)
    sql(
        f"DELETE FROM {table} WHERE recorded_at < {_timestamp(cutoff)} "
        f"AND ({columns}, recorded_at) NOT IN (SELECT {columns}, max(recorded_at) "
        f"FROM {table} WHERE done GROUP BY {columns})"
    )
    return _rows_removed_since(sql, table, since)


def read_positions(sql: Sql, existing: set[str]) -> dict[str, tuple[str, int]]:
    """Last done read position of each reader: {reader: (table read, snapshot id)}.
    Silver models read Bronze, Gold models read their Silver models. Readers that never
    ran have no position (they read the table from its first snapshot)."""
    bronze = f"{config.SPARK_CATALOG}.{config.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"
    positions: dict[str, tuple[str, int]] = {}
    if SILVER_PROGRESS in existing:
        for row in sql(
            "SELECT model, max_by(bronze_snapshot_id, recorded_at) AS position "
            f"FROM {SILVER_PROGRESS} WHERE done GROUP BY model"
        ):
            positions[row["model"]] = (bronze, int(row["position"]))
    if GOLD_PROGRESS in existing:
        for row in sql(
            "SELECT model, silver_model, max_by(silver_snapshot_id, recorded_at) AS position "
            f"FROM {GOLD_PROGRESS} WHERE done GROUP BY model, silver_model"
        ):
            silver = f"{config.SPARK_CATALOG}.silver.{row['silver_model']}"
            positions[f"{row['model']} <- {row['silver_model']}"] = (silver, int(row["position"]))
    return positions


def stale_readers(
    sql: Sql,
    positions: dict[str, tuple[str, int]],
    tables: set[str],
    now: datetime | None = None,
    max_age_hours: int = config.READ_POSITION_MAX_AGE_HOURS,
) -> dict[str, str]:
    """Readers of the maintained `tables` whose oldest unread snapshot is older than the
    time travel window, or whose position is already gone: expire_snapshots would make
    their next incremental read fail (guard D16, D21, as the DuckDB branch)."""
    limit = (now or datetime.now(UTC)) - timedelta(hours=max_age_hours)
    stale = {}
    for reader, (table, snapshot_id) in positions.items():
        if table not in tables:
            continue
        rows = sql(
            "SELECT CAST(min(committed_at) AS STRING) AS oldest, count(*) AS position_kept "
            f"FROM {table}.snapshots WHERE snapshot_id = {int(snapshot_id)}"
        )
        if not rows or not int(rows[0]["position_kept"]):
            stale[reader] = f"position {snapshot_id} already expired"
            continue
        oldest = _scalar(
            sql,
            f"SELECT CAST(min(committed_at) AS STRING) FROM {table}.snapshots "
            f"WHERE committed_at > TIMESTAMP '{rows[0]['oldest']}'",
        )
        if oldest is not None and _parse(oldest) < limit:
            stale[reader] = oldest
    return stale


def _parse(timestamp: str) -> datetime:
    """A Spark TIMESTAMP cast to string: the Thrift server's session time zone is UTC."""
    return datetime.fromisoformat(timestamp).replace(tzinfo=UTC)


def _call(sql: Sql, procedure: str, arguments: str) -> dict[str, Any]:
    """CALL one Iceberg procedure; its result row as a dict (counts for the metadata)."""
    rows = sql(f"CALL {PROCEDURES}.{procedure}({arguments})")
    return rows[0] if rows else {}


def rewrite_data_files(sql: Sql, table: str, where: str | None = None) -> dict[str, Any]:
    """Merge small files up to the target size. Partial progress commits each file group
    on its own: a conflict with a concurrent append only redoes one group. `where`
    restricts the rewrite to some partitions (the hourly compaction: today only)."""
    options = {
        "target-file-size-bytes": str(config.ICEBERG_TARGET_FILE_SIZE_BYTES),
        "partial-progress.enabled": "true",
        # One group at a time: bounded memory in the Thrift server's 768 MB heap (D31)
        "max-concurrent-file-group-rewrites": "1",
    }
    pairs = ", ".join(f"'{k}', '{v}'" for k, v in options.items())
    arguments = f"table => '{table}', options => map({pairs})"
    if where:
        arguments += f', where => "{where}"'  # double quotes: the predicate holds quotes
    return _call(sql, "rewrite_data_files", arguments)


def today_filter(column: str, now: datetime | None = None) -> str:
    """Predicate on the current UTC day: the partition the streaming job writes into."""
    start = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y-%m-%d")
    return f"{column} >= TIMESTAMP '{start} 00:00:00'"


def rewrite_manifests(sql: Sql, table: str) -> dict[str, Any]:
    """Merge the manifests, one per commit otherwise."""
    return _call(sql, "rewrite_manifests", f"table => '{table}'")


def expire_snapshots(sql: Sql, table: str, now: datetime | None = None) -> dict[str, Any]:
    """Expire snapshots past the time travel window, and delete the files only they used."""
    older_than = (now or datetime.now(UTC)) - timedelta(
        hours=config.ICEBERG_SNAPSHOT_RETENTION_HOURS
    )
    return _call(
        sql,
        "expire_snapshots",
        f"table => '{table}', older_than => {_timestamp(older_than)}, retain_last => 1",
    )


def remove_orphan_files(sql: Sql, table: str, now: datetime | None = None) -> int:
    """Delete files of the table's location that no metadata references (failed writes).
    Listed through Iceberg's S3FileIO (prefix listing): there is no Hadoop S3 filesystem."""
    older_than = (now or datetime.now(UTC)) - timedelta(hours=config.ICEBERG_ORPHAN_MIN_AGE_HOURS)
    rows = sql(
        f"CALL {PROCEDURES}.remove_orphan_files(table => '{table}', "
        f"older_than => {_timestamp(older_than)}, prefix_listing => true)"
    )
    return len(rows)
