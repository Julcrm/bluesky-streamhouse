"""
Iceberg maintenance of branch A, the counterpart of DuckLake's single CHECKPOINT
(decision D21): Iceberg needs one procedure per task, run by Spark. That difference is
a benchmark result (operational complexity).

Order, nightly: retention DELETE (whole day partitions, D14), rewrite_data_files (merge
the ~17 000 small files a day of 5-second commits), rewrite_manifests, expire_snapshots
(24 h of time travel, like DuckLake), remove_orphan_files (failed writes). Every file
deletion goes through Lakekeeper's remote signing (no S3 key in Spark).

Kept free of Dagster imports: the assets only call these functions.
"""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession

from src import config

PROCEDURES = f"{config.SPARK_CATALOG}.system"


@dataclass(frozen=True)
class TableStats:
    """State of one Iceberg table, read from its metadata tables."""

    data_files: int
    data_bytes: int
    snapshots: int
    manifests: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def _timestamp(moment: datetime) -> str:
    """TIMESTAMP literal (CALL arguments must be constants, not expressions)."""
    return f"TIMESTAMP '{moment.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')}'"


def table_stats(spark: SparkSession, table: str) -> TableStats:
    files, size = spark.sql(
        f"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM {table}.files"
    ).collect()[0]
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").collect()[0][0]
    manifests = spark.sql(f"SELECT count(*) FROM {table}.manifests").collect()[0][0]
    return TableStats(int(files), int(size), int(snapshots), int(manifests))


def retention_cutoff(days: int, now: datetime | None = None) -> datetime:
    """Start of the oldest UTC day kept, as for DuckLake: whole day partitions go."""
    now = (now or datetime.now(UTC)).astimezone(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)


def delete_expired_rows(
    spark: SparkSession, table: str, column: str, days: int, now: datetime | None = None
) -> int:
    """DELETE the rows older than the retention; returns how many (read from the file
    metadata first: the DELETE of whole partitions only rewrites metadata)."""
    cutoff = retention_cutoff(days, now)
    # Partitioned by days(column): the partition value is the day, so the files of the
    # days before the cutoff hold exactly the rows the DELETE removes
    rows = spark.sql(
        f"SELECT coalesce(sum(record_count), 0) FROM {table}.files "
        f"WHERE partition.{column}_day < DATE '{cutoff.date().isoformat()}'"
    ).collect()[0][0]
    spark.sql(f"DELETE FROM {table} WHERE {column} < {_timestamp(cutoff)}")
    return int(rows)


def _call(spark: SparkSession, procedure: str, arguments: str) -> dict[str, Any]:
    """CALL one Iceberg procedure; its result row as a dict (counts for the metadata)."""
    rows = spark.sql(f"CALL {PROCEDURES}.{procedure}({arguments})").collect()
    return rows[0].asDict() if rows else {}


def rewrite_data_files(spark: SparkSession, table: str) -> dict[str, Any]:
    """Merge small files up to the target size. Partial progress commits each file group
    on its own: a conflict with the streaming appends only redoes one group."""
    options = {
        "target-file-size-bytes": str(config.ICEBERG_TARGET_FILE_SIZE_BYTES),
        "partial-progress.enabled": "true",
        # One group at a time: bounded memory in the 1.5 GB code server
        "max-concurrent-file-group-rewrites": "1",
    }
    pairs = ", ".join(f"'{k}', '{v}'" for k, v in options.items())
    return _call(spark, "rewrite_data_files", f"table => '{table}', options => map({pairs})")


def rewrite_manifests(spark: SparkSession, table: str) -> dict[str, Any]:
    """Merge the manifests, one per commit otherwise."""
    return _call(spark, "rewrite_manifests", f"table => '{table}'")


def expire_snapshots(
    spark: SparkSession, table: str, now: datetime | None = None
) -> dict[str, Any]:
    """Expire snapshots past the time travel window, and delete the files only they used."""
    older_than = (now or datetime.now(UTC)) - timedelta(
        hours=config.ICEBERG_SNAPSHOT_RETENTION_HOURS
    )
    return _call(
        spark,
        "expire_snapshots",
        f"table => '{table}', older_than => {_timestamp(older_than)}, retain_last => 1",
    )


def remove_orphan_files(spark: SparkSession, table: str, now: datetime | None = None) -> int:
    """Delete files of the table's location that no metadata references (failed writes).
    Listed through Iceberg's S3FileIO (prefix listing): there is no Hadoop S3 filesystem."""
    older_than = (now or datetime.now(UTC)) - timedelta(hours=config.ICEBERG_ORPHAN_MIN_AGE_HOURS)
    rows = spark.sql(
        f"CALL {PROCEDURES}.remove_orphan_files(table => '{table}', "
        f"older_than => {_timestamp(older_than)}, prefix_listing => true)"
    ).collect()
    return len(rows)
