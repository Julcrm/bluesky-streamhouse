"""
Parity of the benchmark contract (phase 5): fed with the same Kafka offsets, the Spark branch
(Spark, Iceberg, dbt-spark) and the DuckDB branch (Quix, DuckLake, dbt-duckdb) must produce the
same Silver and Gold rows. Each branch's tables are dumped to Parquet, then compared row
by row, both ways, with DuckDB.

Not compared: the lineage columns, engine-specific by nature (Iceberg snapshot ids are
random, DuckLake's are counters; processed_at is each engine's write time). List
columns whose order the contract does not fix are sorted.
"""

from dataclasses import dataclass
from pathlib import Path

import duckdb

SILVER_TABLES = (
    "silver_posts",
    "silver_likes",
    "silver_reposts",
    "silver_follows",
    "silver_deletes",
)
GOLD_TABLES = (
    "gold_activity_minute",
    "gold_langs_hour",
    "gold_hashtags_hour",
    "gold_active_users_hour",
    "gold_engagement_hour",
)
TABLES = SILVER_TABLES + GOLD_TABLES
LINEAGE_COLUMNS = ("bronze_snapshot_id", "silver_snapshot_id", "processed_at")
# Built with list_distinct (DuckDB branch), array_distinct (Spark branch): same set, any order
UNORDERED_LISTS = ("hashtags", "mention_dids")


@dataclass(frozen=True)
class TableParity:
    """Rows of one table in each branch, and those found in one branch only."""

    table: str
    rows_a: int
    rows_b: int
    only_in_a: int
    only_in_b: int

    @property
    def equal(self) -> bool:
        return self.only_in_a == 0 and self.only_in_b == 0 and self.rows_a == self.rows_b


def _columns(conn: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    return [
        row[0]
        for row in conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        if row[0] not in LINEAGE_COLUMNS
    ]


def _normalized(path: str, columns: list[str]) -> str:
    exprs = [f"list_sort({c}) AS {c}" if c in UNORDERED_LISTS else c for c in columns]
    return f"SELECT {', '.join(exprs)} FROM read_parquet('{path}')"


def compare_table(
    conn: duckdb.DuckDBPyConnection, table: str, dir_a: Path, dir_b: Path
) -> TableParity:
    """Compare one table's dumps (`<dir>/<table>/*.parquet`) as multisets of rows."""
    path_a, path_b = f"{dir_a}/{table}/*.parquet", f"{dir_b}/{table}/*.parquet"
    columns = sorted(set(_columns(conn, path_a)) & set(_columns(conn, path_b)))
    a, b = _normalized(path_a, columns), _normalized(path_b, columns)
    rows_a = conn.execute(f"SELECT count(*) FROM ({a})").fetchone()[0]
    rows_b = conn.execute(f"SELECT count(*) FROM ({b})").fetchone()[0]
    only_a = conn.execute(f"SELECT count(*) FROM ({a} EXCEPT ALL {b})").fetchone()[0]
    only_b = conn.execute(f"SELECT count(*) FROM ({b} EXCEPT ALL {a})").fetchone()[0]
    return TableParity(table, rows_a, rows_b, only_a, only_b)


def compare(dir_a: Path, dir_b: Path, tables: tuple[str, ...] = TABLES) -> list[TableParity]:
    """Every table of the contract; the columns must match too (checked by the caller)."""
    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'UTC'")
    try:
        return [compare_table(conn, t, dir_a, dir_b) for t in tables]
    finally:
        conn.close()


def column_differences(dir_a: Path, dir_b: Path, tables: tuple[str, ...] = TABLES) -> dict:
    """Columns present in one branch only, by table (the contract wants none)."""
    conn = duckdb.connect()
    try:
        diffs = {}
        for t in tables:
            a = set(_columns(conn, f"{dir_a}/{t}/*.parquet"))
            b = set(_columns(conn, f"{dir_b}/{t}/*.parquet"))
            if a != b:
                diffs[t] = {"only_in_a": sorted(a - b), "only_in_b": sorted(b - a)}
        return diffs
    finally:
        conn.close()
