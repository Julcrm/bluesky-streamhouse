"""
Storage of the two branches, once a night (added on 2026-10-22).

- Bucket usage per prefix (H5b): what each branch keeps on disk at its peak, copies
  awaiting expiration included (Iceberg keeps its compacted copies until expire_snapshots,
  DuckLake its files until cleanup), before the nightly maintenances remove them.
- Table usage (H5): live rows, bytes and files of each table after the maintenances, read
  from the catalogs' metadata without any engine: DuckLake's tables in Postgres (live
  files, minus deleted rows, plus the rows still inlined in Postgres), Iceberg's current
  snapshot summary through Lakekeeper's REST API.

Prefixes: `ducklake/<catalog>` (DuckDB branch), `iceberg` (Spark branch; Lakekeeper names
table folders by uuid, the namespace is not in the path), and the controlled test's
`bench/` and `bench_*` objects, kept apart. Sizes are the object sizes listed by Garage,
not the space on its disk (Garage compresses and deduplicates blocks).
"""

from collections import defaultdict
from dataclasses import astuple, dataclass
from datetime import datetime
from typing import Any

from src import config

DDL = """CREATE TABLE IF NOT EXISTS bucket_samples (
    ts          TIMESTAMPTZ NOT NULL,
    prefix      TEXT NOT NULL,
    branch      TEXT,
    bytes       BIGINT NOT NULL,
    objects     BIGINT NOT NULL,
    PRIMARY KEY (ts, prefix)
)"""


def prefix_of(key: str) -> str:
    """Prefix an object counts under: two levels for DuckLake (one per catalog), one
    otherwise. `key` is relative to the bucket."""
    parts = key.split("/")
    if parts[0] == "ducklake" and len(parts) > 2:
        return "/".join(parts[:2])
    return parts[0]


def usage(objects: dict[str, int]) -> dict[str, tuple[int, int]]:
    """{prefix: (bytes, objects)} from {key: size}."""
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for key, size in objects.items():
        entry = totals[prefix_of(key)]
        entry[0] += size
        entry[1] += 1
    return {prefix: (b, n) for prefix, (b, n) in totals.items()}


def branch_of(prefix: str) -> str | None:
    return config.BUCKET_USAGE_BRANCHES.get(prefix.split("/")[0])


def list_bucket(fs, bucket: str) -> dict[str, int]:
    """{key relative to the bucket: size} of every object."""
    found = fs.find(bucket, detail=True)
    return {
        name.removeprefix(f"{bucket}/"): int(info.get("size") or 0)
        for name, info in found.items()
        if info.get("type") != "directory"
    }


def record(conn, ts: datetime, figures: dict[str, tuple[int, int]]) -> int:
    """One row per prefix; rows written."""
    with conn.cursor() as cur:
        cur.execute(DDL)
        for prefix, (size, count) in sorted(figures.items()):
            cur.execute(
                "INSERT INTO bucket_samples VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (ts, prefix, branch_of(prefix), size, count),
            )
    return len(figures)


# --- Table usage (H5) -------------------------------------------------------------

TABLE_DDL = """CREATE TABLE IF NOT EXISTS table_samples (
    ts            TIMESTAMPTZ NOT NULL,
    branch        TEXT NOT NULL,
    namespace     TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    rows          BIGINT NOT NULL,
    bytes         BIGINT NOT NULL,
    files         BIGINT NOT NULL,
    inlined_rows  BIGINT NOT NULL,
    PRIMARY KEY (ts, branch, namespace, table_name)
)"""


@dataclass(frozen=True)
class TableUsage:
    branch: str
    namespace: str
    table_name: str
    rows: int
    bytes: int
    files: int
    # DuckLake only: rows still in Postgres (data inlining, D15), counted in `rows`, not in
    # `bytes` (their size is Postgres's)
    inlined_rows: int = 0


def ducklake_tables(cur, metadata_schema: str) -> list[TableUsage]:
    """Live tables of one DuckLake catalog (one Postgres schema of metadata)."""
    m = f'"{metadata_schema}"'
    cur.execute(
        f"""
        SELECT t.table_id, s.schema_name, t.table_name,
               (SELECT coalesce(sum(f.record_count), 0) FROM {m}.ducklake_data_file AS f
                WHERE f.table_id = t.table_id AND f.end_snapshot IS NULL),
               (SELECT coalesce(sum(f.file_size_bytes), 0) FROM {m}.ducklake_data_file AS f
                WHERE f.table_id = t.table_id AND f.end_snapshot IS NULL),
               (SELECT count(*) FROM {m}.ducklake_data_file AS f
                WHERE f.table_id = t.table_id AND f.end_snapshot IS NULL),
               (SELECT coalesce(sum(d.delete_count), 0) FROM {m}.ducklake_delete_file AS d
                WHERE d.table_id = t.table_id AND d.end_snapshot IS NULL)
        FROM {m}.ducklake_table AS t
        JOIN {m}.ducklake_schema AS s ON s.schema_id = t.schema_id
        WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL
        ORDER BY s.schema_name, t.table_name
        """
    )
    tables = cur.fetchall()
    cur.execute(f"SELECT table_id, table_name FROM {m}.ducklake_inlined_data_tables")
    inlined_tables: dict[int, list[str]] = defaultdict(list)
    for table_id, name in cur.fetchall():
        inlined_tables[table_id].append(name)
    usage = []
    for table_id, schema, name, rows, size, files, deleted in tables:
        inlined = 0
        for inlined_table in inlined_tables.get(table_id, []):
            cur.execute(f'SELECT count(*) FROM {m}."{inlined_table}" WHERE end_snapshot IS NULL')
            inlined += cur.fetchone()[0]
        usage.append(
            TableUsage(
                "duckdb",
                schema,
                name,
                int(rows) - int(deleted) + inlined,
                int(size),
                files,
                inlined,
            )
        )
    return usage


def iceberg_usage(namespace: str, table: str, metadata: dict) -> TableUsage:
    """One Iceberg table from its metadata: the current snapshot's summary totals (rows of
    the data files minus the deleted ones; bytes of data and delete files)."""
    current = metadata.get("current-snapshot-id")
    snapshot = next(
        (s for s in metadata.get("snapshots", []) if s.get("snapshot-id") == current), None
    )
    summary = (snapshot or {}).get("summary", {})

    def total(key: str) -> int:
        return int(summary.get(key, 0) or 0)

    rows = (
        total("total-records") - total("total-position-deletes") - total("total-equality-deletes")
    )
    return TableUsage(
        "spark",
        namespace,
        table,
        rows,
        total("total-files-size"),
        total("total-data-files") + total("total-delete-files"),
    )


def iceberg_tables(url: str, session: Any, namespaces: tuple[str, ...]) -> list[TableUsage]:
    from src.resources import iceberg_rest

    return [
        iceberg_usage(namespace, table, iceberg_rest.table_metadata(url, session, namespace, table))
        for namespace in namespaces
        for table in iceberg_rest.list_tables(url, session, namespace)
    ]


def record_tables(conn, ts: datetime, tables: list[TableUsage]) -> int:
    with conn.cursor() as cur:
        cur.execute(TABLE_DDL)
        for table in tables:
            cur.execute(
                "INSERT INTO table_samples VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                (ts, *astuple(table)),
            )
    return len(tables)
