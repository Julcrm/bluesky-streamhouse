"""
Bucket usage per prefix, once a night (H5b, added on 2026-10-22): what each branch keeps
on disk at its peak, copies awaiting expiration included (Iceberg keeps its compacted
copies until expire_snapshots, DuckLake its files until cleanup), before the nightly
maintenances remove them.

Prefixes: `ducklake/<catalog>` (DuckDB branch), `iceberg` (Spark branch; Lakekeeper names
table folders by uuid, the namespace is not in the path), and the controlled test's
`bench/` and `bench_*` objects, kept apart. Sizes are the object sizes listed by Garage,
not the space on its disk (Garage compresses and deduplicates blocks).
"""

from collections import defaultdict
from datetime import datetime

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
