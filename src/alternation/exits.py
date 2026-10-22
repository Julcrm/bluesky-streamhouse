"""
Unexpected exits of the processes the supervisors run (engines and the Spark Thrift
server), in the benchmark database.

The supervisor restarts such a process inside the same container, so the collector, which
tells restarts apart by container id, never sees it: the JVM of the Thrift server ended
on OutOfMemoryError on 18 and 20/10 and its windows counted as clean. The windows flag
the windows holding an exit restarted (src/benchmark/windows.py).
"""

from datetime import datetime

SERVICE_EXITS_DDL = """CREATE TABLE IF NOT EXISTS service_exits (
    ts          TIMESTAMPTZ NOT NULL,
    service     TEXT NOT NULL,
    branch      TEXT NOT NULL,
    exit_code   INTEGER,
    PRIMARY KEY (ts, service)
)"""


def record_exit(conn, ts: datetime, service: str, branch: str, exit_code: int | None) -> None:
    """One exit (the connection is in autocommit, see calendar.connect_benchmark)."""
    with conn.cursor() as cur:
        cur.execute(SERVICE_EXITS_DDL)
        cur.execute(
            "INSERT INTO service_exits VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (ts, service, branch, exit_code),
        )


def exits_between(cur, start: datetime, end: datetime) -> dict[str, list[datetime]]:
    """{service: exit times} in [start, end)."""
    cur.execute(
        "SELECT service, ts FROM service_exits WHERE ts >= %s AND ts < %s ORDER BY ts",
        (start, end),
    )
    found: dict[str, list[datetime]] = {}
    for service, ts in cur.fetchall():
        found.setdefault(service, []).append(ts)
    return found
