"""
Silver/Gold passes of both branches, one row per finished run (H8a, added on 2026-10-22):
their duration through the day shows whether a branch slows down as its tables take
commits. Written by a run status sensor of each code location, so the history stays in
the benchmark database (the reset purges Dagster's runs, and the passes are measures).
"""

from datetime import datetime

DDL = """CREATE TABLE IF NOT EXISTS transform_passes (
    run_id      TEXT PRIMARY KEY,
    branch      TEXT NOT NULL,
    job         TEXT NOT NULL,
    status      TEXT NOT NULL,
    nightly     BOOLEAN NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ NOT NULL,
    seconds     DOUBLE PRECISION NOT NULL
)"""


def pass_row(
    run_id: str,
    branch: str,
    job: str,
    status: str,
    nightly: bool,
    started_at: datetime | None,
    ended_at: datetime | None,
) -> tuple | None:
    """The row of one pass, None when it never started (queued then canceled)."""
    if started_at is None or ended_at is None:
        return None
    return (
        run_id,
        branch,
        job,
        status,
        nightly,
        started_at,
        ended_at,
        (ended_at - started_at).total_seconds(),
    )


def record(conn, row: tuple) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            "INSERT INTO transform_passes VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            row,
        )
