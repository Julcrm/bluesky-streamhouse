"""
Completeness of a finished day in branch B's Bronze (decision D28): every Kafka offset
of the day's bounds must be in Bronze, at least once (duplicates are expected and
removed in Silver). Same contract as the audits of phase 2, now bounded by the day.
"""

import duckdb

from src import config
from src.alternation import calendar as cal
from src.processing.bronze import BRONZE_TABLE


def missing_offsets(
    conn: duckdb.DuckDBPyConnection,
    day: cal.CalendarDay,
    alias: str = config.DUCKLAKE_BRONZE_ALIAS,
) -> dict[int, int]:
    """Offsets of the day's bounds missing from Bronze, by partition (0 everywhere when
    complete). The day must be closed."""
    if day.end_offsets is None:
        raise cal.CalendarError(f"Day {day.day} has no end offsets yet")
    missing = {}
    for partition, end in sorted(day.end_offsets.items()):
        start = day.start_offsets.get(partition, end)
        found = conn.execute(
            f"SELECT count(DISTINCT kafka_offset) FROM {alias}.main.{BRONZE_TABLE} "
            "WHERE kafka_partition = ? AND kafka_offset >= ? AND kafka_offset < ?",
            [partition, start, end],
        ).fetchone()[0]
        missing[partition] = (end - start) - found
    return missing
