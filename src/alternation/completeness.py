"""
Completeness of a finished benchmark day (decision D28), the same contract in both
branches: every Kafka offset of the day's bounds must be in the branch's Bronze, at
least once (duplicates are expected and removed in Silver).

Engine-free: the query is plain SQL that DuckDB and Spark SQL
both run, one scan of Bronze for the three partitions.
"""

from src.alternation import calendar as cal


def offsets_query(table: str, day: cal.CalendarDay) -> str:
    """Distinct offsets of the day found in `table`, by partition. The day must be
    closed; offsets are integers from the calendar, never user input."""
    if day.end_offsets is None:
        raise cal.CalendarError(f"Day {day.day} has no end offsets yet")
    ranges = " OR ".join(
        f"(kafka_partition = {int(p)} AND kafka_offset >= {int(day.start_offsets.get(p, end))} "
        f"AND kafka_offset < {int(end)})"
        for p, end in sorted(day.end_offsets.items())
    )
    return (
        f"SELECT kafka_partition, count(DISTINCT kafka_offset) AS found FROM {table} "
        f"WHERE {ranges} GROUP BY kafka_partition"
    )


def missing_offsets(day: cal.CalendarDay, found: dict[int, int]) -> dict[int, int]:
    """Offsets of the day missing from Bronze, by partition (0 everywhere when complete)."""
    return {
        p: (end - day.start_offsets.get(p, end)) - found.get(p, 0)
        for p, end in sorted(day.end_offsets.items())
    }


def completeness_metadata(day: cal.CalendarDay, missing: dict[int, int]) -> dict:
    """Dagster metadata of a completeness check, the same in both branches."""
    return {
        "day": day.day.isoformat(),
        "branch": day.effective_branch,
        "messages": sum(end - day.start_offsets.get(p, end) for p, end in day.end_offsets.items()),
        "missing": sum(missing.values()),
        "missing_by_partition": {str(p): n for p, n in missing.items()},
    }
