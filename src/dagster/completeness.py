"""
Completeness check of a finished day (decision D28), shared by both code locations:
each asset runs `offsets_query` on its own Bronze (DuckDB for B, Spark for A) and hands
the counts here. No engine import, so branch A's location does not load branch B's.
"""

from datetime import date

from dagster import AssetExecutionContext, Config, Failure, MaterializeResult, RunRequest

from src.alternation import calendar as cal
from src.alternation.completeness import completeness_metadata, missing_offsets


class CompletenessDay(Config):
    """The finished day to check (ISO date)."""

    day: str


def closed_day(value: str) -> cal.CalendarDay:
    """The day's calendar row; fails the run if it is not closed."""
    conn = cal.connect_benchmark()
    try:
        day = cal.CalendarStore(conn).get(date.fromisoformat(value))
    finally:
        conn.close()
    if day is None or day.end_offsets is None:
        raise Failure(f"Day {value} is not closed", allow_retries=False)
    return day


def completeness_result(
    context: AssetExecutionContext, day: cal.CalendarDay, found: dict[int, int]
) -> MaterializeResult:
    """Materialization when every offset is there; a failed run (alert) otherwise."""
    missing = missing_offsets(day, found)
    metadata = completeness_metadata(day, missing)
    if any(missing.values()):
        raise Failure(
            f"Day {day.day} ({day.effective_branch}): {metadata['missing']} offsets of the day "
            f"missing from Bronze ({missing})",
            metadata=metadata,
            allow_retries=False,
        )
    context.log.info(f"Day {day.day}: all {metadata['messages']} offsets are in Bronze")
    return MaterializeResult(metadata=metadata)


def completeness_request(op_name: str, branch: str, day: date) -> RunRequest:
    """One completeness run per branch and day (the run key makes it once)."""
    return RunRequest(
        run_key=f"completeness:{branch}:{day.isoformat()}",
        run_config={"ops": {op_name: {"config": {"day": day.isoformat()}}}},
    )
