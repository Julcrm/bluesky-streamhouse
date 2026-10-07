"""Tests for src.alternation.monitor (failure alerts, Silver/Gold window) and
src.alternation.completeness."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb

from src.alternation import calendar as cal
from src.alternation import monitor
from src.alternation.completeness import (
    completeness_metadata,
    missing_offsets,
    offsets_query,
)

DAY = cal.CalendarDay(
    day=datetime(2026, 10, 8).date(),
    branch="B",
    forced_branch=None,
    start_offsets={0: 100, 1: 200},
    end_offsets=None,
    offsets_source="watermark",
    status=cal.OPEN,
)
# October: Paris is UTC+2
OPEN_UTC = datetime(2026, 10, 8, 5, 0, tzinfo=UTC)
CLOSE_UTC = datetime(2026, 10, 8, 17, 0, tzinfo=UTC)


def _kinds(days, now, deployed=("B",)) -> set[str]:
    return {a.key.split(":")[1] for a in monitor.alerts(days, now, deployed)}


def test_engine_not_started_15_min_after_the_opening() -> None:
    assert _kinds([DAY], OPEN_UTC + timedelta(minutes=14)) == set()
    assert _kinds([DAY], OPEN_UTC + timedelta(minutes=15)) == {"not_started"}
    started = replace(DAY, started_at=OPEN_UTC)
    assert _kinds([started], OPEN_UTC + timedelta(hours=1)) == set()


def test_day_not_closed_15_min_after_19() -> None:
    started = replace(DAY, started_at=OPEN_UTC)
    assert _kinds([started], CLOSE_UTC + timedelta(minutes=14)) == set()
    assert _kinds([started], CLOSE_UTC + timedelta(minutes=15)) == {"not_closed"}
    assert _kinds([started], CLOSE_UTC + timedelta(minutes=30)) == {"not_closed", "late"}


def test_late_at_19_30_incomplete_and_missed_stop() -> None:
    """Late from 19:30 while running; incomplete once stopped at 06:30; an engine still
    running 15 min after the hard stop means the supervisor did not act."""
    closing = replace(DAY, status=cal.CLOSING, started_at=OPEN_UTC, end_offsets={0: 1})
    assert _kinds([closing], CLOSE_UTC + timedelta(minutes=29)) == set()
    assert _kinds([closing], CLOSE_UTC + timedelta(minutes=30)) == {"late"}
    hard_stop = cal.hard_stop_time(DAY.day)
    assert _kinds([closing], hard_stop + timedelta(minutes=15)) == {"late", "stop_missed"}
    incomplete = replace(closing, status=cal.INCOMPLETE)
    assert _kinds([incomplete], hard_stop) == {"incomplete"}


def test_no_alert_for_a_branch_not_deployed() -> None:
    """A's days while B runs alone: no engine is expected."""
    a_day = replace(DAY, branch="A")
    assert _kinds([a_day], CLOSE_UTC + timedelta(hours=1)) == set()
    assert _kinds([a_day], CLOSE_UTC + timedelta(hours=1), ("A", "B")) == {
        "not_started",
        "not_closed",
        "late",
    }


def test_alert_keys_are_per_day_and_kind() -> None:
    (alert,) = monitor.alerts([replace(DAY, status=cal.INCOMPLETE)], CLOSE_UTC, ("B",))
    assert alert.key == "2026-10-08:incomplete"
    assert "2026-10-08" in alert.subject


def test_transform_runs_on_b_days_and_one_hour_after() -> None:
    assert monitor.transform_due([DAY], "B", OPEN_UTC) is not None
    assert monitor.transform_due([DAY], "A", OPEN_UTC) is None
    stopped = replace(DAY, status=cal.DONE, stopped_at=CLOSE_UTC)
    assert monitor.transform_due([stopped], "B", CLOSE_UTC + timedelta(minutes=59)) is not None
    assert monitor.transform_due([stopped], "B", CLOSE_UTC + timedelta(minutes=61)) is None
    assert monitor.transform_due([], "B", OPEN_UTC) is None


def test_completeness_query_counts_distinct_offsets_in_bounds() -> None:
    """One scan for every partition; duplicates and offsets outside the day do not hide
    a hole. Plain SQL: the same query runs in DuckDB (B) and Spark SQL (A)."""
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE bronze_events AS SELECT * FROM (VALUES "
        "(0, 100), (0, 101), (0, 101), (0, 102), (0, 99), (0, 103), "  # partition 0 complete
        "(1, 200), (1, 202)"  # partition 1 lacks 201, partition 2 has nothing
        ") AS t(kafka_partition, kafka_offset)"
    )
    day = replace(DAY, start_offsets={0: 100, 1: 200, 2: 300}, end_offsets={0: 103, 1: 203, 2: 301})
    found = dict(conn.execute(offsets_query("bronze_events", day)).fetchall())
    assert missing_offsets(day, found) == {0: 0, 1: 1, 2: 1}
    metadata = completeness_metadata(day, missing_offsets(day, found))
    assert (metadata["messages"], metadata["missing"]) == (7, 2)


def test_nightly_checks_only_the_night_after_a_day_of_the_branch() -> None:
    """03:00 on 10/09 (01:00 UTC): the day that ended at 19:00 on 10/08 decides (D30)."""
    night = datetime(2026, 10, 9, 1, 0, tzinfo=UTC)
    done = replace(DAY, status=cal.DONE, stopped_at=CLOSE_UTC)
    assert monitor.nightly_checks_due([done], "B", night) is not None
    assert monitor.nightly_checks_due([done], "A", night) is None
    # The night after, nothing of B in the window
    assert monitor.nightly_checks_due([done], "B", night + timedelta(days=1)) is None
    # Late and still finishing: tested; closing on time but not done yet: not
    closing = replace(DAY, status=cal.CLOSING)
    assert monitor.nightly_checks_due([closing], "B", night) is None
    late = replace(closing, late_at=CLOSE_UTC + timedelta(minutes=30))
    assert monitor.nightly_checks_due([late], "B", night) is not None
    # A skipped day of A (not deployed): no test for A
    skipped = replace(DAY, branch="A", status=cal.SKIPPED)
    assert monitor.nightly_checks_due([skipped], "A", night) is None
