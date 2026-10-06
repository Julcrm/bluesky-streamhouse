"""Tests for src.alternation.monitor (failure alerts, Silver/Gold window) and
src.alternation.completeness."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb

from src.alternation import calendar as cal
from src.alternation import monitor
from src.alternation.completeness import missing_offsets

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


def test_incomplete_and_missed_guard() -> None:
    incomplete = replace(DAY, status=cal.INCOMPLETE, started_at=OPEN_UTC)
    assert _kinds([incomplete], CLOSE_UTC) == {"incomplete"}
    closing = replace(DAY, status=cal.CLOSING, started_at=OPEN_UTC, end_offsets={0: 1})
    assert _kinds([closing], CLOSE_UTC + timedelta(minutes=44)) == set()
    assert _kinds([closing], CLOSE_UTC + timedelta(minutes=45)) == {"guard_missed"}


def test_no_alert_for_a_branch_not_deployed() -> None:
    """A's days while B runs alone: no engine is expected."""
    a_day = replace(DAY, branch="A")
    assert _kinds([a_day], CLOSE_UTC + timedelta(hours=1)) == set()
    assert _kinds([a_day], CLOSE_UTC + timedelta(hours=1), ("A", "B")) == {
        "not_started",
        "not_closed",
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


def test_missing_offsets_counts_distinct_offsets_in_bounds() -> None:
    """Duplicates and offsets outside the day do not hide a hole."""
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS bronze")
    conn.execute(
        "CREATE TABLE bronze.main.bronze_events AS SELECT * FROM (VALUES "
        "(0, 100), (0, 101), (0, 101), (0, 102), (0, 99), (0, 103), "  # partition 0 complete
        "(1, 200), (1, 202)"  # partition 1 lacks 201
        ") AS t(kafka_partition, kafka_offset)"
    )
    day = replace(DAY, end_offsets={0: 103, 1: 203})
    assert missing_offsets(conn, day) == {0: 0, 1: 1}
