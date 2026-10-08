"""Tests for src.alternation.control: 07:00 and 19:00 decisions with fake Kafka lookups,
on the calendar store of the local Postgres."""

from datetime import UTC, datetime, timedelta

import pytest

from src import config
from src.alternation import calendar as cal
from src.alternation import control
from tests.test_alternation_calendar import START, store  # noqa: F401 (fixture)

OPEN_AT = datetime(2026, 10, 8, 5, 0, tzinfo=UTC)  # 07:00 Paris
CLOSE_AT = datetime(2026, 10, 8, 17, 0, 3, tzinfo=UTC)  # 19:00:03 Paris


@pytest.fixture(autouse=True)
def start_date(monkeypatch) -> None:
    monkeypatch.setattr(config, "ALTERNATION_START_DATE", START)


def _offsets_at(moment: datetime) -> cal.Offsets:
    """Fake lookup by time: one message per minute since midnight UTC, per partition."""
    minutes = int(moment.timestamp() // 60) % 10_000
    return {0: minutes, 1: minutes, 2: minutes}


def test_eve_of_the_start_creates_the_first_day_on_exact_offsets(store) -> None:  # noqa: F811
    """19:00 the day before the start: no day to close, the first one starts there."""
    eve = CLOSE_AT - timedelta(days=1)
    assert control.close_day(store, eve, watermarks=lambda: {0: 7, 1: 8, 2: 9}) is None
    first = store.get(START)
    assert (first.branch, first.start_offsets, first.offsets_source) == (
        "duckdb",
        {0: 7, 1: 8, 2: 9},
        "watermark",
    )


def test_open_before_the_start_does_nothing(store) -> None:  # noqa: F811
    assert control.open_day(store, OPEN_AT - timedelta(days=1), offsets_at=_offsets_at) is None


def test_open_without_a_row_falls_back_on_timestamps(store) -> None:  # noqa: F811
    """First day without the eve's close: start offsets at 19:00 the day before, marked."""
    day = control.open_day(store, OPEN_AT, offsets_at=_offsets_at)
    assert (day.day, day.status, day.offsets_source) == (START, cal.OPEN, "timestamp")
    assert day.start_offsets == _offsets_at(cal.day_window(START)[0])


def test_close_on_time_reads_the_high_watermarks(store) -> None:  # noqa: F811
    store.create_day(START, {0: 0, 1: 0, 2: 0})
    control.open_day(store, OPEN_AT)
    closed = control.close_day(store, CLOSE_AT, watermarks=lambda: {0: 10, 1: 20, 2: 30})
    assert (closed.day, closed.end_offsets, closed.offsets_source) == (
        START,
        {0: 10, 1: 20, 2: 30},
        "watermark",
    )
    assert control.day_messages(closed) == 60
    assert store.get(START + timedelta(days=1)).start_offsets == {0: 10, 1: 20, 2: 30}


def test_late_close_uses_the_offsets_at_19(store) -> None:  # noqa: F811
    """A manual close at 23:00 must not move the bound to 23:00."""
    store.create_day(START, {0: 0, 1: 0, 2: 0})

    def no_watermarks() -> cal.Offsets:
        raise AssertionError("a late close must not read the current watermarks")

    closed = control.close_day(
        store,
        CLOSE_AT + timedelta(hours=4),
        watermarks=no_watermarks,
        offsets_at=_offsets_at,
    )
    assert closed.end_offsets == _offsets_at(cal.at(START, "19:00"))
    assert closed.offsets_source == "timestamp"


def test_calendar_asset_end_to_end(store, monkeypatch) -> None:  # noqa: F811
    """The Dagster asset on the local Redpanda: the eve's close creates the first day on
    the real high watermarks, and each schedule's run config is valid for the job."""
    from dagster import materialize, validate_run_config

    from src.dagster.alternation import (
        branch_calendar,
        calendar_job,
        close_day_schedule,
        open_day_schedule,
    )
    from src.dagster.definitions import defs
    from src.resources import redpanda

    today = datetime.now(UTC).astimezone(cal.TIMEZONE).date()
    monkeypatch.setattr(config, "ALTERNATION_START_DATE", today + timedelta(days=1))
    monkeypatch.setattr(config, "BENCHMARK_DB", "bluesky_benchmark_test")
    op = branch_calendar.op.name
    result = materialize(
        [branch_calendar],
        run_config={"ops": {op: {"config": {"action": "close", "day": today.isoformat()}}}},
    )
    assert result.success
    first = store.get(today + timedelta(days=1))
    watermarks = redpanda.high_watermarks()
    assert first.branch == "duckdb"
    assert all(first.start_offsets[p] <= watermarks[p] for p in watermarks)

    job = defs.get_job_def(calendar_job.name)
    for schedule in (open_day_schedule, close_day_schedule):
        request = schedule(None)
        validate_run_config(job, request.run_config)
