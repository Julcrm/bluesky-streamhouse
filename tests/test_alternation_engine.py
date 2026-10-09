"""Tests for src.alternation.engine and src.alternation.supervisor."""

import sys
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from src.alternation import calendar as cal
from src.alternation.engine import EngineDay
from src.alternation.supervisor import ServiceSupervisor, Supervisor, main
from tests.test_alternation_calendar import START, store  # noqa: F401 (fixture)

NOW = datetime(2026, 10, 8, 5, 0, tzinfo=UTC)


def _engine_day(store, clock=lambda: 0.0, **kwargs) -> EngineDay:  # noqa: F811
    return EngineDay(START, store=lambda: store, poll_seconds=30, clock=clock, **kwargs)


def _open(store) -> None:  # noqa: F811
    store.create_day(START, {0: 100, 1: 200})
    store.open_day(START, NOW)


# --- Engine ---------------------------------------------------------------------


def test_first_launch_seeks_the_start_then_a_restart_does_not(store) -> None:  # noqa: F811
    """Only the day's first launch moves the read position; it records the start."""
    _open(store)
    seeks = []
    _engine_day(store).prepare(seeks.append)
    assert seeks == [{0: 100, 1: 200}]
    assert store.get(START).started_at is not None
    _engine_day(store).prepare(seeks.append)
    assert seeks == [{0: 100, 1: 200}]


def test_end_offsets_filter_once_known(store) -> None:  # noqa: F811
    """No end before 19:00: everything passes; then messages at or past the end drop."""
    _open(store)
    now = [0.0]
    # Before 19:00 of START: past it, the engine would wait for the end offsets
    engine = _engine_day(store, clock=lambda: now[0], now=lambda: NOW)
    engine.prepare(lambda _: None)
    assert engine.in_bounds(0, 10_000)
    store.close_day(START, {0: 150, 1: 260}, NOW)
    assert engine.in_bounds(0, 10_000)  # not re-read before the poll interval
    now[0] = 31.0
    assert engine.in_bounds(0, 149)
    assert not engine.in_bounds(0, 150)
    assert not engine.in_bounds(1, 300)


def test_after_19_waits_for_the_end_offsets(store) -> None:  # noqa: F811
    """Past 19:00 without end offsets, the message waits until Dagster closes the day,
    then is judged on the exact end (nothing of the next day slips through)."""
    _open(store)
    after_close = cal.at(START, "19:00") + timedelta(seconds=1)
    waits = []

    def sleep(_seconds: float) -> None:
        waits.append(_seconds)
        if len(waits) == 3:  # Dagster closes the day while the engine waits
            store.close_day(START, {0: 150, 1: 260}, NOW)

    engine = EngineDay(
        START, store=lambda: store, poll_seconds=30, now=lambda: after_close, sleep=sleep
    )
    engine.prepare(lambda _: None)
    assert not engine.in_bounds(0, 150)
    assert len(waits) == 3
    assert engine.in_bounds(1, 259)


def test_caught_up_recorded_once_under_the_lag(store) -> None:  # noqa: F811
    _open(store)
    engine = _engine_day(store)
    engine.prepare(lambda _: None)
    engine.record_commit(datetime.now(UTC) - timedelta(hours=3))
    assert store.get(START).caught_up_at is None
    engine.record_commit(datetime.now(UTC) - timedelta(seconds=2))
    first = store.get(START).caught_up_at
    assert first is not None
    engine.record_commit(datetime.now(UTC))
    assert store.get(START).caught_up_at == first


def test_done_once_every_partition_reached_its_end(store) -> None:  # noqa: F811
    _open(store)
    now = [0.0]
    engine = _engine_day(store, clock=lambda: now[0], now=lambda: NOW)
    engine.prepare(lambda _: None)
    assert not engine.check_complete({0: 500, 1: 500})  # no end yet
    store.close_day(START, {0: 150, 1: 260}, NOW)
    now[0] = 31.0
    assert not engine.check_complete({0: 150, 1: 259})
    assert engine.check_complete({0: 151, 1: 260})
    assert store.get(START).status == cal.DONE
    assert store.get(START).late_at is None
    assert store.active_for("duckdb") is None


def test_a_stop_after_19_30_carries_the_late_mark(store) -> None:  # noqa: F811
    """Done during the night: complete, kept in the benchmark, marked late at 19:30."""
    _open(store)
    store.close_day(START, {0: 150, 1: 260}, NOW)
    store.mark_stopped(START, datetime(2026, 10, 8, 23, 0, tzinfo=UTC), cal.DONE)
    day = store.get(START)
    assert (day.status, day.late_at) == (cal.DONE, cal.late_time(START))


# --- Supervisor -----------------------------------------------------------------


class _Calendar:
    """In-memory calendar for the supervisor: one day for one branch, or none."""

    def __init__(self, day: cal.CalendarDay | None = None) -> None:
        self.day = day
        self.owner_branch: str | None = None
        self.stopped: list[tuple[date, str]] = []
        self.late: list[date] = []
        self.unreachable = False

    def active_for(self, branch: str) -> cal.CalendarDay | None:
        if self.unreachable:
            raise ConnectionError("postgres down")
        if self.day is None or self.day.effective_branch != branch:
            return None
        return self.day

    def owner(self) -> str | None:
        if self.unreachable:
            raise ConnectionError("postgres down")
        return self.owner_branch

    def mark_stopped(self, day: date, now: datetime, status: str, note: str | None = None):
        self.stopped.append((day, status))

    def mark_late(self, day: date) -> bool:
        self.late.append(day)
        return True


def _day(status: str = cal.OPEN, branch: str = "duckdb") -> cal.CalendarDay:
    return cal.CalendarDay(
        day=START,
        branch=branch,
        forced_branch=None,
        start_offsets={0: 0},
        end_offsets=None if status == cal.OPEN else {0: 10},
        offsets_source="watermark",
        status=status,
    )


SLEEPER = [sys.executable, "-c", "import time; time.sleep(60)"]


@pytest.fixture
def supervisor():
    calendar = _Calendar()
    clock = {"now": datetime(2026, 10, 8, 12, 0, tzinfo=UTC), "mono": 1000.0}
    sup = Supervisor(
        "duckdb",
        SLEEPER,
        store=lambda: calendar,
        now=lambda: clock["now"],
        restart_delay=30,
        monotonic=lambda: clock["mono"],
    )
    yield sup, calendar, clock
    sup.stop()


def test_starts_only_on_spark_day_of_its_branch(supervisor) -> None:
    sup, calendar, _ = supervisor
    sup.tick()
    assert not sup.running()
    calendar.day = _day(branch="spark")
    sup.tick()
    assert not sup.running()
    calendar.day = _day()
    sup.tick()
    assert sup.running() and sup.day == START


def test_stops_when_the_day_is_no_longer_active(supervisor) -> None:
    """The engine marked the day done (or Dagster changed it): the engine is stopped."""
    sup, calendar, _ = supervisor
    calendar.day = _day()
    sup.tick()
    calendar.day = None
    sup.tick()
    assert not sup.running()
    assert calendar.stopped == []  # the engine recorded its own end


def test_late_at_19_30_then_hard_stop_at_06_30(supervisor) -> None:
    """Still running at 19:30 (17:30 UTC in October): marked late, keeps going; still
    running at 06:30 the next morning (04:30 UTC): stopped, incomplete."""
    sup, calendar, clock = supervisor
    calendar.day = _day()
    sup.tick()
    calendar.day = _day(cal.CLOSING)
    clock["now"] = datetime(2026, 10, 8, 17, 30, tzinfo=UTC)
    sup.tick()
    assert sup.running()
    assert calendar.late == [START]
    calendar.day = replace(calendar.day, late_at=clock["now"])
    clock["now"] = datetime(2026, 10, 9, 4, 29, tzinfo=UTC)
    sup.tick()
    assert sup.running() and calendar.late == [START]
    clock["now"] = datetime(2026, 10, 9, 4, 30, tzinfo=UTC)
    sup.tick()
    assert not sup.running()
    assert calendar.stopped == [(START, cal.INCOMPLETE)]


def test_hard_stop_also_ends_spark_day_never_closed(supervisor) -> None:
    """No close at all (the engine waits for its end offsets): stopped at 06:30."""
    sup, calendar, clock = supervisor
    calendar.day = _day()
    sup.tick()
    clock["now"] = datetime(2026, 10, 9, 4, 30, tzinfo=UTC)
    sup.tick()
    assert not sup.running()
    assert calendar.stopped == [(START, cal.INCOMPLETE)]


def test_crashed_engine_restarts_after_the_delay(supervisor) -> None:
    sup, calendar, clock = supervisor
    calendar.day = _day()
    sup.tick()
    sup.process.kill()
    sup.process.wait()
    sup.tick()
    assert not sup.running()
    clock["mono"] += 31
    sup.tick()
    assert sup.running()


def test_unreachable_calendar_keeps_the_current_state(supervisor) -> None:
    """Postgres down: a running engine keeps running, a stopped one is not started."""
    sup, calendar, _ = supervisor
    calendar.unreachable = True
    sup.tick()
    assert not sup.running()
    calendar.unreachable = False
    calendar.day = _day()
    sup.tick()
    calendar.unreachable = True
    sup.tick()
    assert sup.running()


# --- Service mode ---------------------------------------------------------------


@pytest.fixture
def service():
    calendar = _Calendar()
    clock = {"mono": 1000.0}
    sup = ServiceSupervisor(
        "spark", SLEEPER, store=lambda: calendar, restart_delay=30, monotonic=lambda: clock["mono"]
    )
    yield sup, calendar, clock
    sup.stop()


def test_service_runs_while_its_branch_owns_the_latest_day(service) -> None:
    """The Spark branch's Thrift server: up from a Spark day's opening, down from the next
    DuckDB day's opening."""
    sup, calendar, _ = service
    sup.tick()
    assert not sup.running()
    calendar.owner_branch = "spark"
    sup.tick()
    assert sup.running()
    calendar.owner_branch = "duckdb"
    sup.tick()
    assert not sup.running()
    assert calendar.stopped == [] and calendar.late == []  # no day of its own


def test_crashed_service_restarts_after_the_delay(service) -> None:
    sup, calendar, clock = service
    calendar.owner_branch = "spark"
    sup.tick()
    sup.process.kill()
    sup.process.wait()
    sup.tick()
    assert not sup.running()
    clock["mono"] += 31
    sup.tick()
    assert sup.running()


def test_unreachable_calendar_keeps_the_service_state(service) -> None:
    sup, calendar, _ = service
    calendar.owner_branch = "spark"
    sup.tick()
    calendar.unreachable = True
    sup.tick()
    assert sup.running()


def test_cli_rejects_a_command_without_separator() -> None:
    with pytest.raises(SystemExit):
        main(["spark", "--service", "python"])


def test_supervisor_loop_writes_its_heartbeat(monkeypatch, tmp_path) -> None:
    """The container healthcheck reads this file: written at each loop, whatever the
    calendar says (an engine stopped on the other branch's days is healthy)."""
    import os
    import signal as signals

    from src import config
    from src.alternation.supervisor import Supervisor

    beat = tmp_path / "heartbeat"
    monkeypatch.setattr(config, "HEARTBEAT_FILE", str(beat))
    supervisor = Supervisor("duckdb", SLEEPER, store=lambda: None)
    # One tick, then the stop signal the loop handles (as a container stop)
    monkeypatch.setattr(supervisor, "tick", lambda: os.kill(os.getpid(), signals.SIGTERM))
    handlers = {sig: signals.getsignal(sig) for sig in (signals.SIGTERM, signals.SIGINT)}
    try:
        supervisor.run(poll_seconds=0)
    finally:
        for sig, handler in handlers.items():
            signals.signal(sig, handler)
    assert beat.exists()
