"""Tests for src.alternation.calendar: rotation, Paris times, offsets, and the store on
the local Postgres (make up), in a throwaway database."""

from datetime import UTC, date, datetime, timedelta

import pytest

from src import config
from src.alternation import calendar as cal
from tests.test_resources_ducklake import _local_stack_up

START = date(2026, 10, 8)
TEST_DB = "bluesky_benchmark_test"


def test_rotation_starts_with_b_and_alternates() -> None:
    """B on the start date (deployed first, D28), then A, B, A..."""
    days = [START + timedelta(days=i) for i in range(4)]
    assert [cal.branch_for(d, START) for d in days] == ["B", "A", "B", "A"]
    with pytest.raises(cal.CalendarError):
        cal.branch_for(START - timedelta(days=1), START)


def test_paris_times_follow_daylight_saving() -> None:
    """19:00 Paris is 17:00 UTC in summer time, 18:00 UTC in winter time."""
    assert cal.at(date(2026, 10, 24), "19:00") == datetime(2026, 10, 24, 17, 0, tzinfo=UTC)
    assert cal.at(date(2026, 10, 26), "19:00") == datetime(2026, 10, 26, 18, 0, tzinfo=UTC)


def test_benchmark_day_switches_at_19_paris() -> None:
    """Day J is (19:00 J-1, 19:00 J]: 18:59 belongs to J, 19:00 to J+1."""
    day = date(2026, 10, 8)
    assert cal.benchmark_day(datetime(2026, 10, 8, 16, 59, tzinfo=UTC)) == day
    assert cal.benchmark_day(datetime(2026, 10, 8, 17, 0, tzinfo=UTC)) == day + timedelta(days=1)
    start, end = cal.day_window(day)
    assert (start, end) == (
        datetime(2026, 10, 7, 17, 0, tzinfo=UTC),
        datetime(2026, 10, 8, 17, 0, tzinfo=UTC),
    )


def test_offsets_round_trip_in_spark_format() -> None:
    """Stored as Spark's startingOffsets JSON, read back as {partition: offset}."""
    offsets = {2: 30, 0: 10, 1: 20}
    text = cal.offsets_to_json(offsets)
    assert text == '{"raw_events": {"0": 10, "1": 20, "2": 30}}'
    assert cal.offsets_from_json(text) == offsets


def test_end_reached_on_every_partition() -> None:
    """End offsets are exclusive: reached when the next offset to read is at the end."""
    end = {0: 10, 1: 20}
    assert cal.reached({0: 10, 1: 25}, end)
    assert not cal.reached({0: 9, 1: 25}, end)
    assert not cal.reached({0: 10}, end)


# --- Store, on the local Postgres --------------------------------------------------

NOW = datetime(2026, 10, 8, 5, 0, tzinfo=UTC)


@pytest.fixture
def store(monkeypatch):
    # The rotation counts from the configured start date: tests use their own
    monkeypatch.setattr(config, "ALTERNATION_START_DATE", START)
    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    admin = cal.connect_benchmark("postgres")
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()
    conn = cal.connect_benchmark(TEST_DB)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS branch_calendar")
    store = cal.CalendarStore(conn)
    store.ensure_table()
    yield store
    conn.close()


def test_close_chains_the_next_day_on_the_same_offsets(store) -> None:
    """End of J = start of J+1, written together, branch of J+1 by rotation."""
    store.create_day(START, {0: 0, 1: 0, 2: 0})
    store.open_day(START, NOW)
    closed = store.close_day(START, {0: 100, 1: 110, 2: 120}, NOW + timedelta(hours=12))
    assert closed.status == cal.CLOSING
    assert closed.end_offsets == {0: 100, 1: 110, 2: 120}
    following = store.get(START + timedelta(days=1))
    assert following.start_offsets == closed.end_offsets
    assert (following.branch, following.status) == ("A", cal.PENDING)
    with pytest.raises(cal.CalendarError):
        store.close_day(START, {0: 200}, NOW)


def test_supervisor_sees_only_its_branch_running(store) -> None:
    """B's day is visible to B only once open, and stays so while closing."""
    store.create_day(START, {0: 0})
    assert store.active_for("B") is None
    store.open_day(START, NOW)
    assert store.active_for("B").day == START
    assert store.active_for("A") is None
    store.close_day(START, {0: 5}, NOW)
    assert store.active_for("B").status == cal.CLOSING
    store.mark_stopped(START, NOW, cal.DONE)
    assert store.active_for("B") is None


def test_opening_waits_for_the_previous_engine_to_stop(store) -> None:
    """Never two engines: a previous day still running blocks the opening."""
    store.create_day(START, {0: 0})
    store.open_day(START, NOW)
    store.mark_started(START, NOW)
    store.close_day(START, {0: 5}, NOW)
    following = START + timedelta(days=1)
    with pytest.raises(cal.CalendarError, match="never reported its stop"):
        store.open_day(following, NOW + timedelta(days=1))
    store.mark_stopped(START, NOW, cal.DONE)
    assert store.open_day(following, NOW + timedelta(days=1)).status == cal.OPEN


def test_a_day_nobody_ran_is_skipped_at_the_next_opening(store) -> None:
    """A's day while A is not deployed: closed, never started, then skipped."""
    store.create_day(START, {0: 0})
    store.open_day(START, NOW)
    store.close_day(START, {0: 5}, NOW)
    store.open_day(START + timedelta(days=1), NOW + timedelta(days=1))
    assert store.get(START).status == cal.SKIPPED


def test_force_before_the_engine_started_only(store) -> None:
    """The force wins over the rotation, but never once the engine started."""
    store.create_day(START, {0: 0})
    assert store.force_branch(START, "A").effective_branch == "A"
    store.open_day(START, NOW)
    store.mark_started(START, NOW)
    with pytest.raises(cal.CalendarError):
        store.force_branch(START, "B")


def test_observations_keep_the_first_time(store) -> None:
    """A restart keeps the first start; caught up is recorded once."""
    store.create_day(START, {0: 0})
    store.mark_started(START, NOW)
    store.mark_started(START, NOW + timedelta(hours=1))
    assert store.mark_caught_up(START, NOW + timedelta(hours=2))
    assert not store.mark_caught_up(START, NOW + timedelta(hours=3))
    day = store.get(START)
    assert (day.started_at, day.caught_up_at) == (NOW, NOW + timedelta(hours=2))


def test_a_missed_opening_is_closed_and_noted(store) -> None:
    """07:00 missed: the day is still closed (bounds chain), marked as a late catch-up."""
    store.create_day(START, {0: 0})
    closed = store.close_day(START, {0: 5}, NOW)
    assert closed.status == cal.CLOSING
    assert "late catch-up" in closed.note


def test_failed_transaction_leaves_nothing(store) -> None:
    """A rejected opening rolls back the skips it made before failing."""
    store.create_day(START, {0: 0})
    store.open_day(START, NOW)
    store.mark_started(START, NOW)
    store.close_day(START, {0: 5}, NOW)
    with pytest.raises(cal.CalendarError):
        store.open_day(START + timedelta(days=1), NOW)
    assert store.get(START + timedelta(days=1)).status == cal.PENDING
