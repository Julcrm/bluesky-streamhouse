"""Tests for the controlled test's runs (src.benchmark.runs) and the bench mode of the
supervisor and the engines (decision D36)."""

from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from src import config
from src.alternation import calendar as cal
from src.alternation.supervisor import Supervisor
from src.benchmark import runs
from tests.test_alternation_engine import SLEEPER, START, _Calendar, _day

NOW = datetime(2026, 10, 12, 20, 0, tzinfo=UTC)
NIGHT = date(2026, 10, 12)


def _run(run_id: int = 1, status: str = runs.REQUESTED, end=None) -> runs.BenchRunRow:
    return runs.BenchRunRow(
        run_id=run_id,
        branch="duckdb",
        night=NIGHT,
        rate="4x",
        repetition=1,
        status=status,
        end_offsets=end,
        requested_at=NOW,
    )


class _Bench:
    """In-memory bench_runs: one run at most."""

    def __init__(self) -> None:
        self.run: runs.BenchRunRow | None = None
        self.unreadable = False
        self.ingested: list[int] = []

    def get(self, run_id: int) -> runs.BenchRunRow | None:
        return self.run if self.run and self.run.run_id == run_id else None

    def active_for(self, branch: str) -> runs.BenchRunRow | None:
        if self.unreadable:
            raise RuntimeError("relation bench_runs does not exist")
        if self.run and self.run.branch == branch and self.run.status in runs.ACTIVE_STATUSES:
            return self.run
        return None

    def mark_running(self, run_id: int, now: datetime) -> bool:
        if self.run.status != runs.REQUESTED:
            return False
        self.run = replace(self.run, status=runs.RUNNING, started_at=now)
        return True

    def mark_ingested(self, run_id: int, now: datetime) -> bool:
        self.ingested.append(run_id)
        self.run = replace(self.run, status=runs.INGESTED, ingested_at=now)
        return True


# --- Engine side ----------------------------------------------------------------


def test_first_launch_reads_the_topic_from_its_start_then_a_restart_does_not() -> None:
    bench = _Bench()
    bench.run = _run()
    seeks = []
    runs.BenchRun(1, store=lambda: bench).prepare(seeks.append)
    assert seeks == [{0: 0, 1: 0, 2: 0}]
    assert bench.run.status == runs.RUNNING
    runs.BenchRun(1, store=lambda: bench).prepare(seeks.append)
    assert len(seeks) == 1


def test_everything_passes_until_the_replay_records_its_end() -> None:
    """End offsets appear once the replay sent everything; re-read every poll."""
    bench = _Bench()
    bench.run = _run()
    now = [0.0]
    run = runs.BenchRun(1, store=lambda: bench, poll_seconds=10, clock=lambda: now[0])
    run.prepare(lambda _: None)
    assert run.in_bounds(0, 10**9)
    bench.run = replace(bench.run, end_offsets={0: 100, 1: 100, 2: 100})
    assert run.in_bounds(0, 10**9)  # not re-read before the poll interval
    now[0] = 11.0
    assert not run.in_bounds(0, 100)
    assert run.in_bounds(1, 99)


def test_ingested_once_every_partition_reached_the_end() -> None:
    bench = _Bench()
    bench.run = _run(end={0: 10, 1: 10, 2: 10})
    run = runs.BenchRun(1, store=lambda: bench)
    run.prepare(lambda _: None)
    assert not run.check_complete({0: 10, 1: 10, 2: 9})
    assert run.check_complete({0: 10, 1: 10, 2: 10})
    assert bench.ingested == [1] and run.done


def test_run_id_comes_from_the_environment(monkeypatch) -> None:
    monkeypatch.delenv(runs.RUN_ENV, raising=False)
    assert runs.BenchRun.from_env() is None
    monkeypatch.setenv(runs.RUN_ENV, "42")
    assert runs.BenchRun.from_env().run_id == 42


# --- Supervisor -----------------------------------------------------------------


@pytest.fixture
def bench_supervisor():
    calendar, bench = _Calendar(), _Bench()
    clock = {"now": NOW, "mono": 1000.0}
    sup = Supervisor(
        "duckdb",
        SLEEPER,
        store=lambda: calendar,
        now=lambda: clock["now"],
        restart_delay=30,
        monotonic=lambda: clock["mono"],
        bench=lambda: bench,
    )
    yield sup, calendar, bench, clock
    sup.stop()


def test_requested_run_starts_the_engine_in_bench_mode(bench_supervisor) -> None:
    sup, _, bench, _ = bench_supervisor
    sup.tick()
    assert not sup.running()
    bench.run = _run()
    sup.tick()
    assert sup.running() and sup.run_id == 1 and sup.day is None


def test_other_branch_run_is_ignored(bench_supervisor) -> None:
    sup, _, bench, _ = bench_supervisor
    bench.run = replace(_run(), branch="spark")
    sup.tick()
    assert not sup.running()


def test_run_over_stops_the_engine(bench_supervisor) -> None:
    sup, _, bench, _ = bench_supervisor
    bench.run = _run()
    sup.tick()
    bench.run = replace(bench.run, status=runs.INGESTED)
    sup.tick()
    assert not sup.running() and sup.run_id is None


def test_a_day_always_wins_over_a_run(bench_supervisor) -> None:
    """A day of the branch becomes active during a run: the run is stopped, the day's
    engine starts after the restart delay; a run never starts while a day is active."""
    sup, calendar, bench, clock = bench_supervisor
    bench.run = _run()
    sup.tick()
    calendar.day = _day()
    sup.tick()
    assert not sup.running()
    clock["mono"] += 31
    sup.tick()
    assert sup.running() and sup.day == START and sup.run_id is None


def test_unreadable_bench_runs_never_keeps_a_day_from_starting(bench_supervisor) -> None:
    sup, calendar, bench, _ = bench_supervisor
    bench.unreadable = True
    sup.tick()
    assert not sup.running()
    calendar.day = _day()
    sup.tick()
    assert sup.running() and sup.day == START


# --- Engines' bench settings ----------------------------------------------------


def test_bench_catalogs_keep_the_alias_with_their_own_schema_and_path() -> None:
    from src.resources.ducklake import DuckLakeSettings, bench_settings, transform_settings

    bronze = bench_settings(DuckLakeSettings())
    assert bronze.alias == config.DUCKLAKE_BRONZE_ALIAS
    assert bronze.metadata_schema == config.DUCKLAKE_BENCH_BRONZE_METADATA_SCHEMA
    assert bronze.data_path == config.DUCKLAKE_BENCH_BRONZE_DATA_PATH
    transform = bench_settings(transform_settings())
    assert transform.alias == config.DUCKLAKE_TRANSFORM_ALIAS
    assert transform.metadata_schema == config.DUCKLAKE_BENCH_TRANSFORM_METADATA_SCHEMA


def test_spark_run_checkpoint_and_position_on_the_bench_topic(tmp_path) -> None:
    from src.processing.spark import stream_job

    run = runs.BenchRun(7, store=lambda: None)
    assert stream_job.day_checkpoint(run).endswith("bench/7")
    progress = '{"sources": [{"endOffset": {"bench_raw_events": {"0": 5, "1": 6, "2": 7}}}]}'
    assert stream_job.committed_position(progress, config.BENCH_TOPIC) == {0: 5, 1: 6, 2: 7}
    stale = tmp_path / "7"
    (stale / "offsets").mkdir(parents=True)
    stream_job.reset_checkpoint(str(stale))
    assert not stale.exists()


# --- Store, on the local stack ----------------------------------------------------


def _store():
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    conn = cal.connect_benchmark()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_runs")
    store = runs.BenchStore(conn)
    store.ensure_table()
    return store, conn


def test_store_lifecycle_of_a_run() -> None:
    store, conn = _store()
    try:
        run = store.request("spark", NIGHT, "max", 1, NOW)
        assert store.active_for("spark").run_id == run.run_id
        with pytest.raises(cal.CalendarError):
            store.request("spark", NIGHT, "max", 2, NOW)
        assert store.mark_running(run.run_id, NOW)
        assert not store.mark_running(run.run_id, NOW)
        store.set_end(run.run_id, {0: 3, 1: 4, 2: 5})
        assert store.get(run.run_id).end_offsets == {0: 3, 1: 4, 2: 5}
        assert store.mark_ingested(run.run_id, NOW)
        assert store.active_for("spark") is None
        store.finish(run.run_id, runs.DONE, NOW, "measured")
        assert store.get(run.run_id).status == runs.DONE
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS bench_runs")
        conn.close()
