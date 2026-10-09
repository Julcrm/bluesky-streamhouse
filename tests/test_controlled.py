"""Tests for one night of the controlled test (src.benchmark.controlled, D36)."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

from src import config
from src.benchmark import controlled, runs
from src.benchmark.replay import ReplayResult

NIGHT = date(2026, 10, 12)
EVENING = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)  # 20:00 in Paris
END = {0: 10, 1: 10, 2: 10}


class _Store:
    """In-memory bench_runs; `engine` decides how the engine reacts on each poll."""

    def __init__(self, engine_starts: bool = True) -> None:
        self.rows: dict[int, runs.BenchRunRow] = {}
        self.engine_starts = engine_starts

    def ensure_table(self) -> None:
        pass

    def request(self, branch, night, rate, repetition, now, end_offsets=None):
        run_id = len(self.rows) + 1
        self.rows[run_id] = runs.BenchRunRow(
            run_id, branch, night, rate, repetition, runs.REQUESTED, end_offsets, now
        )
        return self.rows[run_id]

    def get(self, run_id):
        return self.rows.get(run_id)

    def set_end(self, run_id, end):
        self.rows[run_id] = replace(self.rows[run_id], end_offsets=end)

    def record_replay(self, run_id, started, finished, messages):
        self.rows[run_id] = replace(self.rows[run_id], replay_messages=messages)

    def record_transform(self, run_id, started, finished, passes):
        self.rows[run_id] = replace(self.rows[run_id], transform_passes=passes)

    def record_measures(self, run_id, measures):
        self.rows[run_id] = replace(self.rows[run_id], bronze_messages=measures["messages"])

    def finish(self, run_id, status, now, note=None):
        self.rows[run_id] = replace(self.rows[run_id], status=status, note=note)

    def engine_tick(self) -> None:
        """What the supervisor and the engine do between two polls."""
        for run_id, row in self.rows.items():
            if row.status == runs.REQUESTED and self.engine_starts:
                self.rows[run_id] = replace(row, status=runs.RUNNING)
            elif row.status == runs.RUNNING and row.end_offsets:
                self.rows[run_id] = replace(row, status=runs.INGESTED)


def _night(store: _Store, start: datetime = EVENING, fail_clean: bool = False):
    clock = {"now": start}
    calls = []

    def sleep(seconds: float) -> None:
        clock["now"] += timedelta(seconds=seconds)
        store.engine_tick()

    def replay(factor):
        calls.append(("replay", factor))
        clock["now"] += timedelta(seconds=60)
        return ReplayResult(messages=30, seconds=60, end_offsets=END)

    def clean():
        calls.append(("clean",))
        if fail_clean:
            raise RuntimeError("catalog down")

    def transform():
        calls.append(("transform",))
        return controlled.Transformed(passes=2)

    def measure(_store, row, arrivals):
        calls.append(("measure", row.rate, len(arrivals)))
        return {"messages": len(arrivals)}

    tools = controlled.Tools(
        store=store,
        now=lambda: clock["now"],
        sleep=sleep,
        reset_topic=lambda: calls.append(("reset",)),
        replay=replay,
        measure=measure,
    )
    branch = controlled.BenchBranch(
        "duckdb", clean, transform, lambda: [(0, i, 1000 + i) for i in range(30)]
    )
    return tools, branch, calls, clock


def test_plan_calibrates_the_first_week_then_4x_and_max() -> None:
    first = controlled.night_plan(date(2026, 10, 12))
    assert [r for r, _ in first] == ["1x"] * 3 + ["4x"] * 3 + ["max"] * 3
    later = controlled.night_plan(date(2026, 10, 19))
    assert [r for r, _ in later] == ["4x"] * 3 + ["max"] * 3


def test_deadline_is_01_30_the_next_morning() -> None:
    # 01:30 in Paris on 13/10 (CEST) is 23:30 UTC on 12/10
    assert controlled.night_deadline(NIGHT) == datetime(2026, 10, 12, 23, 30, tzinfo=UTC)


def test_paced_run_waits_for_the_engine_then_replays() -> None:
    store = _Store()
    tools, branch, calls, _ = _night(store)
    row = controlled.run_once(tools, branch, NIGHT, "4x", 1, controlled.night_deadline(NIGHT))
    assert row.status == runs.DONE
    assert row.end_offsets == END and row.replay_messages == 30 and row.transform_passes == 2
    assert calls == [
        ("clean",),
        ("reset",),
        ("replay", 4.0),
        ("transform",),
        ("measure", "4x", 30),
    ]
    assert row.bronze_messages == 30


def test_max_run_fills_the_topic_before_the_engine_starts() -> None:
    store = _Store()
    tools, branch, calls, _ = _night(store)
    row = controlled.run_once(tools, branch, NIGHT, "max", 1, controlled.night_deadline(NIGHT))
    assert row.status == runs.DONE
    assert calls[:3] == [("clean",), ("reset",), ("replay", None)]


def test_engine_that_never_starts_fails_the_run() -> None:
    store = _Store(engine_starts=False)
    tools, branch, calls, clock = _night(store)
    row = controlled.run_once(tools, branch, NIGHT, "4x", 1, controlled.night_deadline(NIGHT))
    assert row.status == runs.FAILED and "not running" in row.note
    assert ("replay", 4.0) not in calls
    assert clock["now"] - EVENING >= timedelta(seconds=config.BENCH_START_TIMEOUT_SECONDS)


def test_failure_before_the_run_exists_does_not_stop_the_night() -> None:
    store = _Store()
    tools, branch, _, _ = _night(store, fail_clean=True)
    summary = controlled.run_night(tools, branch, date(2026, 10, 19))
    assert len(summary["failed"]) == 6 and not summary["done"]


def test_runs_that_would_end_past_the_deadline_are_skipped() -> None:
    """Started at 01:20 in Paris: no run (13-16 min estimated) fits before 01:30."""
    store = _Store()
    late = datetime(2026, 10, 19, 23, 20, tzinfo=UTC)
    tools, branch, calls, _ = _night(store, start=late)
    summary = controlled.run_night(tools, branch, date(2026, 10, 19))
    assert len(summary["skipped"]) == 6 and calls == []


def test_full_calibration_night_fits_and_runs_in_order() -> None:
    store = _Store()
    tools, branch, _, _ = _night(store)
    summary = controlled.run_night(tools, branch, NIGHT)
    assert summary["done"] == [f"{r}#{i}" for r in ("1x", "4x", "max") for i in (1, 2, 3)]


# --- Dagster wiring and cleanup ---------------------------------------------------


class _Backlog:
    def __init__(self, rows: int) -> None:
        self.silver_rows, self.gold_hours = rows, 0

    def progressed_from(self, previous: "_Backlog") -> bool:
        return self.silver_rows < previous.silver_rows

    def __repr__(self) -> str:
        return f"{self.silver_rows} rows"


class _Log:
    def info(self, _message: str) -> None:
        pass


def test_passes_run_until_nothing_is_left() -> None:
    from src.dagster.bench import transform_passes

    left = iter([_Backlog(300), _Backlog(0)])
    passes = transform_passes(
        lambda: _Backlog(600), lambda b: b.silver_rows > 0, lambda: next(left), _Log()
    )
    assert passes == 2


def test_a_pass_that_moves_nothing_fails() -> None:
    import pytest
    from dagster import Failure

    from src.dagster.bench import transform_passes

    with pytest.raises(Failure):
        transform_passes(
            lambda: _Backlog(600), lambda b: b.silver_rows > 0, lambda: _Backlog(600), _Log()
        )


def test_night_schedule_runs_after_its_own_day_only(monkeypatch) -> None:
    from dagster import RunRequest, SkipReason, build_schedule_context

    from src.alternation import calendar as cal
    from src.dagster import bench

    class _Conn:
        def close(self) -> None:
            pass

    owner = {"branch": "spark"}

    def get(self, day):
        return cal.CalendarDay(day, owner["branch"], None, {0: 0}, {0: 1}, "watermark", cal.DONE)

    monkeypatch.setattr(cal, "connect_benchmark", _Conn)
    monkeypatch.setattr(cal.CalendarStore, "__init__", lambda self, conn: None)
    monkeypatch.setattr(cal.CalendarStore, "get", get)
    _, schedule = bench.bench_night_definitions("duckdb", lambda: None, lambda d, log: None, list)
    assert isinstance(schedule(build_schedule_context()), SkipReason)
    owner["branch"] = "duckdb"
    request = schedule(build_schedule_context())
    assert isinstance(request, RunRequest) and request.run_key.startswith("bench-duckdb-")


def test_cleanup_refuses_anything_but_bench_objects() -> None:
    import pytest

    from src.benchmark import cleanup

    with pytest.raises(ValueError):
        cleanup.drop_ducklake_bench(schemas=("bronze",), paths=())
    with pytest.raises(ValueError):
        cleanup.drop_iceberg_bench(namespaces=("silver",))


def test_drop_ducklake_bench_leaves_production_untouched() -> None:
    """On the local stack: the bench catalog is gone and recreated empty; production
    keeps its tables."""
    import pytest

    from src.benchmark import cleanup
    from src.resources.ducklake import DuckLakeSettings, bench_settings, connect
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    bench = bench_settings(DuckLakeSettings())
    conn = connect(bench)
    conn.execute("CREATE TABLE IF NOT EXISTS bronze.main.probe AS SELECT 1 AS x")
    conn.execute("INSERT INTO bronze.main.probe VALUES (2)")
    conn.execute("CALL ducklake_flush_inlined_data('bronze')")
    conn.close()
    deleted = cleanup.drop_ducklake_bench()
    assert deleted[bench.data_path] >= 1
    conn = connect(bench)
    tables = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'bronze'"
    ).fetchall()
    conn.close()
    assert tables == []
    prod = connect(DuckLakeSettings(), read_only=True)
    try:
        prod_tables = prod.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'bronze'"
        ).fetchall()
    finally:
        prod.close()
    assert ("bronze_events",) in prod_tables
