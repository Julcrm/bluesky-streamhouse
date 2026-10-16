"""
One night of the controlled test for one branch (decision D36): every rate and
repetition of the night's plan, each one a run of `bench_runs`.

A run:
1. empties the branch's bench tables (at the start, not the end: the night's last run
   stays for the parity check against the other branch);
2. recreates the replay topic empty;
3. at "max", replays the whole sample first, then requests the run with its end
   offsets (the engine starts on a full topic: a pure catch-up); at 1x and 4x,
   requests the run, waits for the engine to start, waits BENCH_WARMUP_SECONDS, then
   replays at the target rate and records the end offsets;
4. waits until the engine has written every replayed message (`ingested`): the
   supervisor then stops the engine;
5. runs Silver/Gold on the bench tables (passes until nothing is left, then the tests);
6. measures the run (src/benchmark/run_measures.py: the branch hands in its bench Bronze
   arrivals) and closes it `done`. Any error closes it `failed` and the night goes on.

A run is started only if its estimate fits before the night's deadline (01:30): the
runs that do not fit are skipped. Kept free of Dagster and of each engine: the branch
passes how to empty its tables and how to run Silver/Gold.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from loguru import logger

from src import config
from src.alternation import calendar as cal
from src.benchmark import replay as rp
from src.benchmark import runs


@dataclass(frozen=True)
class Transformed:
    """What Silver/Gold did on a run's bench tables."""

    passes: int


@dataclass(frozen=True)
class BenchBranch:
    """What a night needs from a branch: empty its bench tables, run Silver/Gold, read
    its bench Bronze arrivals (partition, offset, first processed_at in ms)."""

    branch: str
    clean: Callable[[], None]
    transform: Callable[[], Transformed]
    arrivals: Callable[[], list[tuple[int, int, int]]]


def _measure(store: runs.BenchStore, row: runs.BenchRunRow, arrivals: list) -> dict:
    """The run's measures, with the replay topic's send times (1x and 4x only)."""
    from src.benchmark import run_measures

    sends = run_measures.send_times() if row.rate != "max" else None
    return run_measures.measure_run(store.connection, row, arrivals, sends)


@dataclass
class Tools:
    """The night's dependencies, replaced in the tests."""

    store: runs.BenchStore
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    sleep: Callable[[float], None] = time.sleep
    reset_topic: Callable[[], None] = rp.reset_bench_topic
    replay: Callable[[float | None], rp.ReplayResult] = rp.replay
    measure: Callable[[runs.BenchStore, runs.BenchRunRow, list], dict] = _measure


def night_plan(night: date) -> list[tuple[str, int]]:
    """(rate, repetition) of a night: 1x, 4x and max during calibration, then 4x and
    max; each rate repeated BENCH_REPETITIONS times."""
    calibration = night <= date.fromisoformat(config.BENCH_CALIBRATION_UNTIL)
    rates = ("1x", "4x", "max") if calibration else ("4x", "max")
    return [(rate, r) for rate in rates for r in range(1, config.BENCH_REPETITIONS + 1)]


def night_deadline(night: date) -> datetime:
    """When the night's runs must be over: BENCH_DEADLINE the next morning."""
    return cal.at(night + timedelta(days=1), config.BENCH_DEADLINE)


def estimated_seconds(rate: str, branch: str) -> float:
    """A generous estimate of one run, to decide whether it fits before the deadline."""
    factor = rp.RATE_FACTORS[rate]
    if factor is None:
        replay_seconds = config.BENCH_MAX_REPLAY_ESTIMATE_SECONDS
    else:
        replay_seconds = config.BENCH_SAMPLE_MINUTES * 60 / factor + config.BENCH_WARMUP_SECONDS
    return (
        replay_seconds
        + config.BENCH_TRANSFORM_ESTIMATE_SECONDS[branch]
        + config.BENCH_RUN_MARGIN_SECONDS
    )


class RunFailed(RuntimeError):
    """A run that cannot go on (engine not started, ingestion not over in time)."""


def wait_for(
    tools: Tools,
    run_id: int,
    statuses: tuple[str, ...],
    until: datetime,
    poll_seconds: float = 5.0,
) -> runs.BenchRunRow:
    """Poll the run until it reaches one of `statuses`; RunFailed past `until`."""
    while True:
        row = tools.store.get(run_id)
        if row is not None and row.status in statuses:
            return row
        if row is not None and row.status == runs.FAILED:
            raise RunFailed(f"Run {run_id} failed: {row.note}")
        if tools.now() >= until:
            raise RunFailed(f"Run {run_id} not {'/'.join(statuses)} by {until.isoformat()}")
        tools.sleep(poll_seconds)


def run_once(
    tools: Tools, branch: BenchBranch, night: date, rate: str, repetition: int, deadline: datetime
) -> runs.BenchRunRow | None:
    """One run, closed `done` or `failed`; returns its final row (None when it failed
    before its row was created: cleaning, topic reset, or the replay at "max")."""
    store, factor = tools.store, rp.RATE_FACTORS[rate]
    logger.info(f"Bench {branch.branch} {night} {rate} #{repetition}: starting")
    run_id = None
    try:
        branch.clean()
        tools.reset_topic()
        if factor is None:
            started = tools.now()
            result = tools.replay(None)
            run = store.request(
                branch.branch, night, rate, repetition, tools.now(), result.end_offsets
            )
            run_id = run.run_id
            store.record_replay(run_id, started, tools.now(), result.messages)
        else:
            run = store.request(branch.branch, night, rate, repetition, tools.now())
            run_id = run.run_id
            start_by = min(
                deadline, tools.now() + timedelta(seconds=config.BENCH_START_TIMEOUT_SECONDS)
            )
            wait_for(tools, run_id, (runs.RUNNING,), start_by)
            tools.sleep(config.BENCH_WARMUP_SECONDS)
            started = tools.now()
            result = tools.replay(factor)
            store.set_end(run_id, result.end_offsets)
            store.record_replay(run_id, started, tools.now(), result.messages)
        wait_for(tools, run_id, (runs.INGESTED,), deadline)
        transform_started = tools.now()
        transformed = branch.transform()
        store.record_transform(run_id, transform_started, tools.now(), transformed.passes)
        tools.sleep(config.BENCH_MEASURE_DELAY_SECONDS)
        measures = tools.measure(store, store.get(run_id), branch.arrivals())
        store.record_measures(run_id, measures)
        store.finish(run_id, runs.DONE, tools.now())
    except Exception as e:  # noqa: BLE001 - the run fails, the night goes on
        logger.error(f"Bench {branch.branch} {night} {rate} #{repetition} failed: {e}")
        if run_id is None:
            return None
        store.finish(run_id, runs.FAILED, tools.now(), str(e)[:500])
    return store.get(run_id)


def run_night(tools: Tools, branch: BenchBranch, night: date) -> dict:
    """Every run of the night's plan that fits before the deadline; a summary."""
    tools.store.ensure_table()
    deadline = night_deadline(night)
    summary = {"done": [], "failed": [], "skipped": []}
    for rate, repetition in night_plan(night):
        label = f"{rate}#{repetition}"
        needed = timedelta(seconds=estimated_seconds(rate, branch.branch))
        if tools.now() + needed > deadline:
            logger.warning(f"Bench {branch.branch} {night} {label}: would end past {deadline}")
            summary["skipped"].append(label)
            continue
        row = run_once(tools, branch, night, rate, repetition, deadline)
        summary["done" if row is not None and row.status == runs.DONE else "failed"].append(label)
    return summary
