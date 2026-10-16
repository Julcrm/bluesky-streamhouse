"""
Controlled-test night of a branch (decision D36), shared by both code locations: a job
and its schedule, Monday and Tuesday at 20:00, run only after the branch's own day (so
each branch has one night a week, in an order that flips every week).

The night itself is src.benchmark.controlled; the branch brings how to empty its bench
tables and how to run Silver/Gold on them (`dbt --target bench`, passes until nothing is
left to read, then the tests once).
"""

import time
from collections.abc import Callable
from datetime import datetime

from dagster import (
    DefaultScheduleStatus,
    Failure,
    OpExecutionContext,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    job,
    op,
    schedule,
)
from dagster_dbt import DbtCliResource

from src import config
from src.alternation import calendar as cal
from src.benchmark import controlled, runs
from src.dagster.runs import wait_for_runs

BENCH_TARGET = ["--target", "bench"]
# The branch's day must be over before its engine can run the controlled test
DAY_END_TIMEOUT_SECONDS = 30 * 60
DAY_END_POLL_SECONDS = 30

Transform = Callable[[DbtCliResource, object], controlled.Transformed]


def tonight() -> datetime:
    return datetime.now(cal.TIMEZONE)


def wait_for_day_end(branch: str, log, timeout: float = DAY_END_TIMEOUT_SECONDS) -> None:
    """Wait until today's benchmark day of `branch` is final (its engine stopped)."""
    deadline = time.monotonic() + timeout
    while True:
        conn = cal.connect_benchmark()
        try:
            day = cal.CalendarStore(conn).get(tonight().date())
        finally:
            conn.close()
        if day is None or day.effective_branch != branch:
            raise Failure(f"Today is not a day of the {branch} branch", allow_retries=False)
        if day.status in cal.FINAL_STATUSES:
            return
        if time.monotonic() > deadline:
            raise Failure(f"Day {day.day} still {day.status}: no controlled test tonight")
        log.info(f"Day {day.day} still {day.status}, waiting for its engine to stop")
        time.sleep(DAY_END_POLL_SECONDS)


def bench_night_definitions(
    branch: str,
    clean: Callable[[], None],
    transform: Transform,
    arrivals: Callable[[], list[tuple[int, int, int]]],
):
    """The night job and schedule of `branch`."""

    @op(name=f"{branch}_bench_night")
    def bench_night(context: OpExecutionContext, dbt: DbtCliResource) -> None:
        wait_for_day_end(branch, context.log)
        # The day's Silver/Gold tail and any other run of this location first
        wait_for_runs(
            context.instance,
            context.log,
            context.run_id,
            None,
            config.BENCH_START_TIMEOUT_SECONDS,
        )
        conn = cal.connect_benchmark()
        try:
            summary = controlled.run_night(
                controlled.Tools(store=runs.BenchStore(conn)),
                controlled.BenchBranch(
                    branch, clean, lambda: transform(dbt, context.log), arrivals
                ),
                tonight().date(),
            )
        finally:
            conn.close()
        context.log.info(f"Controlled test of the {branch} branch: {summary}")
        if summary["failed"]:
            # Alert (failure sensor): the night's other runs went on regardless
            raise Failure(f"Controlled-test runs failed: {summary}", allow_retries=False)

    @job(
        name=f"bluesky_{branch}_bench_night",
        description=f"Controlled test of the {branch} branch: replays of the frozen sample "
        "into isolated tables, Silver/Gold on them (D36).",
    )
    def bench_night_job() -> None:
        bench_night()

    @schedule(
        job=bench_night_job,
        name=f"{branch}_bench_night_schedule",
        cron_schedule=config.BENCH_NIGHT_CRON,
        execution_timezone=config.DAGSTER_TIMEZONE,
        default_status=DefaultScheduleStatus.RUNNING,
    )
    def bench_night_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
        """After the branch's own day only: the other branch tests the other night."""
        today = tonight().date()
        try:
            conn = cal.connect_benchmark()
            try:
                day = cal.CalendarStore(conn).get(today)
            finally:
                conn.close()
        except Exception as e:  # noqa: BLE001 - calendar unreachable: no test tonight
            return SkipReason(f"Calendar unreachable: {e}")
        if day is None or day.effective_branch != branch:
            return SkipReason(f"{today} is not a day of the {branch} branch")
        return RunRequest(run_key=f"bench-{branch}-{today.isoformat()}")

    return bench_night_job, bench_night_schedule


def transform_passes(
    measure: Callable[[], object],
    left: Callable[[object], bool],
    run_pass: Callable[[], object],
    log,
) -> int:
    """Run Silver/Gold passes until nothing is left to read; the passes made. A pass that
    moves no read position fails: looping on it would hide a stalled model."""
    backlog, passes = measure(), 0
    while left(backlog):
        if passes >= config.CATCHUP_MAX_PASSES:
            raise Failure(f"Still a backlog after {passes} passes: {backlog}")
        previous, backlog = backlog, run_pass()
        passes += 1
        log.info(f"Bench pass {passes}: {backlog}")
        if not backlog.progressed_from(previous):
            raise Failure(f"Bench pass {passes} moved no read position ({previous})")
    return passes


def duckdb_transform(manifest_path) -> Transform:
    """Silver/Gold of the DuckDB branch on the bench catalogs."""

    def transform(dbt: DbtCliResource, log) -> controlled.Transformed:
        from src.processing.backlog import current_backlog
        from src.resources.ducklake import DuckLakeSettings, bench_settings, transform_settings

        def measure():
            return current_backlog(
                manifest_path,
                bronze_settings=bench_settings(DuckLakeSettings()),
                transform=bench_settings(transform_settings()),
            )

        def run_pass():
            dbt.cli(["run", *BENCH_TARGET]).wait()
            return measure()

        passes = transform_passes(
            measure, lambda b: b.silver_rows > 0 or b.gold_hours > 0, run_pass, log
        )
        dbt.cli(["test", *BENCH_TARGET]).wait()
        return controlled.Transformed(passes=passes)

    return transform


def spark_transform(logged_backlog: Callable) -> Transform:
    """Silver/Gold of the Spark branch on the bench namespaces; `logged_backlog` reads the
    backlog a dbt command logged (log_backlog macro)."""

    def transform(dbt: DbtCliResource, log) -> controlled.Transformed:
        passes = transform_passes(
            lambda: logged_backlog(dbt.cli(["run-operation", "measure_backlog", *BENCH_TARGET])),
            lambda b: b.silver_rows > 0 or b.gold_hours > 0,
            lambda: logged_backlog(dbt.cli(["run", *BENCH_TARGET])),
            log,
        )
        dbt.cli(["test", *BENCH_TARGET]).wait()
        return controlled.Transformed(passes=passes)

    return transform


# Bench Bronze arrivals: (partition, offset, first processed_at in epoch ms) per message
ARRIVALS_SQL = (
    "SELECT kafka_partition, kafka_offset, {epoch_ms}(min(processed_at)) "
    "FROM {table} GROUP BY kafka_partition, kafka_offset"
)


def duckdb_arrivals() -> list[tuple[int, int, int]]:
    """From the bench Bronze catalog of the DuckDB branch."""
    from src.processing.bronze import BRONZE_TABLE
    from src.resources.ducklake import DuckLakeSettings, bench_settings, connect

    settings = bench_settings(DuckLakeSettings())
    conn = connect(settings, read_only=True)
    try:
        table = f"{settings.alias}.main.{BRONZE_TABLE}"
        return conn.execute(ARRIVALS_SQL.format(epoch_ms="epoch_ms", table=table)).fetchall()
    finally:
        conn.close()


def spark_arrivals() -> list[tuple[int, int, int]]:
    """From the bench Bronze namespace of the Spark branch, through the Thrift server."""
    from src.processing.spark.stream_job import BENCH_BRONZE_IDENTIFIER
    from src.resources import thrift

    rows = thrift.query(ARRIVALS_SQL.format(epoch_ms="unix_millis", table=BENCH_BRONZE_IDENTIFIER))
    return [(int(p), int(o), int(ms)) for p, o, ms in rows]
