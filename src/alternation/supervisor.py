"""
Supervisor of an engine container (decision D28): the container's entry point, it runs
the engine as a subprocess only while the calendar gives a day to its branch.

    python -m src.alternation.supervisor duckdb -- python -m src.processing.quix.app

Every SUPERVISOR_POLL_SECONDS it reads the calendar (it never writes a decision):
- a day open or closing for its branch and no engine running: start it, with
  ALTERNATION_DAY set (a crashed engine is restarted after a delay);
- no such day while the engine runs (the engine marked it done, or Dagster changed it):
  stop the engine;
- still running at 19:30: mark the day late and let the engine go on (no engine runs
  at night, it may finish its day);
- still running at 06:30 the next morning (hard stop, before the next opening): stop
  it, day incomplete (end offsets not reached, or the day was never closed).

It never starts an engine without a confirmed row, and keeps the current state when
Postgres is unreachable. SIGTERM (redeploy, container stop) is passed to the engine,
which flushes and commits before exiting; the day goes on after the restart.

Service mode (`--service`) supervises a service of the branch rather than its engine,
e.g. the Spark branch's Thrift server: it runs while the branch owns the latest opened
day, from
that opening until the next one (Silver/Gold tail, nightly maintenance and tests
included), with no day of its own and no late mark or hard stop.

    python -m src.alternation.supervisor spark --service -- \
        python -m src.processing.spark.thrift_server
"""

import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, date, datetime

from loguru import logger

from src import config
from src.alternation import calendar as cal
from src.alternation.engine import DAY_ENV
from src.healthcheck import touch

# Time the engine gets to flush and commit after SIGTERM (compose stop_grace_period: 30 s)
STOP_TIMEOUT_SECONDS = 25


class Supervisor:
    """Starts and stops one engine according to the calendar."""

    def __init__(
        self,
        branch: str,
        command: list[str],
        store: Callable[[], cal.CalendarStore],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        restart_delay: float = config.SUPERVISOR_RESTART_DELAY_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if branch not in cal.BRANCHES:
            raise ValueError(f"Unknown branch {branch!r}")
        self.branch = branch
        self.command = command
        self._store_factory = store
        self._store: cal.CalendarStore | None = None
        self._now = now
        self._monotonic = monotonic
        self._restart_delay = restart_delay
        self.process: subprocess.Popen | None = None
        self.day: date | None = None
        self._exited_at = float("-inf")

    def _calendar(self) -> cal.CalendarStore:
        if self._store is None:
            self._store = self._store_factory()
        return self._store

    def _read(self) -> cal.CalendarDay | None:
        try:
            return self._calendar().active_for(self.branch)
        except Exception:
            self._store = None
            raise

    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self, day: cal.CalendarDay) -> None:
        logger.info(f"Starting the branch {self.branch} engine for day {day.day}")
        env = {**os.environ, DAY_ENV: day.day.isoformat()}
        self.process = subprocess.Popen(self.command, env=env)
        self.day = day.day

    def stop(self) -> int | None:
        """SIGTERM, then SIGKILL if the engine does not exit in time."""
        if not self.running():
            return None
        self.process.send_signal(signal.SIGTERM)
        try:
            return self.process.wait(timeout=STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            logger.warning("Engine did not stop after SIGTERM, killing it")
            self.process.kill()
            return self.process.wait()

    def tick(self) -> None:
        """One look at the calendar and the engine."""
        if self.process is not None and not self.running():
            logger.warning(f"Engine exited with code {self.process.returncode}")
            self.process = None
            self._exited_at = self._monotonic()
        try:
            day = self._read()
        except Exception as e:  # noqa: BLE001 - keep the current state, retry next tick
            logger.warning(f"Calendar unreachable ({e}), keeping the current state")
            return
        if self.running():
            if day is None or day.day != self.day:
                logger.info(f"Day {self.day} is no longer active for {self.branch}: stopping")
                self.stop()
                self.process = None
                return
            # Closing and not done yet, or never closed (the engine waits for its end
            # offsets): late at 19:30, over at the hard stop
            now = self._now()
            if now >= cal.hard_stop_time(day.day):
                why = "end offsets not reached" if day.status == cal.CLOSING else "never closed"
                logger.error(f"Day {day.day}: engine still running at the hard stop ({why})")
                self.stop()
                self.process = None
                self._calendar().mark_stopped(
                    day.day, now, cal.INCOMPLETE, f"stopped at the hard stop: {why}"
                )
            elif now >= cal.late_time(day.day) and day.late_at is None:
                logger.warning(f"Day {day.day}: still running at 19:30, late, going on")
                self._calendar().mark_late(day.day)
            return
        if day is None or day.stopped_at is not None:
            return
        if self._monotonic() - self._exited_at < self._restart_delay:
            return
        self.start(day)

    def run(self, poll_seconds: float = config.SUPERVISOR_POLL_SECONDS) -> None:
        stopping = False

        def on_signal(signum, _frame) -> None:
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)
        logger.info(f"Supervisor of branch {self.branch}: {' '.join(self.command)}")
        while not stopping:
            self.tick()
            # Alive and polling, whatever the calendar says (an engine stopped on the
            # other branch's days is the normal state, not a failure)
            touch(config.HEARTBEAT_FILE)
            deadline = self._monotonic() + poll_seconds
            while not stopping and self._monotonic() < deadline:
                time.sleep(1)
        logger.info("Supervisor stopping: passing SIGTERM to the engine")
        self.stop()


class ServiceSupervisor(Supervisor):
    """Runs a service of the branch while the branch owns the latest opened day."""

    def start(self, day: cal.CalendarDay | None = None) -> None:
        logger.info(f"Branch {self.branch} owns the latest opened day: starting the service")
        self.process = subprocess.Popen(self.command)

    def tick(self) -> None:
        if self.process is not None and not self.running():
            logger.warning(f"Service exited with code {self.process.returncode}")
            self.process = None
            self._exited_at = self._monotonic()
        try:
            owner = self._calendar().owner()
        except Exception as e:  # noqa: BLE001 - keep the current state, retry next tick
            self._store = None
            logger.warning(f"Calendar unreachable ({e}), keeping the current state")
            return
        if self.running():
            if owner != self.branch:
                logger.info(f"Branch {owner} owns the latest opened day: stopping the service")
                self.stop()
                self.process = None
            return
        if owner == self.branch and self._monotonic() - self._exited_at >= self._restart_delay:
            self.start()


USAGE = "usage: python -m src.alternation.supervisor <spark|duckdb> [--service] -- <command...>"


def main(argv: list[str] | None = None) -> None:
    """`supervisor <branch> [--service] -- <command...>`."""
    args = sys.argv[1:] if argv is None else argv
    service = len(args) > 1 and args[1] == "--service"
    if service:
        args = [args[0], *args[2:]]
    if len(args) < 3 or args[1] != "--":
        raise SystemExit(USAGE)
    kind = ServiceSupervisor if service else Supervisor
    kind(args[0], args[2:], store=lambda: cal.CalendarStore(cal.connect_benchmark())).run()


if __name__ == "__main__":
    main()
