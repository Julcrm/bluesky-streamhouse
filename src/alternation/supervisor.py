"""
Supervisor of an engine container (decision D28): the container's entry point, it runs
the engine as a subprocess only while the calendar gives a day to its branch.

    python -m src.alternation.supervisor B -- python -m src.processing.quix.app

Every SUPERVISOR_POLL_SECONDS it reads the calendar (it never writes a decision):
- a day open or closing for its branch and no engine running: start it, with
  ALTERNATION_DAY set (a crashed engine is restarted after a delay);
- no such day while the engine runs (the engine marked it done, or Dagster changed it):
  stop the engine;
- still running at the guard time (19:30): stop it, day incomplete (end offsets not
  reached, or the day was never closed).

It never starts an engine without a confirmed row, and keeps the current state when
Postgres is unreachable. SIGTERM (redeploy, container stop) is passed to the engine,
which flushes and commits before exiting; the day goes on after the restart.
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
            # offsets): either way the day is over
            if self._now() >= cal.at(day.day, config.ALTERNATION_GUARD_TIME):
                why = "end offsets not reached" if day.status == cal.CLOSING else "never closed"
                logger.error(f"Day {day.day}: engine still running at the guard time ({why})")
                self.stop()
                self.process = None
                self._calendar().mark_stopped(
                    day.day, self._now(), cal.INCOMPLETE, f"stopped by the guard: {why}"
                )
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
            deadline = self._monotonic() + poll_seconds
            while not stopping and self._monotonic() < deadline:
                time.sleep(1)
        logger.info("Supervisor stopping: passing SIGTERM to the engine")
        self.stop()


def main(argv: list[str] | None = None) -> None:
    """`supervisor <branch> -- <engine command...>`."""
    args = sys.argv[1:] if argv is None else argv
    if len(args) < 3 or args[1] != "--":
        raise SystemExit("usage: python -m src.alternation.supervisor <A|B> -- <command...>")
    Supervisor(args[0], args[2:], store=lambda: cal.CalendarStore(cal.connect_benchmark())).run()


if __name__ == "__main__":
    main()
