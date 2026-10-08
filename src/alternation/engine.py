"""
The engine's side of a benchmark day (decision D28), shared by Quix and Spark.

The supervisor launches the engine with ALTERNATION_DAY set. The engine then:
- on the day's first launch, moves its read position to the day's start offsets, then
  records `started_at` (a restart later in the day resumes from its own commits);
- drops every message at or past the day's end offsets, which appear at 19:00. From
  19:00 by its own clock until it knows them, it waits instead of processing: whatever
  it processed before 19:00 was written before Dagster read the high watermarks, so the
  end offsets are exact (otherwise ~30 s of the next day would reach Bronze, as measured
  locally on 2026-10-06);
- records `caught_up_at` at its first commit whose newest event is less than
  CAUGHT_UP_LAG_SECONDS old (same rule in both branches);
- marks the day done once its committed position reaches the end offsets on every
  partition. The supervisor then sees the day is no longer active and stops it.

Postgres being briefly unreachable never stops the engine: the last bounds are kept and
an observation is retried at the next commit.
"""

import os
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

from loguru import logger

from src import config
from src.alternation import calendar as cal

DAY_ENV = "ALTERNATION_DAY"

Store = Callable[[], cal.CalendarStore]


def _default_store() -> cal.CalendarStore:
    return cal.CalendarStore(cal.connect_benchmark())


class EngineDay:
    """Bounds and observations of the day an engine works on."""

    def __init__(
        self,
        day: date,
        store: Store = _default_store,
        poll_seconds: float = config.ENGINE_BOUNDS_POLL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.day = day
        self.close_at = cal.at(day, config.ALTERNATION_CLOSE_TIME)
        self._now = now
        self._sleep = sleep
        self._store_factory = store
        self._store: cal.CalendarStore | None = None
        self._poll_seconds = poll_seconds
        self._clock = clock
        self._last_read = float("-inf")
        self.start: cal.Offsets = {}
        self.end: cal.Offsets | None = None
        self.caught_up = False
        self.done = False

    @classmethod
    def from_env(cls) -> "EngineDay | None":
        """The day set by the supervisor; None when run by hand (unbounded, local dev)."""
        value = os.getenv(DAY_ENV)
        return cls(date.fromisoformat(value)) if value else None

    def _calendar(self) -> cal.CalendarStore:
        if self._store is None:
            self._store = self._store_factory()
        return self._store

    def _drop_store(self) -> None:
        if self._store is not None:
            try:
                self._store._conn.close()
            except Exception:  # noqa: BLE001 - already broken, only freeing it
                pass
            self._store = None

    def _load(self) -> cal.CalendarDay:
        row = self._calendar().get(self.day)
        if row is None:
            raise cal.CalendarError(f"Day {self.day} has no calendar row")
        self.start, self.end = row.start_offsets, row.end_offsets
        self.caught_up = row.caught_up_at is not None
        self._last_read = self._clock()
        return row

    def prepare(self, seek: Callable[[cal.Offsets], None]) -> None:
        """Before the engine consumes: on the day's first launch, `seek` to the start
        offsets, then record the start. Errors propagate: the engine must not start
        from an unknown position (the supervisor restarts it)."""
        row = self._load()
        if row.started_at is None:
            seek(row.start_offsets)
            logger.info(f"Day {self.day}: first launch, reading from {row.start_offsets}")
            self._calendar().mark_started(self.day, datetime.now(UTC))
        else:
            logger.info(f"Day {self.day}: restart, resuming from the engine's own commits")

    def refresh(self) -> None:
        """Re-read the end offsets every poll interval (they appear at 19:00)."""
        if self.end is not None or self._clock() - self._last_read < self._poll_seconds:
            return
        try:
            self._load()
            if self.end is not None:
                logger.info(f"Day {self.day}: end offsets {self.end}")
        except Exception as e:  # noqa: BLE001 - keep consuming on the last known bounds
            logger.warning(f"Reading the day's bounds failed ({e}), retrying later")
            self._drop_store()
            self._last_read = self._clock()

    def wait_for_end(self) -> None:
        """Past 19:00 without end offsets: block until Dagster records them. Never
        returns without them; if the close never comes, the supervisor stops the engine
        at the hard stop."""
        logger.info(f"Day {self.day}: 19:00 passed, waiting for the end offsets")
        while self.end is None:
            self._sleep(1)
            self._last_read = float("-inf")
            self.refresh()

    def bounds(self) -> cal.Offsets | None:
        """End offsets to apply now: None before 19:00 while unknown (nothing to drop
        yet); from 19:00, waits for them instead of guessing."""
        self.refresh()
        if self.end is None and self._now() >= self.close_at:
            self.wait_for_end()
        return self.end

    def in_bounds(self, partition: int, offset: int) -> bool:
        """True for a message of the day: before its end (see `bounds`)."""
        end = self.bounds()
        return end is None or offset < end.get(partition, 0)

    def record_commit(self, newest_event: datetime | None) -> None:
        """After a commit: caught up once the newest event is recent enough."""
        if self.caught_up or newest_event is None:
            return
        now = datetime.now(UTC)
        if now - newest_event >= timedelta(seconds=config.CAUGHT_UP_LAG_SECONDS):
            return
        try:
            self._calendar().mark_caught_up(self.day, now)
            self.caught_up = True
            logger.info(f"Day {self.day}: caught up (newest event {now - newest_event} old)")
        except Exception as e:  # noqa: BLE001 - retried at the next commit
            logger.warning(f"Recording caught_up_at failed ({e})")
            self._drop_store()

    def check_complete(self, position: cal.Offsets) -> bool:
        """Mark the day done once the committed position covers the end offsets."""
        self.refresh()
        if self.done or self.end is None or not cal.reached(position, self.end):
            return self.done
        try:
            self._calendar().mark_stopped(self.day, datetime.now(UTC), cal.DONE)
        except Exception as e:  # noqa: BLE001 - retried at the next check
            logger.warning(f"Marking the day done failed ({e})")
            self._drop_store()
            return False
        self.done = True
        logger.info(f"Day {self.day}: every partition reached its end offsets, done")
        return True
