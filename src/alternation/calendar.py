"""
Benchmark calendar: which branch owns each day, and the exact Kafka bounds of the day
(decision D28). Kept free of Dagster imports: the engine containers read it too.

A benchmark day J runs from 19:00 (J-1) to 19:00 (J), Europe/Paris. Its bounds are
Kafka offsets, not times: at 19:00 the high watermark of each partition is both the end
of J (exclusive) and the start of J+1, so no event is lost or counted twice between two
days, whatever the clock jitter of the 19:00 read.

Only Dagster writes the decisions (branch, bounds, status open/closing). The supervisor
of each engine container only reads them, and the engines record what they observed
(started_at, caught_up_at, stopped_at, final status).

Status of a day:
    pending   created at 19:00 the day before, with its start offsets
    open      07:00: the engine of its branch may run
    closing   19:00: end offsets known, the engine stops once it has written them all
    done      the engine stopped after its end offsets
    incomplete stopped by the 19:30 guard, or the day was never closed properly
    skipped   nobody ran it (branch not deployed yet, e.g. A while B runs alone)
"""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

from src import config

BRANCH_A = "A"
BRANCH_B = "B"
BRANCHES = (BRANCH_A, BRANCH_B)

PENDING, OPEN, CLOSING = "pending", "open", "closing"
DONE, INCOMPLETE, SKIPPED = "done", "incomplete", "skipped"
# A day in these statuses may have an engine running: never two at a time
RUNNING_STATUSES = (OPEN, CLOSING)
FINAL_STATUSES = (DONE, INCOMPLETE, SKIPPED)

# Kafka offsets of one day, by partition. End offsets are exclusive (next to read)
Offsets = dict[int, int]

TIMEZONE = ZoneInfo(config.ALTERNATION_TIMEZONE)

DDL = f"""
CREATE TABLE IF NOT EXISTS branch_calendar (
    day             DATE PRIMARY KEY,
    branch          CHAR(1) NOT NULL CHECK (branch IN ('A', 'B')),
    forced_branch   CHAR(1) CHECK (forced_branch IN ('A', 'B')),
    start_offsets   JSONB NOT NULL,
    end_offsets     JSONB,
    -- 'watermark' (read at 19:00) or 'timestamp' (fallback: offsets_for_times)
    offsets_source  TEXT NOT NULL DEFAULT 'watermark',
    status          TEXT NOT NULL CHECK (status IN (
                        '{PENDING}', '{OPEN}', '{CLOSING}', '{DONE}', '{INCOMPLETE}', '{SKIPPED}')),
    opened_at       TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    caught_up_at    TIMESTAMPTZ,
    stopped_at      TIMESTAMPTZ,
    note            TEXT
)
"""


class CalendarError(Exception):
    """A calendar decision that would break the alternation rules."""


@dataclass(frozen=True)
class CalendarDay:
    """One row of branch_calendar."""

    day: date
    branch: str
    forced_branch: str | None
    start_offsets: Offsets
    end_offsets: Offsets | None
    offsets_source: str
    status: str
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    started_at: datetime | None = None
    caught_up_at: datetime | None = None
    stopped_at: datetime | None = None
    note: str | None = None

    @property
    def effective_branch(self) -> str:
        """The branch that runs the day: a manual force wins over the rotation."""
        return self.forced_branch or self.branch


# --- Days and times ---------------------------------------------------------------


def branch_for(day: date, start: date = config.ALTERNATION_START_DATE) -> str:
    """Branch of a benchmark day by rotation (D1): B on the start date, then A, B, ..."""
    if day < start:
        raise CalendarError(f"{day} is before the alternation start date {start}")
    return BRANCH_B if (day - start).days % 2 == 0 else BRANCH_A


def _local_time(value: str) -> time:
    hours, minutes = value.split(":")
    return time(int(hours), int(minutes))


def at(day: date, local: str) -> datetime:
    """`local` (HH:MM, Europe/Paris) on `day`, in UTC: the UTC hour of 19:00 changes
    twice a year with daylight saving time."""
    return datetime.combine(day, _local_time(local), TIMEZONE).astimezone(UTC)


def benchmark_day(now: datetime) -> date:
    """The day whose window (19:00 J-1, 19:00 J] contains `now`."""
    local = now.astimezone(TIMEZONE)
    if local.time() >= _local_time(config.ALTERNATION_CLOSE_TIME):
        return local.date() + timedelta(days=1)
    return local.date()


def day_window(day: date) -> tuple[datetime, datetime]:
    """Start and end (UTC) of a benchmark day: 19:00 the day before, 19:00 the day."""
    return (
        at(day - timedelta(days=1), config.ALTERNATION_CLOSE_TIME),
        at(day, config.ALTERNATION_CLOSE_TIME),
    )


# --- Offsets -------------------------------------------------------------------------


def offsets_to_json(offsets: Offsets, topic: str = config.RAW_EVENTS_TOPIC) -> str:
    """Spark's startingOffsets format: {"raw_events": {"0": 123, "1": 456}}."""
    return json.dumps({topic: {str(p): o for p, o in sorted(offsets.items())}})


def offsets_from_json(
    value: str | dict | None, topic: str = config.RAW_EVENTS_TOPIC
) -> Offsets | None:
    """Inverse of offsets_to_json (psycopg2 already decodes JSONB into a dict)."""
    if value is None:
        return None
    data = json.loads(value) if isinstance(value, str) else value
    return {int(p): int(o) for p, o in data[topic].items()}


def reached(position: Offsets, end: Offsets) -> bool:
    """True once every partition's next offset to read is at or past its end."""
    return all(position.get(p, -1) >= o for p, o in end.items())


# --- Store ---------------------------------------------------------------------------


def connect_benchmark(dbname: str = config.BENCHMARK_DB):
    """Autocommit session on the benchmark database of the shared Postgres."""
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=dbname,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        connect_timeout=10,
        application_name="branch_calendar",
    )
    conn.autocommit = True
    return conn


def _row(record: dict) -> CalendarDay:
    return CalendarDay(
        **{
            **record,
            "start_offsets": offsets_from_json(record["start_offsets"]),
            "end_offsets": offsets_from_json(record["end_offsets"]),
        }
    )


class CalendarStore:
    """branch_calendar in the benchmark database. Each method is one transaction."""

    def __init__(self, conn) -> None:
        self._conn = conn

    @contextmanager
    def _transaction(self) -> Iterator:
        with self._conn:  # commits, or rolls back on error (autocommit is restored)
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur

    def ensure_table(self) -> None:
        with self._transaction() as cur:
            cur.execute(DDL)

    def get(self, day: date) -> CalendarDay | None:
        with self._transaction() as cur:
            cur.execute("SELECT * FROM branch_calendar WHERE day = %s", (day,))
            record = cur.fetchone()
        return _row(record) if record else None

    def running_days(self) -> list[CalendarDay]:
        """Days that may have an engine running (open or closing), oldest first."""
        with self._transaction() as cur:
            cur.execute(
                "SELECT * FROM branch_calendar WHERE status = ANY(%s) ORDER BY day",
                (list(RUNNING_STATUSES),),
            )
            return [_row(r) for r in cur.fetchall()]

    def active_for(self, branch: str) -> CalendarDay | None:
        """The day the engine of `branch` must work on now, if any (supervisor)."""
        days = [d for d in self.running_days() if d.effective_branch == branch]
        return days[0] if days else None

    # Decisions: Dagster only

    def create_day(
        self, day: date, start_offsets: Offsets, source: str = "watermark"
    ) -> CalendarDay:
        """Row of a day not created by the previous close (first day, missed close)."""
        with self._transaction() as cur:
            cur.execute(
                "INSERT INTO branch_calendar (day, branch, start_offsets, offsets_source, status) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (day) DO NOTHING",
                (day, branch_for(day), offsets_to_json(start_offsets), source, PENDING),
            )
        return self.get(day)

    def open_day(self, day: date, now: datetime) -> CalendarDay:
        """07:00: let the engine of the day's branch run. Days left running before are
        settled first (never started: skipped); one that may still have an engine
        running blocks the opening: two engines must never run together."""
        with self._transaction() as cur:
            cur.execute(
                "SELECT * FROM branch_calendar WHERE status = ANY(%s) AND day <> %s FOR UPDATE",
                (list(RUNNING_STATUSES), day),
            )
            for other in map(_row, cur.fetchall()):
                if other.started_at is None:
                    cur.execute(
                        "UPDATE branch_calendar SET status = %s, "
                        "note = coalesce(note || '; ', '') || 'never started' WHERE day = %s",
                        (SKIPPED, other.day),
                    )
                elif other.stopped_at is None:
                    raise CalendarError(
                        f"Day {other.day} ({other.effective_branch}) has an engine that never "
                        f"reported its stop: {day} is not opened"
                    )
            cur.execute(
                "UPDATE branch_calendar SET status = %s, opened_at = %s "
                "WHERE day = %s AND status = %s RETURNING *",
                (OPEN, now, day, PENDING),
            )
            if cur.fetchone() is None:
                cur.execute("SELECT status FROM branch_calendar WHERE day = %s", (day,))
                current = cur.fetchone()
                if current is None:
                    raise CalendarError(f"Day {day} has no calendar row")
                # Already open (a rerun of the 07:00 job) is fine
                if current["status"] not in RUNNING_STATUSES:
                    raise CalendarError(f"Day {day} is {current['status']}, it cannot be opened")
        return self.get(day)

    def close_day(
        self, day: date, end_offsets: Offsets, now: datetime, source: str = "watermark"
    ) -> CalendarDay:
        """19:00: the end offsets of `day`, and in the same transaction the next day's
        row starting exactly there (no gap, no overlap by construction)."""
        next_day = day + timedelta(days=1)
        with self._transaction() as cur:
            cur.execute("SELECT * FROM branch_calendar WHERE day = %s FOR UPDATE", (day,))
            record = cur.fetchone()
            if record is None:
                raise CalendarError(f"Day {day} has no calendar row")
            current = _row(record)
            if current.end_offsets is not None:
                raise CalendarError(f"Day {day} is already closed")
            # A day nobody opened (07:00 missed) is closed too: its bounds still chain, and
            # its engine processes it as a late catch-up (noted, the day is not comparable)
            status = CLOSING if current.status in (OPEN, PENDING) else current.status
            note = "never opened, late catch-up" if current.status == PENDING else None
            cur.execute(
                "UPDATE branch_calendar SET end_offsets = %s, closed_at = %s, status = %s, "
                "offsets_source = CASE WHEN %s = 'watermark' THEN offsets_source ELSE %s END, "
                "note = CASE WHEN %s::text IS NULL THEN note "
                "ELSE coalesce(note || '; ', '') || %s END WHERE day = %s",
                (offsets_to_json(end_offsets), now, status, source, source, note, note, day),
            )
            cur.execute(
                "INSERT INTO branch_calendar (day, branch, start_offsets, offsets_source, status) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (day) DO NOTHING",
                (next_day, branch_for(next_day), offsets_to_json(end_offsets), source, PENDING),
            )
        return self.get(day)

    def force_branch(self, day: date, branch: str) -> CalendarDay:
        """Manual override of a day's branch, before its engine started."""
        if branch not in BRANCHES:
            raise CalendarError(f"Unknown branch {branch!r}")
        with self._transaction() as cur:
            cur.execute(
                "UPDATE branch_calendar SET forced_branch = %s "
                "WHERE day = %s AND started_at IS NULL AND status = ANY(%s) RETURNING day",
                (branch, day, [PENDING, OPEN]),
            )
            if cur.fetchone() is None:
                raise CalendarError(f"Day {day} is missing, started or closed: not forced")
        return self.get(day)

    # Observations: supervisors and engines

    def mark_started(self, day: date, now: datetime) -> None:
        """First start of the day's engine (a restart keeps the first time)."""
        with self._transaction() as cur:
            cur.execute(
                "UPDATE branch_calendar SET started_at = coalesce(started_at, %s) WHERE day = %s",
                (now, day),
            )

    def mark_caught_up(self, day: date, now: datetime) -> bool:
        """First commit within the caught-up lag; False if already recorded."""
        with self._transaction() as cur:
            cur.execute(
                "UPDATE branch_calendar SET caught_up_at = %s "
                "WHERE day = %s AND caught_up_at IS NULL RETURNING day",
                (now, day),
            )
            return cur.fetchone() is not None

    def mark_stopped(self, day: date, now: datetime, status: str, note: str | None = None) -> None:
        """Final status of a day, set when its engine stopped (done or incomplete)."""
        if status not in (DONE, INCOMPLETE):
            raise CalendarError(f"Engines end a day as done or incomplete, not {status}")
        with self._transaction() as cur:
            cur.execute(
                "UPDATE branch_calendar SET stopped_at = %s, status = %s, "
                "note = CASE WHEN %s::text IS NULL THEN note "
                "ELSE coalesce(note || '; ', '') || %s END WHERE day = %s",
                (now, status, note, note, day),
            )
