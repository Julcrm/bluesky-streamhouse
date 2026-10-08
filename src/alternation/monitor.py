"""
What the calendar says about the running days (decision D28): failure alerts, and
whether a branch's Silver/Gold should run. No Dagster import: the sensor and the
schedule only call these functions.

Failure alerts only (Julien, 2026-10-06), each sent once per day and kind:
- not_started: the day is open for a deployed branch, its engine has not started;
- not_closed: still open after 19:00 (the 19:00 close did not happen);
- late: still running at 19:30 (the engine goes on, the day stays in the benchmark,
  marked);
- incomplete: stopped at the 06:30 hard stop, messages missing from Bronze;
- stop_missed: still running after the hard stop (the supervisor did not act).
A lost message is caught by the completeness check once the day is done.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from src import config
from src.alternation import calendar as cal


@dataclass(frozen=True)
class Alert:
    """One failure to report; `key` makes it sent once."""

    key: str
    subject: str
    body: str


def _alert(day: cal.CalendarDay, kind: str, subject: str, body: str) -> Alert:
    return Alert(
        key=f"{day.day.isoformat()}:{kind}",
        subject=f"day {day.day} ({day.effective_branch}): {subject}",
        body=f"{body} Status: {day.status}. Note: {day.note or '-'}.",
    )


def alerts(
    days: list[cal.CalendarDay],
    now: datetime,
    deployed: tuple[str, ...] | None = None,
) -> list[Alert]:
    """Failures visible in the calendar now, for the branches whose engine is deployed."""
    deployed = config.DEPLOYED_BRANCHES if deployed is None else deployed
    found = []
    for day in days:
        if day.effective_branch not in deployed:
            continue
        opened = cal.at(day.day, config.ALTERNATION_OPEN_TIME)
        closed = cal.at(day.day, config.ALTERNATION_CLOSE_TIME)
        hard_stop = cal.hard_stop_time(day.day)
        if (
            day.status == cal.OPEN
            and day.started_at is None
            and now >= opened + timedelta(minutes=config.ALERT_NOT_STARTED_MINUTES)
        ):
            found.append(
                _alert(day, "not_started", "engine not started", "The day is open, no engine ran.")
            )
        if day.status == cal.OPEN and now >= closed + timedelta(
            minutes=config.ALERT_NOT_CLOSED_MINUTES
        ):
            found.append(
                _alert(
                    day,
                    "not_closed",
                    "day not closed",
                    "The 19:00 close did not run: no end offsets, the engine waits for them.",
                )
            )
        if day.status in cal.RUNNING_STATUSES and now >= cal.late_time(day.day):
            found.append(
                _alert(
                    day,
                    "late",
                    "day late",
                    "The engine had not reached the end offsets at 19:30. It goes on until "
                    "06:30; the day stays in the benchmark, marked late.",
                )
            )
        if day.status == cal.INCOMPLETE:
            found.append(
                _alert(
                    day,
                    "incomplete",
                    "day incomplete",
                    "The engine was stopped at the 06:30 hard stop before its end offsets: "
                    "messages of the day are missing from its Bronze.",
                )
            )
        if day.status in cal.RUNNING_STATUSES and now >= hard_stop + timedelta(
            minutes=config.ALERT_STOP_MISSED_MINUTES
        ):
            found.append(
                _alert(
                    day,
                    "stop_missed",
                    "still running after the hard stop",
                    "The supervisor did not stop the engine at 06:30: the next opening is "
                    "refused while it may still run.",
                )
            )
    return found


def recent_days(limit: int = 3) -> list[cal.CalendarDay]:
    """The latest calendar days, empty when the calendar is unreachable (table not
    created yet, or Postgres down): schedules then skip their tick."""
    try:
        conn = cal.connect_benchmark()
        try:
            return cal.CalendarStore(conn).recent(limit)
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 - skip the tick, retried at the next one
        return []


def transform_due(days: list[cal.CalendarDay], branch: str, now: datetime) -> str | None:
    """Why the branch's Silver/Gold should run now, None if it should not: its engine
    may be running, or stopped less than TRANSFORM_TAIL_SECONDS ago (the day's last
    Bronze commits still have to reach Gold)."""
    for day in days:
        if day.effective_branch != branch:
            continue
        if day.status in cal.RUNNING_STATUSES:
            return f"day {day.day} is {day.status}"
        if day.stopped_at is not None and now - day.stopped_at < timedelta(
            seconds=config.TRANSFORM_TAIL_SECONDS
        ):
            return f"day {day.day} stopped at {day.stopped_at:%H:%M} UTC, finishing Gold"
    return None


def nightly_checks_due(days: list[cal.CalendarDay], branch: str, now: datetime) -> str | None:
    """Why the branch's nightly tests should run now (D30), None if they should not: the
    day that ended at the last 19:00 is the branch's and its engine is done, or late and
    finishing. Otherwise the one-day test window holds none of the branch's rows, and
    the tests would pass without checking anything."""
    ended = cal.benchmark_day(now) - timedelta(days=1)
    for day in days:
        if day.day != ended or day.effective_branch != branch:
            continue
        if day.status == cal.DONE:
            return f"day {day.day} of branch {branch} is done"
        if day.late_at is not None:
            return f"day {day.day} of branch {branch} is late, finishing"
    return None
