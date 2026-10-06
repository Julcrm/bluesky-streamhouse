"""
What the calendar says about the running days (decision D28): failure alerts, and
whether a branch's Silver/Gold should run. No Dagster import: the sensor and the
schedule only call these functions.

Failure alerts only (Julien, 2026-10-06), each sent once per day and kind:
- not_started: the day is open for a deployed branch, its engine has not started;
- not_closed: still open after 19:00 (the 19:00 close did not happen);
- incomplete: stopped by the 19:30 guard, or never properly closed;
- guard_missed: still closing after the guard time (the supervisor did not act).
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
        guard = cal.at(day.day, config.ALTERNATION_GUARD_TIME)
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
        if day.status == cal.INCOMPLETE:
            found.append(
                _alert(
                    day,
                    "incomplete",
                    "day incomplete",
                    "The engine did not reach the end offsets; the day is out of the benchmark.",
                )
            )
        if day.status == cal.CLOSING and now >= guard + timedelta(
            minutes=config.ALERT_GUARD_MISSED_MINUTES
        ):
            found.append(
                _alert(
                    day,
                    "guard_missed",
                    "still running after the guard",
                    "The supervisor did not stop the engine at the guard time.",
                )
            )
    return found


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
