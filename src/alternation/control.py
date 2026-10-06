"""
Calendar decisions taken by Dagster at 07:00 and 19:00 (decision D28): open the day for
its branch, then close it on the Kafka high watermarks. No Dagster import: the assets
only call these functions, and tests pass fake Kafka lookups.
"""

from collections.abc import Callable
from datetime import date, datetime, timedelta

from src import config
from src.alternation import calendar as cal
from src.resources import redpanda

# A close this late after 19:00 no longer reads the 19:00 bound from the high watermark
CLOSE_TOLERANCE = timedelta(minutes=5)

Watermarks = Callable[[], cal.Offsets]
OffsetsAt = Callable[[datetime], cal.Offsets]


def _start_offsets(day: date, offsets_at: OffsetsAt) -> cal.Offsets:
    """Start of a day whose row the previous close did not create: the offsets at
    19:00 the day before, by timestamp (fallback, the day is marked)."""
    return offsets_at(cal.day_window(day)[0])


def open_day(
    store: cal.CalendarStore,
    now: datetime,
    day: date | None = None,
    offsets_at: OffsetsAt = redpanda.offsets_at,
) -> cal.CalendarDay | None:
    """07:00: open the current benchmark day for its branch; None before the start date."""
    day = day or cal.benchmark_day(now)
    if day < config.ALTERNATION_START_DATE:
        return None
    if store.get(day) is None:
        store.create_day(day, _start_offsets(day, offsets_at), source="timestamp")
    return store.open_day(day, now)


def close_day(
    store: cal.CalendarStore,
    now: datetime,
    day: date | None = None,
    watermarks: Watermarks = redpanda.high_watermarks,
    offsets_at: OffsetsAt = redpanda.offsets_at,
) -> cal.CalendarDay | None:
    """19:00: end offsets of the day and start of the next one, in one transaction.

    The bound is the high watermark read now, unless the close runs late (manual rerun),
    in which case it falls back to the offsets at 19:00 by timestamp. The eve of the
    start date only creates the first day, on exact offsets. Returns the closed day."""
    day = day or now.astimezone(cal.TIMEZONE).date()
    close_at = cal.at(day, config.ALTERNATION_CLOSE_TIME)
    late = now - close_at > CLOSE_TOLERANCE
    end, source = (offsets_at(close_at), "timestamp") if late else (watermarks(), "watermark")
    next_day = day + timedelta(days=1)
    if day < config.ALTERNATION_START_DATE:
        if next_day >= config.ALTERNATION_START_DATE:
            store.create_day(next_day, end, source=source)
        return None
    if store.get(day) is None:
        store.create_day(day, _start_offsets(day, offsets_at), source="timestamp")
    return store.close_day(day, end, now, source=source)


def day_messages(day: cal.CalendarDay) -> int | None:
    """Messages in the day's bounds (end - start, all partitions); None until closed."""
    if day.end_offsets is None:
        return None
    return sum(end - day.start_offsets.get(p, end) for p, end in day.end_offsets.items())
