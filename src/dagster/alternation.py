"""
Alternation of the branches (decision D28): Dagster is the only writer of the calendar
decisions. Every materialization of `branch_calendar` keeps the day's branch, bounds and
status in Dagster's history. No business logic here: see `src.alternation`.

- 07:00 Europe/Paris: open the day for its branch (the engine's supervisor starts it).
- 19:00: close it on the Kafka high watermarks, which also start the next day.
- Manual force from the launchpad: `action: force`, `day`, `branch`.
"""

from datetime import UTC, date, datetime

from dagster import (
    AssetExecutionContext,
    Config,
    DefaultScheduleStatus,
    Failure,
    MaterializeResult,
    RunRequest,
    ScheduleEvaluationContext,
    asset,
    define_asset_job,
    schedule,
)

# Imported under another name: Dagster requires the asset's run config parameter to be
# called `config`
from src import config as settings
from src.alternation import calendar as cal
from src.alternation import control

GROUP = "alternation"
OPEN, CLOSE, FORCE = "open", "close", "force"


class CalendarAction(Config):
    """What the run does to the calendar; the day defaults to the current one."""

    action: str = OPEN
    # ISO date, e.g. 2026-10-08
    day: str | None = None
    # Force only: A or B
    branch: str | None = None


def _metadata(day: cal.CalendarDay) -> dict:
    return {
        "day": day.day.isoformat(),
        "branch": day.effective_branch,
        "forced": day.forced_branch is not None,
        "status": day.status,
        "start_offsets": cal.offsets_to_json(day.start_offsets),
        "end_offsets": cal.offsets_to_json(day.end_offsets) if day.end_offsets else "",
        "offsets_source": day.offsets_source,
        "messages": control.day_messages(day) or 0,
    }


@asset(
    key_prefix=[settings.DAGSTER_ASSET_PREFIX, GROUP],
    group_name=GROUP,
    description="Benchmark calendar: branch, Kafka bounds and status of each day (D28).",
)
def branch_calendar(context: AssetExecutionContext, config: CalendarAction) -> MaterializeResult:
    """Open, close or force a day, then record the day as it stands in the calendar."""
    now = datetime.now(UTC)
    day = date.fromisoformat(config.day) if config.day else None
    conn = cal.connect_benchmark()
    try:
        store = cal.CalendarStore(conn)
        store.ensure_table()
        try:
            if config.action == OPEN:
                result = control.open_day(store, now, day)
            elif config.action == CLOSE:
                result = control.close_day(store, now, day)
            elif config.action == FORCE:
                if day is None or config.branch is None:
                    raise Failure("force needs a day and a branch", allow_retries=False)
                result = store.force_branch(day, config.branch)
            else:
                raise Failure(f"Unknown action {config.action!r}", allow_retries=False)
        except cal.CalendarError as e:
            # A rule of the alternation would break (two engines, day already closed):
            # the run fails and the failure sensor sends the alert
            raise Failure(str(e), allow_retries=False) from e
    finally:
        conn.close()
    if result is None:
        context.log.info(
            f"Before the alternation start date ({settings.ALTERNATION_START_DATE}): nothing to do"
        )
        return MaterializeResult(metadata={"action": config.action, "day": ""})
    context.log.info(
        f"{config.action}: day {result.day} ({result.effective_branch}) is {result.status}"
    )
    return MaterializeResult(metadata={"action": config.action, **_metadata(result)})


calendar_job = define_asset_job(
    name="bluesky_branch_calendar",
    selection=[branch_calendar],
    description="Open (07:00), close (19:00) or force a benchmark day (D28).",
)


def _cron(local: str) -> str:
    hours, minutes = local.split(":")
    return f"{int(minutes)} {int(hours)} * * *"


def _calendar_request(action: str) -> RunRequest:
    return RunRequest(run_config={"ops": {branch_calendar.op.name: {"config": {"action": action}}}})


@schedule(
    job=calendar_job,
    cron_schedule=_cron(settings.ALTERNATION_OPEN_TIME),
    execution_timezone=settings.ALTERNATION_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def open_day_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    """07:00: the branch of the day may start (and catch up the night)."""
    return _calendar_request(OPEN)


@schedule(
    job=calendar_job,
    cron_schedule=_cron(settings.ALTERNATION_CLOSE_TIME),
    execution_timezone=settings.ALTERNATION_TIMEZONE,
    default_status=DefaultScheduleStatus.RUNNING,
)
def close_day_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    """19:00: bounds of the day, start of the next one."""
    return _calendar_request(CLOSE)
