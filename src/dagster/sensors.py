"""
Dagster sensors of the DuckDB branch, alerts by email (Resend, as velib, src/dagster/alerts.py):
- a run fails. A failed blocking storage check (90 GB bucket, 2 GB catalog) fails the
  maintenance run, so it alerts the same way (D14, D21);
- the calendar shows a failed day (D28): engine not started, day not closed, day late
  (19:30), incomplete (06:30 hard stop), hard stop not applied. The same sensor
  launches the completeness check of each finished day of the DuckDB branch.
"""

import html
import json
from datetime import UTC, datetime

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from src.alternation import calendar as cal
from src.alternation import monitor
from src.dagster.alerts import failure_alert_sensor as _failure_alert_sensor
from src.dagster.alerts import send_email, send_failure_email
from src.dagster.alternation import calendar_job, completeness_job, completeness_request_b
from src.dagster.jobs import maintenance_job, nightly_checks_job, silver_gold_job

__all__ = ["calendar_alert_sensor", "failure_alert_sensor", "send_email", "send_failure_email"]

# Every failed run of the DuckDB branch's jobs, the calendar's (a refused opening: an engine may
# still be running) and the completeness check's
failure_alert_sensor = _failure_alert_sensor(
    "failure_alert_sensor",
    [silver_gold_job, maintenance_job, nightly_checks_job, calendar_job, completeness_job],
)

# Alert keys remembered in the cursor (a few days of alerts)
SENT_ALERTS_KEPT = 50


@sensor(
    job=completeness_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def calendar_alert_sensor(context: SensorEvaluationContext) -> list[RunRequest] | SkipReason:
    """Every minute: email each failure the calendar shows (once per day and kind), and
    request the completeness check of each finished day of the DuckDB branch (once per day)."""
    sent: list[str] = json.loads(context.cursor) if context.cursor else []
    try:
        conn = cal.connect_benchmark()
        try:
            days = cal.CalendarStore(conn).recent(4)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - calendar not created yet, or Postgres down
        return SkipReason(f"Calendar unreachable: {e}")
    for alert in monitor.alerts(days, datetime.now(UTC)):
        if alert.key in sent:
            continue
        if not send_email(alert.subject, f"<p>{html.escape(alert.body)}</p>"):
            context.log.warning(f"RESEND_API_KEY or ALERT_EMAIL missing: {alert.subject}")
        sent.append(alert.key)
    context.update_cursor(json.dumps(sent[-SENT_ALERTS_KEPT:]))
    requests_ = [
        completeness_request_b(day.day)
        for day in days
        if day.status == cal.DONE and day.effective_branch == cal.BRANCH_DUCKDB
    ]
    return requests_ or SkipReason("No finished day of the DuckDB branch to check")
