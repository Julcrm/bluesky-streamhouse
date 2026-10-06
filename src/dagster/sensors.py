"""
Dagster sensors of branch B, alerts by email (Resend, as velib):
- a run fails. A failed blocking storage check (80 GB bucket, 2 GB catalog) fails the
  maintenance run, so it alerts the same way (D14, D21);
- the calendar shows a failed day (D28): engine not started, day not closed, day late
  (19:30), incomplete (06:30 hard stop), hard stop not applied. The same sensor
  launches the completeness check of each finished day of branch B.
"""

import html
import json
from datetime import UTC, datetime

import requests
from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    run_failure_sensor,
    sensor,
)

from src import config
from src.alternation import calendar as cal
from src.alternation import monitor
from src.dagster.alternation import calendar_job, completeness_job, completeness_request
from src.dagster.jobs import maintenance_job, nightly_checks_job, silver_gold_job


def send_email(subject: str, body_html: str) -> bool:
    """Send an alert through the Resend API; False when alerts are not configured."""
    if not config.RESEND_API_KEY or not config.ALERT_EMAIL:
        return False
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {config.RESEND_API_KEY}"},
        json={
            "from": config.ALERT_FROM,
            "to": [config.ALERT_EMAIL],
            "subject": f"Bluesky Streamhouse - {subject}",
            "html": body_html,
        },
        timeout=10,
    )
    response.raise_for_status()
    return True


def send_failure_email(run_id: str, job_name: str, error: str) -> bool:
    """Alert for a failed run; False when alerts are not configured."""
    return send_email(
        f"{job_name} failed",
        "<h2>Pipeline failure detected</h2>"
        f"<p><strong>Job:</strong> {html.escape(job_name)}</p>"
        f"<p><strong>Run ID:</strong> {html.escape(run_id)}</p>"
        f"<pre>{html.escape(error)}</pre>",
    )


@run_failure_sensor(
    monitored_jobs=[
        silver_gold_job,
        maintenance_job,
        nightly_checks_job,
        calendar_job,
        completeness_job,
    ],
    default_status=DefaultSensorStatus.RUNNING,
)
def failure_alert_sensor(context: RunFailureSensorContext) -> None:
    """Email on any failed run of the Silver/Gold, maintenance, nightly checks, calendar
    (a refused opening: an engine may still be running) or completeness job."""
    error = context.failure_event.message if context.failure_event else "Unknown error"
    if not send_failure_email(context.dagster_run.run_id, context.dagster_run.job_name, error):
        context.log.warning("RESEND_API_KEY or ALERT_EMAIL missing: failure alert not sent")


# Alert keys remembered in the cursor (a few days of alerts)
SENT_ALERTS_KEPT = 50


@sensor(
    job=completeness_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def calendar_alert_sensor(context: SensorEvaluationContext) -> list[RunRequest] | SkipReason:
    """Every minute: email each failure the calendar shows (once per day and kind), and
    request the completeness check of each finished day of branch B (once per day)."""
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
        completeness_request(day.day)
        for day in days
        if day.status == cal.DONE and day.effective_branch == cal.BRANCH_B
    ]
    return requests_ or SkipReason("No finished day of branch B to check")
