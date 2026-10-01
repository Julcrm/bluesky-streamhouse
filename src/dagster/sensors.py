"""
Dagster sensors of branch B: an email alert (Resend, as velib) when a Silver/Gold or
maintenance run fails. A failed blocking storage check (80 GB bucket, 2 GB catalog)
fails the maintenance run, so it alerts the same way (D14, D21).
"""

import html

import requests
from dagster import DefaultSensorStatus, RunFailureSensorContext, run_failure_sensor

from src import config
from src.dagster.jobs import maintenance_job, nightly_checks_job, silver_gold_job


def send_failure_email(run_id: str, job_name: str, error: str) -> bool:
    """Send a failure alert through the Resend API; False when alerts are not configured."""
    if not config.RESEND_API_KEY or not config.ALERT_EMAIL:
        return False
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {config.RESEND_API_KEY}"},
        json={
            "from": config.ALERT_FROM,
            "to": [config.ALERT_EMAIL],
            "subject": f"Bluesky Streamhouse - {job_name} failed",
            "html": (
                "<h2>Pipeline failure detected</h2>"
                f"<p><strong>Job:</strong> {html.escape(job_name)}</p>"
                f"<p><strong>Run ID:</strong> {html.escape(run_id)}</p>"
                f"<pre>{html.escape(error)}</pre>"
            ),
        },
        timeout=10,
    )
    response.raise_for_status()
    return True


@run_failure_sensor(
    monitored_jobs=[silver_gold_job, maintenance_job, nightly_checks_job],
    default_status=DefaultSensorStatus.RUNNING,
)
def failure_alert_sensor(context: RunFailureSensorContext) -> None:
    """Email on any failed run of the Silver/Gold, maintenance or nightly checks job."""
    error = context.failure_event.message if context.failure_event else "Unknown error"
    if not send_failure_email(context.dagster_run.run_id, context.dagster_run.job_name, error):
        context.log.warning("RESEND_API_KEY or ALERT_EMAIL missing: failure alert not sent")
