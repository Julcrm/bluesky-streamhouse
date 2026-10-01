"""
Failure alerts by email (Resend, as velib), shared by both code locations: each builds
its own sensor over its own jobs. No job import here, so branch A's code location does
not load branch B's dbt project.
"""

import html
from collections.abc import Sequence

import requests
from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    SensorDefinition,
    run_failure_sensor,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from src import config


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


def failure_alert_sensor(
    name: str, jobs: Sequence[UnresolvedAssetJobDefinition]
) -> SensorDefinition:
    """Email on any failed run of `jobs`."""

    @run_failure_sensor(
        name=name, monitored_jobs=list(jobs), default_status=DefaultSensorStatus.RUNNING
    )
    def _sensor(context: RunFailureSensorContext) -> None:
        error = context.failure_event.message if context.failure_event else "Unknown error"
        if not send_failure_email(context.dagster_run.run_id, context.dagster_run.job_name, error):
            context.log.warning("RESEND_API_KEY or ALERT_EMAIL missing: failure alert not sent")

    return _sensor
