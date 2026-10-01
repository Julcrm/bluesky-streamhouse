"""
Dagster sensors of branch B: an email alert (Resend, as velib) when a Silver/Gold,
maintenance or nightly checks run fails. A failed blocking storage check (80 GB bucket,
2 GB catalog) fails the maintenance run, so it alerts the same way (D14, D21).
"""

from src.dagster.alerts import failure_alert_sensor as _failure_alert_sensor
from src.dagster.alerts import send_failure_email
from src.dagster.jobs import maintenance_job, nightly_checks_job, silver_gold_job

__all__ = ["failure_alert_sensor", "send_failure_email"]

failure_alert_sensor = _failure_alert_sensor(
    "failure_alert_sensor", [silver_gold_job, maintenance_job, nightly_checks_job]
)
