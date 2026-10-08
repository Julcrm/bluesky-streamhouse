"""
The Spark branch's side of the alternation (decision D28), in the `bluesky_spark` location:
the completeness check of each finished day of the Spark branch on its Iceberg Bronze, the same
contract as the DuckDB branch's (src/dagster/alternation.py). Calendar alerts are emailed once,
by the DuckDB branch's location, for every deployed branch.
"""

from dagster import (
    AssetExecutionContext,
    DefaultSensorStatus,
    MaterializeResult,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    sensor,
)

# Imported under another name: Dagster requires the asset's run config parameter to be
# called `config`
from src import config as settings
from src.alternation import calendar as cal
from src.alternation.completeness import offsets_query
from src.dagster.completeness import (
    CompletenessDay,
    closed_day,
    completeness_request,
    completeness_result,
)
from src.dagster.runs import wait_for_runs
from src.processing.bronze import BRONZE_TABLE
from src.resources import thrift

GROUP = "alternation"
BRONZE = f"{settings.SPARK_CATALOG}.{settings.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"


@asset(
    key_prefix=[settings.DAGSTER_ASSET_PREFIX, settings.DAGSTER_ENGINE_SPARK, GROUP],
    group_name=GROUP,
    description="Every Kafka offset of a finished day of the Spark branch is in its Iceberg "
    "Bronze (D28).",
)
def iceberg_day_completeness(
    context: AssetExecutionContext, config: CompletenessDay
) -> MaterializeResult:
    """Count the day's offsets in Bronze through the Thrift server (no JVM here, 5d); a
    missing one fails the run (alert). Alone in this location (D30): it ends the day
    while Silver/Gold may still be running."""
    day = closed_day(config.day)
    wait_for_runs(
        context.instance, context.log, context.run_id, None, settings.SPARK_RUN_WAIT_SECONDS
    )
    found = {
        int(row["kafka_partition"]): int(row["found"])
        for row in thrift.records(offsets_query(BRONZE, day))
    }
    return completeness_result(context, day, found)


iceberg_completeness_job = define_asset_job(
    name="bluesky_iceberg_day_completeness",
    selection=[iceberg_day_completeness],
    description="Every Kafka offset of a finished day of the Spark branch is in Bronze (D28).",
)


@sensor(
    job=iceberg_completeness_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def iceberg_completeness_sensor(context: SensorEvaluationContext) -> list[RunRequest] | SkipReason:
    """Every minute: the completeness check of each finished day of the Spark branch (once)."""
    try:
        conn = cal.connect_benchmark()
        try:
            days = cal.CalendarStore(conn).recent(4)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - calendar not created yet, or Postgres down
        return SkipReason(f"Calendar unreachable: {e}")
    requests_ = [
        completeness_request(iceberg_day_completeness.op.name, cal.BRANCH_SPARK, day.day)
        for day in days
        if day.status == cal.DONE and day.effective_branch == cal.BRANCH_SPARK
    ]
    return requests_ or SkipReason("No finished day of the Spark branch to check")
