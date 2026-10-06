"""
Branch A's side of the alternation (decision D28), in the `bluesky_spark` location:
the completeness check of each finished day of A on its Iceberg Bronze, the same
contract as branch B's (src/dagster/alternation.py). Calendar alerts are emailed once,
by branch B's location, for every deployed branch.
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
from src.processing.bronze import BRONZE_TABLE
from src.resources.spark import build_session

GROUP = "alternation"
BRONZE = f"{settings.SPARK_CATALOG}.{settings.ICEBERG_BRONZE_NAMESPACE}.{BRONZE_TABLE}"


@asset(
    key_prefix=[settings.DAGSTER_ASSET_PREFIX, GROUP],
    group_name=GROUP,
    description="Every Kafka offset of a finished day of branch A is in its Iceberg Bronze (D28).",
)
def iceberg_day_completeness(
    context: AssetExecutionContext, config: CompletenessDay
) -> MaterializeResult:
    """Count the day's offsets in Bronze; a missing one fails the run (alert)."""
    day = closed_day(config.day)
    spark = build_session("bluesky-day-completeness", settings.SPARK_MAINTENANCE_DRIVER_MEMORY)
    spark.sparkContext.setLogLevel("WARN")
    try:
        found = {
            row["kafka_partition"]: row["found"]
            for row in spark.sql(offsets_query(BRONZE, day)).collect()
        }
    finally:
        spark.stop()
    return completeness_result(context, day, found)


iceberg_completeness_job = define_asset_job(
    name="bluesky_iceberg_day_completeness",
    selection=[iceberg_day_completeness],
    description="Every Kafka offset of a finished day of branch A is in Bronze (D28).",
)


@sensor(
    job=iceberg_completeness_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def iceberg_completeness_sensor(context: SensorEvaluationContext) -> list[RunRequest] | SkipReason:
    """Every minute: the completeness check of each finished day of branch A (once)."""
    try:
        conn = cal.connect_benchmark()
        try:
            days = cal.CalendarStore(conn).recent(4)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 - calendar not created yet, or Postgres down
        return SkipReason(f"Calendar unreachable: {e}")
    requests_ = [
        completeness_request(iceberg_day_completeness.op.name, cal.BRANCH_A, day.day)
        for day in days
        if day.status == cal.DONE and day.effective_branch == cal.BRANCH_A
    ]
    return requests_ or SkipReason("No finished day of branch A to check")
