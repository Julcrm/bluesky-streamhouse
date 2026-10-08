"""Jobs of the DuckDB branch: Silver/Gold every 15 min, maintenance every night (D20, D21)."""

from dagster import AssetSelection, define_asset_job

from src import config
from src.dagster.assets import bluesky_dbt_models, quix_bronze
from src.dagster.maintenance import GROUP as MAINTENANCE_GROUP

silver_gold_job = define_asset_job(
    name="bluesky_silver_gold",
    selection=AssetSelection.keys(quix_bronze.key) | AssetSelection.assets(bluesky_dbt_models),
    description="Observe Bronze, then Silver (with catch-up passes) and Gold, dbt tests as checks.",
)

maintenance_job = define_asset_job(
    name="bluesky_maintenance",
    selection=AssetSelection.groups(MAINTENANCE_GROUP),
    description="Retention DELETE and CHECKPOINT of both DuckLake catalogs, storage checks, "
    "purge of old runs (D14, D21).",
    # One step at a time: each CHECKPOINT gets the whole DuckDB budget of the container
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 1}}}},
)

nightly_checks_job = define_asset_job(
    name="bluesky_nightly_checks",
    # The dbt tests only, no model: rerun over a day instead of 2 hours (D25)
    selection=AssetSelection.checks_for_assets(bluesky_dbt_models),
    description="Every dbt test of Silver and Gold over the last day (D25).",
    tags={config.NIGHTLY_CHECKS_TAG: "true"},
)
