"""
dagster-dbt pieces shared by both branches' Silver/Gold assets (decisions D20, D30).
No engine import here: branch A's code location must not load DuckDB, nor B's a JVM.
"""

import subprocess
from collections.abc import Mapping, Sequence
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator


def stop_process(process: subprocess.Popen, timeout: float = 30) -> None:
    """Terminate a dbt subprocess that is still running, then kill it if it hangs."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()


def is_silver(key: AssetKey) -> bool:
    """Silver models are named silver_* (their asset key ends with the model name)."""
    return key.path[-1].startswith("silver_")


def silver_selected(context: AssetExecutionContext) -> bool:
    """True when the run materializes at least one Silver model."""
    return any(is_silver(key) for key in context.selected_asset_keys)


class BlueskyDbtTranslator(DagsterDbtTranslator):
    """dbt models as `<prefix>/<layer>/<model>` (prefix `bluesky/<engine>`, D30) in the
    group of their layer (silver, gold).

    The Bronze source keeps the key set in its dbt meta (the engine's Bronze asset).
    """

    def __init__(self, prefix: Sequence[str]) -> None:
        super().__init__()
        self._prefix = list(prefix)

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props["resource_type"] != "model":
            return key
        return key.with_prefix(self._prefix)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if dbt_resource_props["resource_type"] != "model":
            return super().get_group_name(dbt_resource_props)
        # fqn = [project, layer folder, ..., model]
        return dbt_resource_props["fqn"][1]
