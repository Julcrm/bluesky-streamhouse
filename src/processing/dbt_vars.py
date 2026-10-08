"""
Caps of one dbt run, read from a project's dbt_project.yml (single source of truth): a
Silver run reads at most `silver_max_rows_per_run` Bronze rows and a Gold model
rebuilds at most `gold_max_hours_per_run` hours. Same vars in both projects (contract).
Kept free of engine imports: both code locations read it.
"""

from pathlib import Path

import yaml

from src import config


def dbt_var(name: str, project_dir: Path = config.DBT_DUCKDB_PROJECT_DIR) -> int:
    """Integer var of dbt_project.yml (single source of truth for the caps)."""
    with open(project_dir / "dbt_project.yml") as f:
        return int(yaml.safe_load(f)["vars"][name])


def silver_max_rows_per_run(project_dir: Path = config.DBT_DUCKDB_PROJECT_DIR) -> int:
    """Bronze rows one Silver run reads at most."""
    return dbt_var("silver_max_rows_per_run", project_dir)


def gold_max_hours_per_run(project_dir: Path = config.DBT_DUCKDB_PROJECT_DIR) -> int:
    """Hours one Gold model rebuilds per run at most."""
    return dbt_var("gold_max_hours_per_run", project_dir)
