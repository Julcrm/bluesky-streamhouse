"""
Lakekeeper's Iceberg REST API, without Spark: table listing and metadata (the benchmark
reset, the nightly table usage). No Thrift server needed, it only runs on Spark days.
"""

from typing import Any

from src import config


def catalog() -> tuple[str, Any]:
    """(base URL of the warehouse's REST paths, HTTP session)."""
    import requests

    session = requests.Session()
    base = config.ICEBERG_CATALOG_URI.rstrip("/")
    response = session.get(
        f"{base}/v1/config", params={"warehouse": config.ICEBERG_WAREHOUSE}, timeout=30
    )
    response.raise_for_status()
    body = response.json()
    # The warehouse prefix: in `defaults` for Lakekeeper, `overrides` win when set
    prefix = {**body.get("defaults", {}), **body.get("overrides", {})}["prefix"]
    return f"{base}/v1/{prefix}", session


def list_tables(url: str, session: Any, namespace: str) -> list[str]:
    """Table names of a namespace (none when it does not exist), every page."""
    names, token = [], None
    while True:
        params = {"pageToken": token} if token else {}
        response = session.get(f"{url}/namespaces/{namespace}/tables", params=params, timeout=30)
        if response.status_code == 404:
            return names
        response.raise_for_status()
        body = response.json()
        names += [ident["name"] for ident in body.get("identifiers", [])]
        token = body.get("next-page-token")
        if not token:
            return names


def table_metadata(url: str, session: Any, namespace: str, table: str) -> dict:
    response = session.get(f"{url}/namespaces/{namespace}/tables/{table}", timeout=30)
    response.raise_for_status()
    return response.json()["metadata"]
