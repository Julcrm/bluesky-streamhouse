"""Unit tests for src.resources.ducklake, plus one round trip against the local stack."""

import socket
import uuid

import pytest

from src import config
from src.resources.ducklake import (
    DuckLakeSettings,
    connect,
    s3_endpoint,
    setup_statements,
    sql_literal,
)

SETTINGS = DuckLakeSettings(
    catalog_host="postgres-host",
    catalog_port=5433,
    catalog_db="catalog_db",
    catalog_user="lake_user",
    catalog_password="pa'ss",
    s3_endpoint_url="http://garage:3900",
    s3_region="us-east-1",
    s3_access_key_id="GKtest",
    s3_secret_access_key="s3-secret",
    data_path="s3://bucket/ducklake/",
    inlining_row_limit=10,
    alias="lake",
)


def test_sql_literal_escapes_quotes() -> None:
    """Single quotes are doubled so a password cannot break out of the literal."""
    assert sql_literal("pa'ss") == "'pa''ss'"
    assert sql_literal(5432) == "'5432'"


def test_s3_endpoint_http_and_https() -> None:
    """DuckDB wants host:port without scheme, and SSL only for https."""
    assert s3_endpoint("http://garage:3900") == ("garage:3900", False)
    assert s3_endpoint("https://s3.example.com") == ("s3.example.com", True)


def test_s3_endpoint_rejects_bare_host() -> None:
    """A missing scheme is a configuration error, not a silent http default."""
    with pytest.raises(ValueError):
        s3_endpoint("garage:3900")


def test_setup_keeps_credentials_out_of_attach() -> None:
    """Passwords and keys only appear in CREATE SECRET, never in the ATTACH string."""
    statements = setup_statements(SETTINGS)
    attach = next(s for s in statements if s.startswith("ATTACH"))
    for secret in ("pa''ss", "s3-secret", "lake_user", "postgres-host"):
        assert secret not in attach
    assert any("PASSWORD 'pa''ss'" in s for s in statements)
    assert any("USE_SSL false" in s and "ENDPOINT 'garage:3900'" in s for s in statements)


def test_setup_postgres_secret_is_unnamed() -> None:
    """DuckLake only uses the default Postgres secret: it must not be named."""
    statements = setup_statements(SETTINGS)
    assert any(s.startswith("CREATE SECRET (\n") and "TYPE postgres" in s for s in statements)


def test_setup_attach_options() -> None:
    """ATTACH carries the data path, the inlining limit, and READ_ONLY on request."""
    attach = next(s for s in setup_statements(SETTINGS) if s.startswith("ATTACH"))
    assert attach == (
        "ATTACH 'ducklake:postgres:' AS lake "
        "(DATA_PATH 's3://bucket/ducklake/', DATA_INLINING_ROW_LIMIT 10)"
    )
    attach_ro = next(
        s for s in setup_statements(SETTINGS, read_only=True) if s.startswith("ATTACH")
    )
    assert attach_ro.endswith(", READ_ONLY)")


def _local_stack_up() -> bool:
    """True when the local Postgres catalog is reachable and S3 credentials are set."""
    if not (config.POSTGRES_USER and config.S3_ACCESS_KEY_ID):
        return False
    try:
        with socket.create_connection((config.POSTGRES_HOST, config.POSTGRES_PORT), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _local_stack_up(), reason="local stack not running (make up)")
def test_connect_round_trip_on_local_stack() -> None:
    """Write through the inlining path and the Parquet path, read back, clean up."""
    table = f"lake.main.test_{uuid.uuid4().hex[:8]}"
    conn = connect()
    try:
        conn.execute(f"CREATE TABLE {table} (id INTEGER)")
        conn.execute(f"INSERT INTO {table} SELECT range FROM range(5)")  # inlined
        conn.execute(f"INSERT INTO {table} SELECT range FROM range(1000)")  # Parquet
        assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (1005,)
        files = conn.execute(
            f"SELECT data_file FROM ducklake_list_files('lake', '{table.split('.')[-1]}')"
        ).fetchall()
        assert len(files) == 1
        assert files[0][0].startswith(config.DUCKLAKE_DATA_PATH)
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
