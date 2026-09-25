"""
Shared DuckLake connection for branch B (Quix sink, maintenance, Dagster assets).
DuckDB attached to a Postgres catalog (decision D12), with Parquet data files on Garage.

Kept free of Dagster imports: the Quix image does not install Dagster. The Dagster
resource (phase 3) wraps `connect()`.

Credentials go through DuckDB secrets, never through the ATTACH string, so they do not
leak into error messages. DuckLake only picks up the *default* (unnamed) Postgres secret.
"""

from dataclasses import dataclass
from urllib.parse import urlsplit

import duckdb

from src import config

EXTENSIONS = ("ducklake", "postgres", "httpfs")


@dataclass(frozen=True)
class DuckLakeSettings:
    """Everything needed to attach the lake; defaults come from `src.config`."""

    catalog_host: str = config.POSTGRES_HOST
    catalog_port: int = config.POSTGRES_PORT
    catalog_db: str = config.DUCKLAKE_CATALOG_DB
    catalog_user: str = config.POSTGRES_USER
    catalog_password: str = config.POSTGRES_PASSWORD
    s3_endpoint_url: str = config.S3_ENDPOINT_URL
    s3_region: str = config.S3_REGION
    s3_access_key_id: str = config.S3_ACCESS_KEY_ID
    s3_secret_access_key: str = config.S3_SECRET_ACCESS_KEY
    data_path: str = config.DUCKLAKE_DATA_PATH
    # Not persisted by DuckLake: applied on every ATTACH
    inlining_row_limit: int = config.DUCKLAKE_DATA_INLINING_ROW_LIMIT
    alias: str = config.DUCKLAKE_ALIAS


def sql_literal(value: str | int) -> str:
    """Quote a value as a SQL string literal (CREATE SECRET does not accept parameters)."""
    return "'" + str(value).replace("'", "''") + "'"


def s3_endpoint(url: str) -> tuple[str, bool]:
    """Split an endpoint URL into DuckDB's `host:port` and USE_SSL flag."""
    parts = urlsplit(url)
    if not parts.netloc:
        raise ValueError(f"S3 endpoint must be a URL with a scheme, got {url!r}")
    return parts.netloc, parts.scheme == "https"


def setup_statements(settings: DuckLakeSettings, read_only: bool = False) -> list[str]:
    """SQL run on a fresh connection: extensions, S3 and Postgres secrets, then ATTACH."""
    endpoint, use_ssl = s3_endpoint(settings.s3_endpoint_url)
    s = sql_literal
    attach_options = [
        f"DATA_PATH {s(settings.data_path)}",
        f"DATA_INLINING_ROW_LIMIT {int(settings.inlining_row_limit)}",
    ]
    if read_only:
        attach_options.append("READ_ONLY")
    return [
        *(f"INSTALL {ext}; LOAD {ext};" for ext in EXTENSIONS),
        f"""CREATE SECRET garage (
            TYPE s3, KEY_ID {s(settings.s3_access_key_id)},
            SECRET {s(settings.s3_secret_access_key)}, REGION {s(settings.s3_region)},
            ENDPOINT {s(endpoint)}, URL_STYLE 'path', USE_SSL {str(use_ssl).lower()}
        )""",
        # Unnamed on purpose: DuckLake ignores named Postgres secrets
        f"""CREATE SECRET (
            TYPE postgres, HOST {s(settings.catalog_host)}, PORT {int(settings.catalog_port)},
            DATABASE {s(settings.catalog_db)}, USER {s(settings.catalog_user)},
            PASSWORD {s(settings.catalog_password)}
        )""",
        f"ATTACH 'ducklake:postgres:' AS {settings.alias} ({', '.join(attach_options)})",
        f"USE {settings.alias}",
    ]


def connect(
    settings: DuckLakeSettings | None = None, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the lake attached and selected."""
    settings = settings or DuckLakeSettings()
    conn = duckdb.connect()
    try:
        for statement in setup_statements(settings, read_only=read_only):
            conn.execute(statement)
    except Exception:
        conn.close()
        raise
    return conn
