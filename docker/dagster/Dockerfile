# =============================================================================
# Dagster gRPC code server `bluesky_duckdb` — Bluesky Streamhouse, branch B
# Serves the Silver/Gold dbt assets to the shared dagster-workspace on port 4000.
# Runs execute here (DefaultRunLauncher): dbt-duckdb and DuckDB, no JVM (decision D20).
# Runtime: Python 3.12-slim, uv, dagster, dagster-dbt, dbt-duckdb (dagster group only)
# =============================================================================

FROM python:3.12.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /bin/uv

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    DAGSTER_HOME=/app/dagster_home \
    # dagster-dbt starts the `dbt` executable by name
    PATH="/app/.venv/bin:$PATH"

# Dependency layer, cached separately from the source code
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group dagster

COPY src ./src
COPY dbt/duckdb ./dbt/duckdb
COPY docker/dagster/dagster.yaml ./dagster_home/dagster.yaml

# dbt writes target/ and logs/ in the project at every run
RUN adduser --disabled-password --gecos '' appuser \
    && chown -R appuser /app/dbt/duckdb /app/dagster_home
USER appuser

# DuckDB extensions downloaded at build time into ~/.duckdb, never at runtime
RUN .venv/bin/python -c "import duckdb; from src.resources.ducklake import EXTENSIONS; \
[duckdb.execute(f'INSTALL {ext}') for ext in EXTENSIONS]"

# Manifest compiled at build (no database needed): the assets load it at startup
RUN .venv/bin/dbt parse --project-dir dbt/duckdb --profiles-dir dbt/duckdb

EXPOSE 4000

CMD [".venv/bin/dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src.dagster.definitions"]
