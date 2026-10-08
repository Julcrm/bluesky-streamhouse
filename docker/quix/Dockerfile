# =============================================================================
# Quix Streams app — Bluesky Streamhouse, the DuckDB branch
# Consumes Redpanda `raw_events` and writes the DuckLake Bronze table
# (Postgres catalog, Parquet files on Garage), on the DuckDB branch's days only (D28).
# Runtime: Python 3.12-slim, uv, quixstreams, duckdb, pyarrow (quix group only)
# =============================================================================

FROM python:3.12.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /bin/uv

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    # No download cache in the image (~300 MB for the quix group)
    UV_NO_CACHE=1

# Dependency layer, cached separately from the source code
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group quix

COPY src ./src

RUN adduser --disabled-password --gecos '' appuser \
    # Quix creates its state directory at startup, even for a stateless app
    && mkdir /app/state && chown appuser /app/state
USER appuser

# DuckDB extensions are downloaded at build time into ~/.duckdb, never at runtime:
# the INSTALL run on each connection then finds them locally
RUN .venv/bin/python -c "import duckdb; from src.resources.ducklake import EXTENSIONS; \
[duckdb.execute(f'INSTALL {ext}') for ext in EXTENSIONS]"

# The supervisor (decision D28) is PID 1: it runs the Quix app only on the DuckDB branch's days
# and passes SIGTERM to it, so Quix flushes the last checkpoint and commits its offsets
# before exiting. Exec form, no shell in between
CMD [".venv/bin/python", "-m", "src.alternation.supervisor", "duckdb", "--", \
     ".venv/bin/python", "-m", "src.processing.quix.app"]
