# =============================================================================
# Dashboard API — Bluesky Streamhouse (phase 8)
# Read-only FastAPI serving the portfolio dashboard: benchmark database, Redpanda
# offsets, Dagster. Runtime: Python 3.12-slim, uv, the `api` dependency group only.
# =============================================================================

FROM python:3.12.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /bin/uv

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

# Dependency layer, cached separately from the source code
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group api

COPY src ./src

RUN adduser --disabled-password --gecos '' appuser
USER appuser

EXPOSE 8000

# One worker: the answers are cached in memory, a second worker would double the queries
CMD [".venv/bin/uvicorn", "src.serving.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
