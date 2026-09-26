# =============================================================================
# Jetstream Producer — Bluesky Streamhouse
# Streams Bluesky Jetstream commit events into the Redpanda `raw_events` topic.
# Runtime: Python 3.12-slim, uv, websockets, confluent-kafka (producer group only)
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
RUN uv sync --frozen --no-dev --group producer

COPY src ./src

RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Exec form: Python is PID 1 and receives SIGTERM, so the producer flushes before exiting
CMD [".venv/bin/python", "-m", "src.ingestion.producer"]
