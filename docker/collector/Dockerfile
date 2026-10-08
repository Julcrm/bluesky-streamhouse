# =============================================================================
# Benchmark metrics collector — Bluesky Streamhouse (phase 7, decision D34)
# Samples the cgroups of the measured containers, the host and the catalogs every 10 s
# into the benchmark database. Neutral: its own cost is measured and charged to no branch.
# Runtime: Python 3.12-slim, uv, psycopg2 (benchmark group only)
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
RUN uv sync --frozen --no-dev --group benchmark

COPY src ./src

RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Exec form: Python is PID 1 and receives SIGTERM
CMD [".venv/bin/python", "-m", "src.benchmark.collector"]
