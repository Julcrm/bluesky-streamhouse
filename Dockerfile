FROM python:3.12-slim

# Install uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Cache dependencies layer separately from source code
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

RUN adduser --disabled-password --gecos '' appuser
USER appuser

CMD ["uv", "run", "bluesky-streamhouse"]
