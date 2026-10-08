.PHONY: check_uv install add add-dev test lint format up down reset logs ps produce quix spark spark-thrift iceberg-maintenance dbt-spark-build dbt-build dbt-parse dagster clean
# Check that uv is available
UV := $(shell command -v uv 2> /dev/null)
COMPOSE_DEV := docker compose -f docker-compose.dev.yaml

check_uv:
ifndef UV
	$(error "uv is not installed")
endif

# --- Python environment ---

install: check_uv
	uv sync --all-groups

add: check_uv
	@test -n "$(lib)" || (echo "Usage: make add lib=pandas [group=quix]" && exit 1)
	uv add $(if $(group),--group $(group)) $(lib)

add-dev: check_uv
	@test -n "$(lib)" || (echo "Usage: make add-dev lib=pytest" && exit 1)
	uv add --dev $(lib)

test: check_uv
	uv run pytest

lint: check_uv
	uv run ruff check --fix .

format: check_uv
	uv run ruff format .

# --- Local stack (Redpanda, Garage, Postgres) ---

# Creates .env from .env.example on first run (local-only credentials)
up:
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")
	$(COMPOSE_DEV) up -d --wait redpanda redpanda-console garage postgres lakekeeper
	# One-shot: bootstrap Lakekeeper and create the Iceberg warehouse (idempotent).
	# Not under --wait, which reports any exited container as a failure
	$(COMPOSE_DEV) up --build lakekeeper-init

down:
	$(COMPOSE_DEV) down

reset:
	$(COMPOSE_DEV) down -v

logs:
	$(COMPOSE_DEV) logs -f

ps:
	$(COMPOSE_DEV) ps

# --- Pipeline ---

# Jetstream -> Redpanda `raw_events` (Ctrl+C flushes and exits)
produce: check_uv
	uv run python -m src.ingestion.producer

quix: check_uv
	uv run python -m src.processing.quix.app

# Spark branch streaming job, in a container of the stack (Lakekeeper hands out garage:3900)
spark:
	$(COMPOSE_DEV) --profile spark up -d --build spark

# Iceberg maintenance of the Spark branch, once, with the bluesky_spark code location image
iceberg-maintenance:
	$(COMPOSE_DEV) --profile spark run --rm --build iceberg-maintenance

# Spark Thrift server of the Spark branch (dbt-spark's engine), localhost:10000
spark-thrift:
	$(COMPOSE_DEV) --profile spark up -d --build --wait spark-thrift

# dbt-spark of the Spark branch against the Thrift server, in the stack's network
# (dbt command and args: CMD="run --select silver_posts", default build)
dbt-spark-build:
	$(COMPOSE_DEV) --profile spark run --rm --build dbt-spark \
		$(or $(CMD),build) --project-dir /app/dbt/spark --profiles-dir /app/dbt/spark

# DuckDB branch Silver/Gold on the local DuckLake (dbt does not read .env itself)
DBT_DUCKDB = set -a && . ./.env && set +a && cd dbt/duckdb && uv run dbt

dbt-build: check_uv
	$(DBT_DUCKDB) build --profiles-dir .

dbt-parse: check_uv
	cd dbt/duckdb && uv run dbt parse --profiles-dir .
	cd dbt/spark && uv run dbt parse --profiles-dir .

# Dagster UI on localhost:3000 with the DuckDB branch's code location (local stack, .env)
dagster: check_uv
	set -a && . ./.env && set +a && uv run dagster dev -m src.dagster.definitions

clean:
	rm -rf .venv
	rm -rf build dist
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
