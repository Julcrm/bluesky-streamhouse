# =============================================================================
# Dagster gRPC code server `bluesky_spark` — Bluesky Streamhouse, branch A
# Serves branch A's assets to the shared dagster-workspace on port 4000 (phase 4: Iceberg
# maintenance; phase 5: dbt-spark Silver/Gold). Runs execute here (DefaultRunLauncher):
# Spark in local mode, its memory counts against this container's limit (D20).
# Runtime: Python 3.12-slim, Java 21, PySpark 4.1.3, dagster, Iceberg jars (build time)
# =============================================================================

FROM python:3.12.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /bin/uv

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    SPARK_JARS_DIR=/opt/spark-jars \
    # Same path as the dagster-workspace (runs carry its instance config)
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DAGSTER_CODE_LOCATION=bluesky_spark \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group dagster --group spark --group dbt-spark

COPY src/config.py ./src/config.py
COPY src/__init__.py ./src/__init__.py

# Iceberg and Kafka jars resolved once at build, PySpark's own duplicates dropped
RUN .venv/bin/python -c "\
from pyspark.sql import SparkSession; from src import config; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', ','.join(config.SPARK_PACKAGES)) \
.config('spark.jars.ivy', '/tmp/ivy').getOrCreate().stop()" \
    && mkdir -p $SPARK_JARS_DIR && cp /tmp/ivy/jars/*.jar $SPARK_JARS_DIR/ \
    && rm -rf /tmp/ivy \
    && for jar in $SPARK_JARS_DIR/*.jar; do \
         name=$(basename $jar | sed -E 's/^[^_]+_//'); \
         [ -f .venv/lib/python3.12/site-packages/pyspark/jars/$name ] && rm $jar; \
       done; ls $SPARK_JARS_DIR

COPY src ./src
COPY docker/dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /tmp/spark && chown -R appuser /opt/dagster /tmp/spark
USER appuser

EXPOSE 4000

# Close the runs the previous container left STARTED (they died with it), then serve
CMD ["sh", "-c", ".venv/bin/python -m src.dagster.reap_runs && exec .venv/bin/dagster api grpc -h 0.0.0.0 -p 4000 -m src.dagster.spark_definitions"]
