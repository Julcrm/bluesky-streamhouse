# =============================================================================
# Spark Structured Streaming job — Bluesky Streamhouse, branch A
# Consumes Redpanda `raw_events` and appends to the Iceberg Bronze table through the
# Lakekeeper REST catalog (Parquet on Garage, S3 requests signed by Lakekeeper).
# Runtime: Python 3.12-slim, Java 21 (Debian trixie; Spark 4.1 supports 17 and 21),
# PySpark 4.1.3 (spark group only), Iceberg and Kafka jars resolved at build time
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
    # Read by the job: every jar of this directory goes on the classpath
    SPARK_JARS_DIR=/opt/spark-jars \
    SPARK_CHECKPOINT_DIR=/app/state/spark-checkpoint/bronze

# Dependency layer, cached separately from the source code
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group spark

COPY src/config.py ./src/config.py
COPY src/__init__.py ./src/__init__.py

# Jars resolved once here (Iceberg runtime and AWS bundle, Kafka source with its
# dependencies), never at runtime: Ivy downloads them, then only the jars are kept
RUN .venv/bin/python -c "\
from pyspark.sql import SparkSession; from src import config; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', ','.join(config.SPARK_PACKAGES)) \
.config('spark.jars.ivy', '/tmp/ivy').getOrCreate().stop()" \
    && mkdir -p $SPARK_JARS_DIR && cp /tmp/ivy/jars/*.jar $SPARK_JARS_DIR/ \
    && rm -rf /tmp/ivy \
    # Drop what PySpark already ships (same versions: hadoop client, slf4j, snappy...)
    && for jar in $SPARK_JARS_DIR/*.jar; do \
         name=$(basename $jar | sed -E 's/^[^_]+_//'); \
         [ -f .venv/lib/python3.12/site-packages/pyspark/jars/$name ] && rm $jar; \
       done; ls $SPARK_JARS_DIR

COPY src ./src

RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /app/state/spark-checkpoint/days && chown -R appuser /app/state
USER appuser

# The supervisor (decision D28) is PID 1: it runs the job only on branch A's days and
# passes SIGTERM to it, which stops the query cleanly. Exec form, no shell in between
CMD [".venv/bin/python", "-m", "src.alternation.supervisor", "A", "--", \
     ".venv/bin/python", "-m", "src.processing.spark.stream_job"]
