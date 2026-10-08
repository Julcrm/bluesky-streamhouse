"""
Spark Thrift server of the Spark branch (decision D6 revised): one long-lived JVM in local mode
that dbt-spark reaches through PyHive (profile `method: thrift`), with the Lakekeeper
Iceberg catalog as default catalog (D26).

    python -m src.processing.spark.thrift_server

Builds the spark-submit command of HiveThriftServer2 and replaces this process with it
(exec), so the supervisor's SIGTERM reaches the JVM. The Iceberg jars also go on the
driver class path: with `--jars` alone, some sessions opened by the server do not find
SparkCatalog (ClassNotFoundException, seen on 2026-10-07).
"""

import os

from loguru import logger

from src import config
from src.resources.spark import catalog_conf, image_jars

THRIFT_SERVER_CLASS = "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"


def server_conf() -> dict[str, str]:
    """Settings of every session the server opens (those of the dbt-spark profile)."""
    return {
        "spark.driver.extraJavaOptions": " ".join(
            # Hive's embedded Derby metastore (unused: Iceberg tables live in Lakekeeper)
            # writes its files in the working directory, read-only for appuser
            filter(None, [config.SPARK_DRIVER_JAVA_OPTIONS, "-Dderby.system.home=/tmp/derby"])
        ),
        "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        # Timestamps are UTC end to end, like the DuckDB branch's DuckDB sessions
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(config.SPARK_THRIFT_CORES),
        "spark.sql.adaptive.enabled": "true",
        **catalog_conf(),
        "spark.sql.defaultCatalog": config.SPARK_CATALOG,
        # insert_overwrite replaces only the partitions present in the output (Gold hours)
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.ui.enabled": "false",
    }


def command(jars: list[str]) -> list[str]:
    """spark-submit arguments of the Thrift server."""
    if not jars:
        raise SystemExit("SPARK_JARS_DIR has no jar: run the server from the dagster-spark image")
    args = [
        "spark-submit",
        "--class",
        THRIFT_SERVER_CLASS,
        "--master",
        f"local[{config.SPARK_THRIFT_CORES}]",
        "--driver-memory",
        config.SPARK_THRIFT_DRIVER_MEMORY,
        "--jars",
        ",".join(jars),
        "--driver-class-path",
        ":".join(jars),
    ]
    for key, value in server_conf().items():
        args += ["--conf", f"{key}={value}"]
    return args + [
        "--hiveconf",
        f"hive.server2.thrift.port={config.SPARK_THRIFT_PORT}",
        "--hiveconf",
        "hive.server2.thrift.bind.host=0.0.0.0",
        # Primary resource of a server bundled with Spark (no application jar)
        "spark-internal",
    ]


def main() -> None:
    args = command(image_jars())
    logger.info(
        f"Starting the Spark Thrift server on port {config.SPARK_THRIFT_PORT} "
        f"(local[{config.SPARK_THRIFT_CORES}], heap {config.SPARK_THRIFT_DRIVER_MEMORY})"
    )
    os.execvp(args[0], args)


if __name__ == "__main__":
    main()
