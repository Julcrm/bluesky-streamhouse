"""
Spark session of branch A (local mode, single node) with the Iceberg REST catalog
(Lakekeeper, decision D26). Shared by the streaming job and the Iceberg maintenance.

Lakekeeper signs the S3 requests of the session (remote signing): no S3 key here.
"""

import glob
import os

from pyspark.sql import SparkSession

from src import config


def build_session(app_name: str, driver_memory: str | None = None) -> SparkSession:
    """Local-mode session with the Lakekeeper catalog as `config.SPARK_CATALOG`.

    `driver_memory` only applies if no JVM runs yet in this process (it sizes the JVM).
    """
    catalog = f"spark.sql.catalog.{config.SPARK_CATALOG}"
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory or config.SPARK_DRIVER_MEMORY)
        .config("spark.driver.extraJavaOptions", config.SPARK_DRIVER_JAVA_OPTIONS)
        # Timestamps are UTC end to end, like branch B's DuckDB sessions
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "rest")
        .config(f"{catalog}.uri", config.ICEBERG_CATALOG_URI)
        .config(f"{catalog}.warehouse", config.ICEBERG_WAREHOUSE)
        # Ask Lakekeeper to sign S3 requests rather than hand out credentials
        .config(f"{catalog}.header.X-Iceberg-Access-Delegation", "remote-signing")
        .config("spark.ui.enabled", "false")
    )
    jars_dir = os.getenv("SPARK_JARS_DIR")
    if jars_dir:  # image: jars resolved at build time into this directory, never at runtime
        jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
        builder = builder.config("spark.jars", ",".join(jars))
    else:  # local run: Spark resolves the pinned packages
        builder = builder.config("spark.jars.packages", ",".join(config.SPARK_PACKAGES))
    return builder.getOrCreate()
