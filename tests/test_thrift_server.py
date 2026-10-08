"""Tests for src.processing.spark.thrift_server."""

import pytest

from src import config
from src.processing.spark.thrift_server import THRIFT_SERVER_CLASS, command

JARS = ["/opt/spark-jars/iceberg.jar", "/opt/spark-jars/aws.jar"]


def _conf(args: list[str]) -> dict[str, str]:
    pairs = [args[i + 1] for i, arg in enumerate(args) if arg == "--conf"]
    return dict(pair.split("=", 1) for pair in pairs)


def test_jars_on_the_classpath_and_the_driver_class_path() -> None:
    """--jars alone leaves some server sessions without SparkCatalog (2026-10-07)."""
    args = command(JARS)
    assert args[args.index("--class") + 1] == THRIFT_SERVER_CLASS
    assert args[args.index("--jars") + 1] == ",".join(JARS)
    assert args[args.index("--driver-class-path") + 1] == ":".join(JARS)
    assert args[-1] == "spark-internal"


def test_sessions_get_the_dbt_settings() -> None:
    """Same cores and shuffle partitions as the DuckDB branch's threads, Iceberg as default."""
    args = command(JARS)
    conf = _conf(args)
    assert args[args.index("--master") + 1] == f"local[{config.SPARK_THRIFT_CORES}]"
    assert conf["spark.sql.shuffle.partitions"] == str(config.SPARK_THRIFT_CORES)
    assert conf["spark.sql.defaultCatalog"] == config.SPARK_CATALOG
    assert conf["spark.sql.sources.partitionOverwriteMode"] == "dynamic"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert "-Dderby.system.home=/tmp/derby" in conf["spark.driver.extraJavaOptions"]
    assert f"hive.server2.thrift.port={config.SPARK_THRIFT_PORT}" in args


def test_refuses_to_start_without_jars() -> None:
    with pytest.raises(SystemExit):
        command([])
