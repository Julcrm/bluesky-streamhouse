"""Tests for the prod docker-compose.yaml: settings local runs cannot catch (their defaults
point at localhost)."""

from pathlib import Path

import pytest
import yaml

COMPOSE = Path(__file__).resolve().parents[1] / "docker-compose.yaml"


def _environment(service: str) -> dict[str, str]:
    entries = yaml.safe_load(COMPOSE.read_text())["services"][service]["environment"]
    return dict(entry.split("=", 1) for entry in entries)


@pytest.mark.parametrize("service", ["bluesky-duckdb", "bluesky-spark"])
def test_code_locations_reach_redpanda(service: str) -> None:
    """Calendar bounds and the controlled test's replay read Kafka: without the variable,
    the Spark bench night of 2026-10-18 failed every run on 127.0.0.1:19092."""
    assert _environment(service)["KAFKA_BOOTSTRAP_SERVERS"] == "redpanda:9092"
