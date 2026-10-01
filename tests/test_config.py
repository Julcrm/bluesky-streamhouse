"""Unit tests for src.config."""

import importlib

import src.config


def test_collections_match_decision_d2() -> None:
    """The four Jetstream collections chosen in D2 are ingested."""
    assert set(src.config.JETSTREAM_COLLECTIONS) == {
        "app.bsky.feed.post",
        "app.bsky.feed.like",
        "app.bsky.feed.repost",
        "app.bsky.graph.follow",
    }


def test_retention_is_24_hours() -> None:
    """raw_events keeps exactly 24 hours of data (D1: no replay)."""
    assert src.config.RAW_EVENTS_RETENTION_MS == 86_400_000


def test_storage_paths_follow_bucket(monkeypatch) -> None:
    """DuckLake data paths are derived from BUCKET (Iceberg's are set in Lakekeeper)."""
    monkeypatch.setenv("BUCKET", "test-bucket")
    config = importlib.reload(src.config)
    try:
        assert config.DUCKLAKE_BRONZE_DATA_PATH == "s3://test-bucket/ducklake/bronze/"
        assert config.DUCKLAKE_TRANSFORM_DATA_PATH == "s3://test-bucket/ducklake/transform/"
    finally:
        monkeypatch.delenv("BUCKET")
        importlib.reload(src.config)
