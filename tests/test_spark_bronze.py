"""Parity of branch A's Spark parsing with branch B's (the shared Bronze contract).

The same raw Kafka messages go through parse_bronze_event (Quix) and parse_raw_events
(Spark); both must keep the same messages with the same values. Needs Java (local
SparkSession, no Kafka nor Iceberg): skipped without it.
"""

import json
import shutil
from datetime import UTC, datetime

import pytest

pyspark = pytest.importorskip("pyspark")
pytestmark = pytest.mark.skipif(shutil.which("java") is None, reason="Java not installed")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.processing.bronze import BRONZE_COLUMN_NAMES, parse_bronze_event  # noqa: E402
from src.processing.spark.stream_job import bronze_ddl, parse_raw_events  # noqa: E402


def _payload(**overrides: object) -> dict:
    payload = {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "cid": "bafyreidin5",
        "collection": "app.bsky.feed.like",
        "did": "did:plc:s75bk",
        "operation": "create",
        "record": {"$type": "app.bsky.feed.like", "subject": {"uri": "at://x", "cid": "b"}},
        "rev": "3mwevbclemo27",
        "rkey": "3mwevbcl4so27",
        "seq": 26330886591,
        "time": "2026-09-25T23:09:22.458527Z",
    }
    payload.update(overrides)
    return payload


def _message(**overrides: object) -> str:
    return json.dumps({"$type": "message", "payload": _payload(**overrides)})


def _without(*keys: str) -> str:
    payload = _payload()
    for key in keys:
        del payload[key]
    return json.dumps({"$type": "message", "payload": payload})


POST = {
    "$type": "app.bsky.feed.post",
    "text": "Été 🌞 à Paris #Bluesky",
    "langs": ["fr"],
    "facets": [{"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "Bluesky"}]}],
    "createdAt": "2026-09-25T23:09:20.000Z",
}

MESSAGES = [
    _message(),
    _message(seq=1, collection="app.bsky.feed.post", record=POST),
    _message(seq=2, operation="update", collection="app.bsky.feed.post", record=POST),
    _without("record", "cid").replace('"operation": "create"', '"operation": "delete"'),
    _message(seq=3, time="2026-09-26T00:00:00Z"),
    _without("rev"),
    # To drop, in both branches
    json.dumps({"$type": "info", "payload": _payload(seq=4)}),
    _without("seq"),
    _without("did"),
    _message(seq=5, time="not a time"),
    "not json at all",
]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _spark_rows(spark: SparkSession) -> list[dict]:
    raw = spark.createDataFrame(
        [(m.encode(), 1, offset) for offset, m in enumerate(MESSAGES)],
        "value BINARY, partition INT, offset BIGINT",
    )
    parsed = parse_raw_events(raw, processed_at=F.lit(None).cast("timestamp"))
    assert tuple(parsed.columns) == BRONZE_COLUMN_NAMES
    return [
        row.asDict()
        for row in parsed.withColumn("event_time_us", F.unix_micros("event_time"))
        .drop("event_time", "processed_at")
        .collect()
    ]


def _python_rows() -> list[dict]:
    rows = []
    for offset, raw in enumerate(MESSAGES):
        try:
            event = parse_bronze_event(json.loads(raw))
        except json.JSONDecodeError:
            event = None
        if event is None:
            continue
        row = {k: getattr(event, k) for k in BRONZE_COLUMN_NAMES if hasattr(event, k)}
        row.pop("event_time")
        row["event_time_us"] = int(event.event_time.astimezone(UTC).timestamp() * 1_000_000)
        row.update(kafka_partition=1, kafka_offset=offset)
        rows.append(row)
    return rows


def _comparable(row: dict) -> dict:
    """`record` compared as JSON: branch B re-serializes with escaped non-ASCII
    characters, Spark keeps them; same document, different text."""
    out = dict(row)
    out["record"] = None if row["record"] is None else json.loads(row["record"])
    return out


def test_spark_keeps_the_same_messages_with_the_same_values(spark) -> None:
    spark_rows = sorted(map(_comparable, _spark_rows(spark)), key=lambda r: r["kafka_offset"])
    python_rows = sorted(map(_comparable, _python_rows()), key=lambda r: r["kafka_offset"])
    assert [r["kafka_offset"] for r in spark_rows] == [0, 1, 2, 3, 4, 5]
    assert spark_rows == python_rows


def test_post_text_survives_unicode(spark) -> None:
    """Accents, emoji and nested facets come out identical once decoded."""
    post = next(r for r in _spark_rows(spark) if r["seq"] == 1)
    assert json.loads(post["record"]) == POST


def test_event_time_is_utc(spark) -> None:
    """Jetstream `time` keeps its microseconds and stays in UTC."""
    first = next(r for r in _spark_rows(spark) if r["seq"] == 26330886591)
    expected = datetime(2026, 9, 25, 23, 9, 22, 458527, tzinfo=UTC)
    assert first["event_time_us"] == int(expected.timestamp() * 1_000_000)


def test_bronze_ddl_matches_the_contract() -> None:
    """Same columns as branch B, hidden day partitioning, zstd."""
    ddl = bronze_ddl("cat.bronze.bronze_events")
    for name in BRONZE_COLUMN_NAMES:
        assert f"\n    {name} " in ddl
    assert "PARTITIONED BY (days(event_time))" in ddl
    assert "'write.parquet.compression-codec' = 'zstd'," in ddl
    assert "'write.metadata.delete-after-commit.enabled' = 'true'" in ddl


# --- Benchmark day (D28) ------------------------------------------------------------


def test_before_end_keeps_only_the_days_offsets(spark) -> None:
    """Exclusive end per partition; a partition missing from the bounds keeps nothing."""
    from src.processing.spark.stream_job import before_end

    rows = [(0, 9), (0, 10), (1, 19), (1, 20), (2, 0)]
    frame = spark.createDataFrame(rows, "partition INT, offset BIGINT")
    kept = frame.where(before_end({0: 10, 1: 20})).collect()
    assert sorted((r["partition"], r["offset"]) for r in kept) == [(0, 9), (1, 19)]


def test_prune_keeps_the_last_day_checkpoints(tmp_path) -> None:
    from src.processing.spark.stream_job import prune_day_checkpoints

    for name in ("2026-10-07", "2026-10-09", "2026-10-11"):
        (tmp_path / name).mkdir()
    assert prune_day_checkpoints(str(tmp_path), keep=2) == ["2026-10-07"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-10-09", "2026-10-11"]
    assert prune_day_checkpoints(str(tmp_path / "missing"), keep=2) == []


def test_committed_position_from_the_progress_json() -> None:
    """Spark's compact progress JSON, where endOffset is a JSON object."""
    from src.processing.spark.stream_job import committed_position

    progress = json.dumps({"sources": [{"endOffset": {"raw_events": {"0": 5, "1": 7}}}]})
    assert committed_position(progress) == {0: 5, 1: 7}
    assert committed_position(json.dumps({"sources": []})) is None
