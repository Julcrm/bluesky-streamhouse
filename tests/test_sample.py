"""Tests for src.benchmark.sample (frozen sample of the controlled test, D35)."""

import json
import socket
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import KafkaError, KafkaException

from src import config
from src.benchmark import sample


def _commit(collection: str, operation: str = "create") -> bytes:
    return json.dumps(
        {"kind": "commit", "commit": {"collection": collection, "operation": operation}}
    ).encode()


def test_collection_of_commit_and_others() -> None:
    assert sample.collection_of(_commit("app.bsky.feed.like")) == "app.bsky.feed.like/create"
    assert sample.collection_of(b'{"kind": "identity"}') == "other"
    assert sample.collection_of(b"not json") == "other"
    assert sample.collection_of(None) == "other"


def test_expected_counts_every_partition() -> None:
    bounds = sample.SampleBounds(start={0: 10, 1: 5, 2: 7}, end={0: 15, 1: 5, 2: 9})
    assert bounds.expected == 7


class _Future:
    def __init__(self, error: KafkaError | None) -> None:
        self.error = error

    def result(self) -> None:
        if self.error is not None:
            raise KafkaException(self.error)


class _Admin:
    def __init__(self, error: KafkaError | None) -> None:
        self.error = error

    def create_topics(self, topics):
        return {t.topic: _Future(self.error) for t in topics}


def test_existing_sample_is_never_rewritten() -> None:
    admin = _Admin(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
    with pytest.raises(sample.SampleExistsError):
        sample.create_sample_topic("bench_sample", 3, admin)


def test_schedule_skips_once_the_sample_exists(monkeypatch) -> None:
    from dagster import RunRequest, SkipReason, build_schedule_context

    from src.dagster.sample import sample_schedule

    monkeypatch.setattr(sample, "sample_exists", lambda: True)
    assert isinstance(sample_schedule(build_schedule_context()), SkipReason)
    monkeypatch.setattr(sample, "sample_exists", lambda: False)
    assert isinstance(sample_schedule(build_schedule_context()), RunRequest)


def _redpanda_up() -> bool:
    host, port = config.KAFKA_BOOTSTRAP_SERVERS.split(",")[0].rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _redpanda_up(), reason="local Redpanda not running (make up)")
def test_freeze_copies_the_slice_once() -> None:
    """Only the slice's messages are copied, to the same partition with their timestamp;
    a second freeze is refused."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    suffix = uuid.uuid4().hex[:8]
    source, target = f"test_raw_{suffix}", f"test_sample_{suffix}"
    admin = AdminClient(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, **config.KAFKA_CLIENT_CONFIG}
    )
    for _, f in admin.create_topics(
        [NewTopic(source, num_partitions=config.RAW_EVENTS_PARTITIONS, replication_factor=1)]
    ).items():
        f.result()
    # In the past: offsets are looked up by the messages' own timestamps
    start = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(hours=1)
    producer = Producer(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, **config.KAFKA_CLIENT_CONFIG}
    )
    # One message a minute from 5 min before to 5 min after a 10-minute slice, per partition
    for minute in range(-5, 15):
        ts = int((start + timedelta(minutes=minute)).timestamp() * 1000)
        for partition in range(config.RAW_EVENTS_PARTITIONS):
            producer.produce(
                source,
                key=str(minute).encode(),
                value=_commit("app.bsky.feed.post" if partition else "app.bsky.feed.like"),
                partition=partition,
                timestamp=ts,
            )
    assert producer.flush(10) == 0
    try:
        figures = sample.freeze_sample(start, 10, source, target)
        assert figures["messages"] == 10 * config.RAW_EVENTS_PARTITIONS
        assert figures["mix"] == {"app.bsky.feed.post/create": 20, "app.bsky.feed.like/create": 10}
        copied = sample.sample_bounds(datetime(2000, 1, 1, tzinfo=UTC), 60 * 24 * 365 * 100, target)
        assert copied.start == {0: 0, 1: 0, 2: 0}
        assert copied.end == {0: 10, 1: 10, 2: 10}
        # Original timestamps kept: the slice starts at `start` in the sample too
        assert sample.sample_bounds(start, 10, target).expected == 30
        with pytest.raises(sample.SampleExistsError):
            sample.freeze_sample(start, 10, source, target)
    finally:
        existing = admin.list_topics(timeout=10).topics
        for _, f in admin.delete_topics([t for t in (source, target) if t in existing]).items():
            f.result()
