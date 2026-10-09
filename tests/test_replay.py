"""Tests for src.benchmark.replay (controlled test, D36)."""

import time
import uuid

import pytest

from src import config
from src.benchmark import replay
from tests.test_sample import _redpanda_up


def test_wait_keeps_a_uniform_rate() -> None:
    assert replay.wait_time(0, 100.0, 10.0, 100.0) == 0.0
    assert replay.wait_time(5, 100.0, 10.0, 100.2) == pytest.approx(0.3)
    assert replay.wait_time(5, 100.0, 10.0, 101.0) == 0.0  # late: no wait
    assert replay.wait_time(10**6, 100.0, None, 100.0) == 0.0  # max: never waits


def test_sample_rate_is_the_mean_rate() -> None:
    assert replay.sample_rate(480_000, 20) == 400.0
    assert replay.RATE_FACTORS == {"1x": 1.0, "4x": 4.0, "max": None}


@pytest.fixture
def topics():
    """A small sample on 3 partitions and a replay topic, deleted afterwards."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    if not _redpanda_up():
        pytest.skip("local Redpanda not running (make up)")
    suffix = uuid.uuid4().hex[:8]
    source, target = f"test_sample_{suffix}", f"test_bench_{suffix}"
    admin = AdminClient(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, **config.KAFKA_CLIENT_CONFIG}
    )
    for _, f in admin.create_topics(
        [NewTopic(source, num_partitions=config.RAW_EVENTS_PARTITIONS, replication_factor=1)]
    ).items():
        f.result()
    producer = Producer(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, **config.KAFKA_CLIENT_CONFIG}
    )
    old = int((time.time() - 3600) * 1000)
    for i in range(30):
        producer.produce(
            source, key=f"k{i}".encode(), value=f"v{i}".encode(), partition=i % 3, timestamp=old
        )
    assert producer.flush(10) == 0
    yield source, target
    existing = admin.list_topics(timeout=10).topics
    for _, f in admin.delete_topics([t for t in (source, target) if t in existing]).items():
        f.result()


def _read(topic: str) -> list:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "test-replay-read",
            "enable.auto.commit": False,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    consumer.assign([TopicPartition(topic, p, 0) for p in range(config.RAW_EVENTS_PARTITIONS)])
    messages = []
    for _ in range(50):
        message = consumer.poll(1.0)
        if message is None:
            break
        messages.append(message)
    consumer.close()
    return messages


def test_max_replay_copies_the_sample_to_the_same_partitions(topics) -> None:
    """Every message, in its partition and in order, stamped with its send time; the
    reset leaves an empty topic each time."""
    source, target = topics
    before = time.time() * 1000
    for _ in range(2):  # a second run starts again from an empty topic
        replay.reset_bench_topic(target)
        result = replay.replay(None, source, target)
    assert result.messages == 30
    assert result.end_offsets == {0: 10, 1: 10, 2: 10}
    copied = _read(target)
    assert len(copied) == 30
    for m in copied:
        assert int(m.key().decode()[1:]) % 3 == m.partition()
        assert m.timestamp()[1] >= before
    by_partition = {p: [m.key() for m in copied if m.partition() == p] for p in range(3)}
    assert by_partition[0] == [f"k{i}".encode() for i in range(0, 30, 3)]


def test_paced_replay_keeps_the_target_rate(topics) -> None:
    """At 1x the 30 messages of a 20-minute sample take ~20 minutes: a fake clock
    advanced by the waits shows it without waiting."""
    source, target = topics
    replay.reset_bench_topic(target)
    now = [0.0]
    waits = []

    def sleep(seconds: float) -> None:
        waits.append(seconds)
        now[0] += seconds

    result = replay.replay(1.0, source, target, clock=lambda: now[0], sleep=sleep)
    rate = replay.sample_rate(30)
    assert result.messages == 30
    assert sum(waits) == pytest.approx(29 / rate, rel=0.01)
