"""
Frozen sample of the controlled test (decision D35): ~20 minutes of `raw_events` copied
once into a topic kept forever, then never modified. The weekly replay reads it at a
target rate, the same messages for both branches.

The slice is taken by Kafka timestamp (the producer stamps the event time) on each
partition, and copied to the same partition with its key, value, timestamp and headers,
so the replay sees the order and partitioning of the live topic.

Kept free of Dagster imports: the asset only calls `freeze_sample`.
"""

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src import config
from src.resources import redpanda

POLL_TIMEOUT_SECONDS = 5.0
# Polls returning nothing in a row before giving up: the slice is in the past, every
# message of it is already in the topic
MAX_EMPTY_POLLS = 6


class SampleExistsError(RuntimeError):
    """The sample topic already exists: the frozen sample is never rewritten."""


@dataclass(frozen=True)
class SampleBounds:
    """Offsets of the slice on each partition: start inclusive, end exclusive."""

    start: redpanda.Offsets
    end: redpanda.Offsets

    @property
    def expected(self) -> int:
        return sum(self.end[p] - self.start[p] for p in self.start)


def sample_bounds(start: datetime, minutes: int, topic: str) -> SampleBounds:
    """Offsets of [start, start + minutes) on each partition of `topic`."""
    return SampleBounds(
        start=redpanda.offsets_at(start, topic),
        end=redpanda.offsets_at(start + timedelta(minutes=minutes), topic),
    )


def collection_of(value: bytes | None) -> str:
    """`collection/operation` of a Jetstream commit message, `other` otherwise."""
    try:
        commit = json.loads(value or b"{}").get("commit") or {}
    except ValueError:
        return "other"
    if not commit.get("collection"):
        return "other"
    return f"{commit['collection']}/{commit.get('operation', '?')}"


def _admin() -> AdminClient:
    return AdminClient(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, **config.KAFKA_CLIENT_CONFIG}
    )


def sample_exists(topic: str = config.BENCH_SAMPLE_TOPIC) -> bool:
    """True once the sample topic was created (whatever it holds)."""
    return topic in _admin().list_topics(timeout=redpanda.LOOKUP_TIMEOUT_SECONDS).topics


def create_sample_topic(topic: str, partitions: int, admin: AdminClient | None = None) -> None:
    """Create the sample topic, kept forever; refuse if it already exists."""
    admin = admin or _admin()
    new = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=1,
        config={
            "retention.ms": "-1",
            "retention.bytes": "-1",
            "cleanup.policy": "delete",
            "message.timestamp.type": "CreateTime",
        },
    )
    for _, future in admin.create_topics([new]).items():
        try:
            future.result()
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                raise SampleExistsError(f"Topic {topic} already exists: sample kept as is") from e
            raise


def copy_slice(bounds: SampleBounds, source: str, target: str) -> Counter:
    """Copy every message of the slice to the same partition of `target`; the mix of
    collections copied. Fails if the slice is not complete."""
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "bluesky-bench-sample",
            "enable.auto.commit": False,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            # Retries never reorder nor duplicate a partition's messages
            "enable.idempotence": True,
            "linger.ms": 50,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    pending = {p for p in bounds.start if bounds.end[p] > bounds.start[p]}
    mix: Counter = Counter()
    copied = 0
    try:
        consumer.assign([TopicPartition(source, p, bounds.start[p]) for p in sorted(pending)])
        empty_polls = 0
        while pending:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    raise RuntimeError(f"Slice incomplete: partitions {sorted(pending)} stalled")
                continue
            empty_polls = 0
            if message.error():
                raise KafkaException(message.error())
            partition = message.partition()
            if partition not in pending or message.offset() >= bounds.end[partition]:
                pending.discard(partition)
                continue
            _, timestamp = message.timestamp()
            producer.produce(
                target,
                key=message.key(),
                value=message.value(),
                partition=partition,
                timestamp=timestamp,
                headers=message.headers(),
            )
            producer.poll(0)
            mix[collection_of(message.value())] += 1
            copied += 1
            if message.offset() + 1 >= bounds.end[partition]:
                pending.discard(partition)
        unsent = producer.flush(60)
        if unsent:
            raise RuntimeError(f"{unsent} messages not delivered to {target}")
    finally:
        consumer.close()
    if copied != bounds.expected:
        raise RuntimeError(f"Copied {copied} messages, expected {bounds.expected}")
    return mix


def freeze_sample(
    start: datetime,
    minutes: int,
    source: str = config.RAW_EVENTS_TOPIC,
    target: str = config.BENCH_SAMPLE_TOPIC,
) -> dict:
    """Copy [start, start + minutes) of `source` into a new `target` kept forever; the
    figures of the sample. Refuses to touch an existing sample."""
    bounds = sample_bounds(start, minutes, source)
    if bounds.expected <= 0:
        raise RuntimeError(f"No message in {source} from {start} for {minutes} min")
    create_sample_topic(target, config.RAW_EVENTS_PARTITIONS)
    mix = copy_slice(bounds, source, target)
    return {
        "start": start.isoformat(),
        "minutes": minutes,
        "messages": bounds.expected,
        "start_offsets": bounds.start,
        "end_offsets": bounds.end,
        "mix": dict(mix.most_common()),
    }
