"""
Replay of the frozen sample for the controlled test (decisions D23, D36).

Each run starts from an empty replay topic (`reset_bench_topic`), then `replay` sends the
whole sample to it:
- at a **uniform** rate, `factor` times the sample's mean rate (1x, 4x): the same, known
  rate every week and for both branches, so the per-message costs are compared at an
  exact throughput (the live alternation already measures real bursts); the sample's own
  timestamps are event times, not monotonic, so they are not used for pacing;
- or as fast as possible (`factor=None`, "max"), before the engine starts: a pure
  catch-up.

Every message keeps its key, value, headers and partition; its Kafka timestamp is the
send time, so the latency of a run is `processed_at` minus that time, the same way for
both branches. Messages are streamed, never all held in memory.

Kept free of Dagster imports: the controlled-test job only calls these functions.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from src import config
from src.alternation import calendar as cal
from src.benchmark.sample import MAX_EMPTY_POLLS, POLL_TIMEOUT_SECONDS
from src.resources import redpanda

# Replay factor of each rate of the controlled test; None: as fast as possible
RATE_FACTORS: dict[str, float | None] = {"1x": 1.0, "4x": 4.0, "max": None}
# A deleted topic takes a moment to disappear before it can be created again
TOPIC_RESET_TIMEOUT_SECONDS = 30.0
# The replay topic only has to outlive its run's measures (latency is read from it)
BENCH_TOPIC_RETENTION_MS = 3 * 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class ReplayResult:
    """What a replay sent: its end offsets (exclusive) feed bench_runs."""

    messages: int
    seconds: float
    end_offsets: cal.Offsets

    @property
    def rate(self) -> float:
        return self.messages / self.seconds if self.seconds else 0.0


def _admin() -> AdminClient:
    return AdminClient(
        {"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, **config.KAFKA_CLIENT_CONFIG}
    )


def reset_bench_topic(
    topic: str = config.BENCH_TOPIC,
    partitions: int = config.RAW_EVENTS_PARTITIONS,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Delete the replay topic if it exists, then create it empty: every run reads its
    messages from offset 0."""
    admin = _admin()
    if topic in admin.list_topics(timeout=redpanda.LOOKUP_TIMEOUT_SECONDS).topics:
        for _, future in admin.delete_topics([topic]).items():
            future.result()
    new = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=1,
        config={
            "retention.ms": str(BENCH_TOPIC_RETENTION_MS),
            "cleanup.policy": "delete",
            "message.timestamp.type": "CreateTime",
        },
    )
    deadline = time.monotonic() + TOPIC_RESET_TIMEOUT_SECONDS
    while True:
        try:
            for _, future in admin.create_topics([new]).items():
                future.result()
            return
        except KafkaException as e:
            # The deletion is still in progress: try again shortly
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS or time.monotonic() > deadline:
                raise
            sleep(0.5)


def sample_rate(messages: int, minutes: int = config.BENCH_SAMPLE_MINUTES) -> float:
    """Mean rate of the sample, msg/s: the 1x of the controlled test."""
    return messages / (minutes * 60)


def wait_time(sent: int, started: float, rate: float | None, now: float) -> float:
    """Seconds to wait before sending message number `sent` (0-based) so that the
    replay keeps a uniform `rate` from `started`; 0 when late or without a rate."""
    if rate is None:
        return 0.0
    return max(0.0, started + sent / rate - now)


def replay(
    factor: float | None,
    source: str = config.BENCH_SAMPLE_TOPIC,
    target: str = config.BENCH_TOPIC,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayResult:
    """Send the whole sample from `source` to `target`, each message to its partition,
    at `factor` times the sample's mean rate (None: as fast as possible)."""
    bounds = {
        p: (low, high) for p, (low, high) in redpanda.watermarks(source).items() if high > low
    }
    total = sum(high - low for low, high in bounds.values())
    if total == 0:
        raise RuntimeError(f"The sample {source} is empty")
    rate = None if factor is None else factor * sample_rate(total)
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "bluesky-bench-replay",
            "enable.auto.commit": False,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            # Retries never reorder nor duplicate a partition's messages
            "enable.idempotence": True,
            "linger.ms": 5,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    pending = set(bounds)
    sent = 0
    started = clock()
    try:
        consumer.assign([TopicPartition(source, p, bounds[p][0]) for p in sorted(pending)])
        empty_polls = 0
        while pending:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    raise RuntimeError(f"Sample read stalled on partitions {sorted(pending)}")
                continue
            empty_polls = 0
            if message.error():
                raise KafkaException(message.error())
            partition = message.partition()
            if partition not in pending or message.offset() >= bounds[partition][1]:
                pending.discard(partition)
                continue
            delay = wait_time(sent, started, rate, clock())
            if delay > 0:
                sleep(delay)
            # No timestamp: the broker keeps the producer's send time (CreateTime)
            producer.produce(
                target,
                key=message.key(),
                value=message.value(),
                partition=partition,
                headers=message.headers(),
            )
            producer.poll(0)
            sent += 1
            if message.offset() + 1 >= bounds[partition][1]:
                pending.discard(partition)
        unsent = producer.flush(60)
        if unsent:
            raise RuntimeError(f"{unsent} messages not delivered to {target}")
    finally:
        consumer.close()
    seconds = clock() - started
    if sent != total:
        raise RuntimeError(f"Replayed {sent} messages, expected {total}")
    return ReplayResult(
        messages=sent, seconds=seconds, end_offsets=redpanda.high_watermarks(target)
    )
