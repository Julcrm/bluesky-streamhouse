"""
Kafka offset lookups and commits on Redpanda `raw_events`, for the alternation (D28).

Kept free of Dagster imports: the calendar jobs use it in the code server, the Quix
engine before it starts. Every function opens a short-lived consumer that never
subscribes, so it never joins a consumer group or triggers a rebalance.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition

from src import config

Offsets = dict[int, int]

LOOKUP_TIMEOUT_SECONDS = 30.0


@contextmanager
def _consumer(group_id: str = "bluesky-offset-lookup") -> Iterator[Consumer]:
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "enable.auto.commit": False,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    try:
        yield consumer
    finally:
        consumer.close()


def _partitions(topic: str, offsets: Offsets | None = None) -> list[TopicPartition]:
    if offsets is not None:
        return [TopicPartition(topic, p, o) for p, o in sorted(offsets.items())]
    return [TopicPartition(topic, p) for p in range(config.RAW_EVENTS_PARTITIONS)]


def high_watermarks(topic: str = config.RAW_EVENTS_TOPIC) -> Offsets:
    """Next offset to be written on each partition: the exclusive end of everything
    produced so far (the 19:00 bound of a day)."""
    with _consumer() as consumer:
        return {
            tp.partition: consumer.get_watermark_offsets(
                tp, timeout=LOOKUP_TIMEOUT_SECONDS, cached=False
            )[1]
            for tp in _partitions(topic)
        }


def offsets_at(moment: datetime, topic: str = config.RAW_EVENTS_TOPIC) -> Offsets:
    """First offset of each partition whose timestamp is at or after `moment`; the high
    watermark when there is none yet. Fallback only: the producer stamps messages with
    the event time, which is not monotonic after a Jetstream replay."""
    timestamp_ms = int(moment.timestamp() * 1000)
    with _consumer() as consumer:
        found = consumer.offsets_for_times(
            [TopicPartition(topic, p, timestamp_ms) for p in range(config.RAW_EVENTS_PARTITIONS)],
            timeout=LOOKUP_TIMEOUT_SECONDS,
        )
        offsets = {}
        for tp in found:
            if tp.offset < 0:  # nothing at or after the time: everything is before
                tp.offset = consumer.get_watermark_offsets(
                    tp, timeout=LOOKUP_TIMEOUT_SECONDS, cached=False
                )[1]
            offsets[tp.partition] = tp.offset
        return offsets


def committed_offsets(group_id: str, topic: str = config.RAW_EVENTS_TOPIC) -> Offsets:
    """Next offset the group will read on each partition (-1001 when none committed)."""
    with _consumer(group_id) as consumer:
        return {
            tp.partition: tp.offset
            for tp in consumer.committed(_partitions(topic), timeout=LOOKUP_TIMEOUT_SECONDS)
        }


def commit_offsets(group_id: str, offsets: Offsets, topic: str = config.RAW_EVENTS_TOPIC) -> None:
    """Set where `group_id` starts reading: only while no member of the group runs (the
    engine is stopped), otherwise its next commit overwrites these offsets."""
    with _consumer(group_id) as consumer:
        consumer.commit(offsets=_partitions(topic, offsets), asynchronous=False)
