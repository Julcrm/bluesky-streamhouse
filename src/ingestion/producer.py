"""
Bluesky Jetstream ingestion.
Streams commit events from the Jetstream v2 WebSocket API and publishes them,
unchanged, to the Redpanda `raw_events` topic (shared source of both branches).

Delivery is at-least-once: on restart the stream resumes from the last `seq`
stored in the topic (Jetstream cursors are inclusive), so duplicates are
expected and removed downstream on `seq`.
"""

import asyncio
import json
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode

import websockets
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger

from src import config

# --- Pure helpers (unit-tested) ---


@dataclass(frozen=True)
class RawEvent:
    """One Jetstream message ready to be produced to Kafka."""

    value: bytes  # raw message, unchanged (Bronze fidelity)
    seq: int  # Jetstream sequence number, used as resume cursor and dedup key
    timestamp_ms: int  # event time, used as the Kafka record timestamp


def build_subscribe_url(
    base_url: str,
    collections: tuple[str, ...],
    kinds: tuple[str, ...],
    cursor: int | None = None,
) -> str:
    """Build the Jetstream subscription URL (repeated query parameters)."""
    params: list[tuple[str, str | int]] = [("collections", c) for c in collections]
    params += [("kinds", k) for k in kinds]
    if cursor is not None:
        params.append(("cursor", cursor))
    return f"{base_url}?{urlencode(params)}"


def parse_event(raw: str | bytes) -> RawEvent | None:
    """Extract seq and event time from a raw message; None if unusable."""
    try:
        message = json.loads(raw)
        payload = message["payload"]
        seq = int(payload["seq"])
        event_time = datetime.fromisoformat(payload["time"])
    except (ValueError, KeyError, TypeError):
        return None
    if message.get("$type") != "message":
        return None
    value = raw.encode() if isinstance(raw, str) else raw
    return RawEvent(
        value=value,
        seq=seq,
        timestamp_ms=int(event_time.timestamp() * 1000),
    )


def resume_cursor(
    last_events: list[tuple[int, int]], now_ms: int, max_replay_minutes: int
) -> int | None:
    """
    Choose the Jetstream cursor from the last (seq, timestamp_ms) of each partition.

    Returns the highest seq (inclusive resume), or a unix-microsecond timestamp
    cursor when that event is older than the replay limit, or None (live tail)
    when the topic is empty.
    """
    if not last_events:
        return None
    seq, timestamp_ms = max(last_events)
    oldest_allowed_ms = now_ms - max_replay_minutes * 60 * 1000
    if timestamp_ms < oldest_allowed_ms:
        return oldest_allowed_ms * 1000
    return seq


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0, jitter: float = 0.0) -> float:
    """Exponential reconnection delay, capped, with optional additive jitter."""
    return min(cap, base * 2**attempt) + jitter


# --- Kafka helpers ---


def kafka_base_config(bootstrap_servers: str) -> dict[str, str]:
    """Settings shared by every librdkafka client (admin, consumer, producer)."""
    return {"bootstrap.servers": bootstrap_servers, **config.KAFKA_CLIENT_CONFIG}


def ensure_topic(bootstrap_servers: str) -> None:
    """Create `raw_events` with the configured partitions and retention if missing."""
    admin = AdminClient(kafka_base_config(bootstrap_servers))
    topic = NewTopic(
        config.RAW_EVENTS_TOPIC,
        num_partitions=config.RAW_EVENTS_PARTITIONS,
        replication_factor=1,
        config={
            "retention.ms": str(config.RAW_EVENTS_RETENTION_MS),
            "cleanup.policy": "delete",
            "message.timestamp.type": "CreateTime",
        },
    )
    for name, future in admin.create_topics([topic]).items():
        try:
            future.result()
            logger.info(f"Created topic {name}")
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            logger.info(f"Topic {name} already exists")


def read_last_events(bootstrap_servers: str) -> list[tuple[int, int]]:
    """Read the last message of each partition and return its (seq, timestamp_ms)."""
    consumer = Consumer(
        {
            **kafka_base_config(bootstrap_servers),
            "group.id": "jetstream-producer-resume",
            "enable.auto.commit": False,
        }
    )
    last_events: list[tuple[int, int]] = []
    try:
        for partition in range(config.RAW_EVENTS_PARTITIONS):
            tp = TopicPartition(config.RAW_EVENTS_TOPIC, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            if high <= low:
                continue
            consumer.assign([TopicPartition(config.RAW_EVENTS_TOPIC, partition, high - 1)])
            message = consumer.poll(timeout=10)
            if message is None or message.error():
                continue
            event = parse_event(message.value())
            if event is not None:
                last_events.append((event.seq, event.timestamp_ms))
    finally:
        consumer.close()
    return last_events


def build_producer(bootstrap_servers: str) -> Producer:
    """Idempotent, fully acknowledged, zstd-compressed producer."""
    return Producer(
        {
            **kafka_base_config(bootstrap_servers),
            "client.id": "jetstream-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 50,
            # Messages have no key (per-account ordering is not needed, Silver sorts
            # on time_us/seq): the sticky partitioner fills one partition per linger
            # window, so partitions stay balanced and batches compress well
            "sticky.partitioning.linger.ms": 50,
            # Bound the local queue (default 1 GB): if Redpanda is down, produce()
            # raises BufferError and ingestion slows down instead of exhausting memory
            "queue.buffering.max.kbytes": 65536,
        }
    )


# --- Streaming loop ---


class JetstreamIngestor:
    """Consumes the Jetstream WebSocket and forwards every event to Redpanda."""

    def __init__(self, producer: Producer, cursor: int | None) -> None:
        self.producer = producer
        self.cursor = cursor
        self.last_acked_seq: int | None = None
        self.delivery_errors = 0
        self.session_received = False
        self.stop = asyncio.Event()
        self._reset_stats()

    def _reset_stats(self) -> None:
        self.stats_started = time.monotonic()
        self.stats_messages = 0
        self.stats_bytes = 0
        self.stats_lag_s = 0.0

    def _on_delivery(self, err: KafkaError | None, msg) -> None:
        """Track the highest acknowledged seq: it is the safe resume point."""
        if err is not None:
            self.delivery_errors += 1
            logger.error(f"Delivery failed: {err}")
            return
        event = parse_event(msg.value())
        if event and (self.last_acked_seq is None or event.seq > self.last_acked_seq):
            self.last_acked_seq = event.seq

    def _produce(self, event: RawEvent) -> None:
        """Produce one event, waiting for queue space instead of dropping it."""
        while True:
            try:
                self.producer.produce(
                    config.RAW_EVENTS_TOPIC,
                    value=event.value,
                    timestamp=event.timestamp_ms,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.producer.poll(0.1)
        self.producer.poll(0)

    def _log_stats(self) -> None:
        elapsed = time.monotonic() - self.stats_started
        if elapsed < config.PRODUCER_STATS_INTERVAL_SECONDS:
            return
        rate = self.stats_messages / elapsed
        logger.info(
            f"{rate:.0f} msg/s | {self.stats_bytes / elapsed / 1024:.0f} KiB/s | "
            f"lag {self.stats_lag_s:.1f}s | last acked seq {self.last_acked_seq} | "
            f"delivery errors {self.delivery_errors}"
        )
        self._reset_stats()

    async def _stream_once(self) -> bool:
        """One WebSocket session: stream until disconnect or stop.

        Returns True if at least one event was received, so the caller can reset
        its backoff after a healthy session. A silent network cut is detected by
        the keepalive ping within ~ping_interval + ping_timeout (20s).
        """
        cursor = self.last_acked_seq if self.last_acked_seq is not None else self.cursor
        url = build_subscribe_url(
            config.JETSTREAM_URL, config.JETSTREAM_COLLECTIONS, config.JETSTREAM_KINDS, cursor
        )
        logger.info(f"Connecting to Jetstream (cursor={cursor})")
        self.session_received = False
        async with websockets.connect(
            url,
            max_size=2**22,
            open_timeout=15,
            close_timeout=2,
            ping_interval=10,
            ping_timeout=10,
        ) as ws:
            async for raw in ws:
                event = parse_event(raw)
                if event is None:
                    continue
                self.session_received = True
                self._produce(event)
                self.stats_messages += 1
                self.stats_bytes += len(event.value)
                self.stats_lag_s = time.time() - event.timestamp_ms / 1000
                self._log_stats()
                if self.stop.is_set():
                    return self.session_received
        return self.session_received

    async def run(self) -> None:
        """Stream forever, reconnecting with exponential backoff."""
        attempt = 0
        while not self.stop.is_set():
            try:
                await self._stream_once()
                attempt = 0
            except (OSError, websockets.WebSocketException) as e:
                # A session that delivered events was healthy: restart the backoff
                if self.session_received:
                    attempt = 0
                # Process pending delivery reports so the resume cursor is exact
                self.producer.flush(10)
                delay = backoff_delay(attempt, jitter=random.uniform(0, 1))
                logger.warning(f"Jetstream connection lost ({e!r}), retrying in {delay:.1f}s")
                attempt += 1
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=delay)
                except TimeoutError:
                    pass


def main() -> None:
    """Entry point: prepare the topic, resume from the last seq, then stream."""
    bootstrap = config.KAFKA_BOOTSTRAP_SERVERS
    ensure_topic(bootstrap)

    cursor = resume_cursor(
        read_last_events(bootstrap), int(time.time() * 1000), config.JETSTREAM_MAX_REPLAY_MINUTES
    )
    if cursor is not None and cursor > 10**14:
        logger.warning(
            f"Last event older than {config.JETSTREAM_MAX_REPLAY_MINUTES} min: "
            "replay capped, events in between are skipped"
        )

    producer = build_producer(bootstrap)
    ingestor = JetstreamIngestor(producer, cursor)

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, ingestor.stop.set)
        await ingestor.run()

    try:
        asyncio.run(_run())
    finally:
        remaining = producer.flush(30)
        logger.info(f"Producer flushed ({remaining} messages not delivered)")


if __name__ == "__main__":
    main()
