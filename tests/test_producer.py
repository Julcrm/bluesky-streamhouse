"""Unit tests for src.ingestion.producer (pure helpers, no network)."""

import json
from urllib.parse import parse_qs, urlsplit

from src.ingestion.producer import backoff_delay, build_subscribe_url, parse_event, resume_cursor

BASE = "wss://jetstream.example/xrpc/network.bsky.jetstream.subscribeEvents"


def _commit(seq: int = 42, did: str = "did:plc:abc", time: str = "2026-09-24T15:46:10.5Z") -> str:
    """Build a raw Jetstream v2 commit message."""
    return json.dumps(
        {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#commit",
                "did": did,
                "seq": seq,
                "time": time,
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3mwblzsc2xd22",
                "record": {"text": "hello"},
            },
        }
    )


def test_subscribe_url_repeats_filters_and_adds_cursor() -> None:
    """Collections and kinds are repeated query parameters; cursor is optional."""
    url = build_subscribe_url(BASE, ("app.bsky.feed.post", "app.bsky.feed.like"), ("commit",), 7)
    query = parse_qs(urlsplit(url).query)
    assert query["collections"] == ["app.bsky.feed.post", "app.bsky.feed.like"]
    assert query["kinds"] == ["commit"]
    assert query["cursor"] == ["7"]


def test_subscribe_url_without_cursor_is_live_tail() -> None:
    """No cursor means the live tail."""
    assert "cursor" not in build_subscribe_url(BASE, ("app.bsky.feed.post",), ("commit",))


def test_parse_event_extracts_key_seq_and_time() -> None:
    """A commit gives the DID as key, its seq, and the event time in ms."""
    raw = _commit(seq=26281967014, did="did:plc:hthoz")
    event = parse_event(raw)
    assert event is not None
    assert event.key == b"did:plc:hthoz"
    assert event.seq == 26281967014
    assert event.timestamp_ms == 1790264770500
    assert event.value == raw.encode()


def test_parse_event_rejects_unusable_messages() -> None:
    """Invalid JSON, missing fields or non-message envelopes are skipped."""
    assert parse_event("not json") is None
    assert parse_event(json.dumps({"$type": "message", "payload": {"did": "x"}})) is None
    assert parse_event(_commit().replace('"$type": "message"', '"$type": "info"')) is None


def test_resume_cursor_empty_topic_starts_live() -> None:
    """Nothing in the topic: live tail."""
    assert resume_cursor([], now_ms=1_000_000, max_replay_minutes=60) is None


def test_resume_cursor_uses_highest_seq_across_partitions() -> None:
    """Recent events: resume from the highest seq (inclusive cursor)."""
    now = 1_790_000_000_000
    last = [(100, now - 5_000), (250, now - 1_000), (180, now - 3_000)]
    assert resume_cursor(last, now_ms=now, max_replay_minutes=60) == 250


def test_resume_cursor_caps_replay_with_timestamp_cursor() -> None:
    """Too old: cap the replay with a unix-microsecond timestamp cursor."""
    now = 1_790_000_000_000
    last = [(250, now - 2 * 60 * 60 * 1000)]
    assert resume_cursor(last, now_ms=now, max_replay_minutes=60) == (now - 60 * 60 * 1000) * 1000


def test_backoff_delay_grows_and_is_capped() -> None:
    """1s, 2s, 4s... never above the cap."""
    assert [backoff_delay(a) for a in range(4)] == [1, 2, 4, 8]
    assert backoff_delay(20) == 60
    assert backoff_delay(0, jitter=0.5) == 1.5
