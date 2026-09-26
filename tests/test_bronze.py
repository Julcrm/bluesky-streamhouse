"""Unit tests for src.processing.bronze (shared Bronze contract)."""

from datetime import UTC, datetime

from src.processing.bronze import (
    BRONZE_COLUMN_NAMES,
    BronzeEvent,
    duckdb_ddl,
    parse_bronze_event,
)


def _message(**payload_overrides: object) -> dict:
    """A decoded Jetstream v2 commit message (like create by default)."""
    payload = {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "cid": "bafyreidin5",
        "collection": "app.bsky.feed.like",
        "did": "did:plc:s75bk",
        "operation": "create",
        "record": {"$type": "app.bsky.feed.like", "subject": {"uri": "at://x"}},
        "rev": "3mwevbclemo27",
        "rkey": "3mwevbcl4so27",
        "seq": 26330886591,
        "time": "2026-09-25T23:09:22.458527Z",
    }
    payload.update(payload_overrides)
    return {"$type": "message", "payload": payload}


def test_parse_create_keeps_envelope_and_compact_record() -> None:
    """A create gives typed envelope fields and the record as compact JSON."""
    event = parse_bronze_event(_message())
    assert event == BronzeEvent(
        seq=26330886591,
        did="did:plc:s75bk",
        collection="app.bsky.feed.like",
        operation="create",
        rkey="3mwevbcl4so27",
        rev="3mwevbclemo27",
        cid="bafyreidin5",
        event_time=datetime(2026, 9, 25, 23, 9, 22, 458527, tzinfo=UTC),
        record='{"$type":"app.bsky.feed.like","subject":{"uri":"at://x"}}',
    )


def test_parse_delete_has_no_record_nor_cid() -> None:
    """Deletes carry neither record nor cid: both become NULL."""
    message = _message(operation="delete")
    del message["payload"]["record"], message["payload"]["cid"]
    event = parse_bronze_event(message)
    assert event is not None
    assert event.record is None and event.cid is None


def test_parse_rejects_unusable_messages() -> None:
    """Wrong envelope, missing required fields or bad timestamps are skipped."""
    assert parse_bronze_event({"$type": "info"}) is None
    assert parse_bronze_event({"$type": "message"}) is None
    assert parse_bronze_event(_message(time="not a date")) is None
    bad = _message()
    del bad["payload"]["rkey"]
    assert parse_bronze_event(bad) is None


def test_event_time_is_timezone_aware() -> None:
    """Jetstream times are UTC: the parsed datetime must carry the timezone."""
    event = parse_bronze_event(_message())
    assert event is not None and event.event_time.tzinfo is not None


def test_ddl_lists_every_column_in_order() -> None:
    """The DDL declares all contract columns, in schema order."""
    ddl = duckdb_ddl("lake.main.bronze_events")
    positions = [ddl.index(f"    {name} ") for name in BRONZE_COLUMN_NAMES]
    assert positions == sorted(positions)
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS lake.main.bronze_events")
