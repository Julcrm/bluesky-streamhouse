"""
Bronze contract shared by both branches: one row per Jetstream commit event.

Branch A (Spark → Iceberg) and branch B (Quix → DuckLake) must write the exact same
columns, so the schema lives here once. Bronze is append-only: duplicates from producer
resumes or consumer replays are expected and removed in Silver on `seq`.

Collection-specific fields (post text, langs, like subject...) stay in `record` (raw
JSON) and are typed in Silver.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

BRONZE_TABLE = "bronze_events"

# (column, DuckDB type, Spark type) — order is the physical column order
BRONZE_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("seq", "BIGINT", "BIGINT"),  # Jetstream sequence: dedup key in Silver
    ("did", "VARCHAR", "STRING"),  # author account
    ("collection", "VARCHAR", "STRING"),  # e.g. app.bsky.feed.post
    ("operation", "VARCHAR", "STRING"),  # create | update | delete
    ("rkey", "VARCHAR", "STRING"),  # record key within the collection
    ("rev", "VARCHAR", "STRING"),  # repo revision
    ("cid", "VARCHAR", "STRING"),  # content id, NULL on delete
    ("event_time", "TIMESTAMPTZ", "TIMESTAMP"),  # Jetstream `time` (UTC)
    ("record", "VARCHAR", "STRING"),  # raw record JSON, NULL on delete
    ("kafka_partition", "INTEGER", "INT"),
    ("kafka_offset", "BIGINT", "BIGINT"),
    ("processed_at", "TIMESTAMPTZ", "TIMESTAMP"),  # write time: benchmark windows
)

BRONZE_COLUMN_NAMES = tuple(name for name, _, _ in BRONZE_COLUMNS)


@dataclass(frozen=True)
class BronzeEvent:
    """Envelope fields parsed from a Jetstream message (Kafka metadata added at write)."""

    seq: int
    did: str
    collection: str
    operation: str
    rkey: str
    rev: str | None
    cid: str | None
    event_time: datetime
    record: str | None


def duckdb_ddl(table: str) -> str:
    """CREATE TABLE statement for the Bronze table in DuckDB/DuckLake."""
    columns = ",\n    ".join(f"{name} {duck}" for name, duck, _ in BRONZE_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n    {columns}\n)"


def parse_bronze_event(message: dict[str, Any]) -> BronzeEvent | None:
    """Extract the Bronze envelope from a decoded Jetstream message; None if unusable."""
    if not isinstance(message, dict) or message.get("$type") != "message":
        return None
    payload = message.get("payload")
    try:
        record = payload.get("record")
        return BronzeEvent(
            seq=int(payload["seq"]),
            did=payload["did"],
            collection=payload["collection"],
            operation=payload["operation"],
            rkey=payload["rkey"],
            rev=payload.get("rev"),
            cid=payload.get("cid"),
            event_time=datetime.fromisoformat(payload["time"]),
            # Compact re-serialization: key order is preserved, only spacing changes
            record=None if record is None else json.dumps(record, separators=(",", ":")),
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return None
