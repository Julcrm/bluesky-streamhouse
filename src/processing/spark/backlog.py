"""
Backlog of the Spark branch, as logged by dbt itself: the `log_backlog` macro of the dbt-spark
project (dbt/spark/macros/backlog.sql) writes one `BLUESKY_BACKLOG {json}` line, at the
end of every `dbt run` (on-run-end) or alone (`dbt run-operation measure_backlog`). Counted
inside the Thrift server's Spark session: no Iceberg client in the code server.

Iceberg snapshot ids are random (DuckLake's are a counter): progress is a position that
changed, not one that grew.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass, field

MARKER = "BLUESKY_BACKLOG "


@dataclass(frozen=True)
class SparkBacklog:
    """What one more dbt run would still have to read."""

    silver_rows: int
    gold_hours: int
    # Silver read positions (Bronze snapshot ids) by model
    positions: dict[str, str] = field(default_factory=dict)

    def progressed_from(self, previous: "SparkBacklog") -> bool:
        """True when a pass moved a Silver read position or shrank either backlog."""
        return (
            self.silver_rows < previous.silver_rows
            or self.gold_hours < previous.gold_hours
            or self.positions != previous.positions
        )


def parse_backlog(messages: Iterable[str]) -> SparkBacklog | None:
    """The last backlog line among dbt log messages, None if dbt logged none."""
    found = None
    for message in messages:
        if message.startswith(MARKER):
            found = json.loads(message[len(MARKER) :])
    if found is None:
        return None
    return SparkBacklog(
        silver_rows=int(found["silver_rows"]),
        gold_hours=int(found["gold_hours"]),
        positions=dict(found["positions"]),
    )
