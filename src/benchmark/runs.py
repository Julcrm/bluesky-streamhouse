"""
Runs of the controlled test (decision D36): one row per branch, night, rate and
repetition in `bench_runs`, the request and the record of each replay of the frozen
sample. Kept free of Dagster imports: the engine containers read it too.

Same split of roles as the calendar (D28):
- Dagster requests a run (status `requested`), and records its outcome;
- the supervisor of the branch's engine container only reads it: it starts the engine
  in bench mode (BENCH_RUN=<run id>) for a requested or running run, never while a day
  of its branch is active, and stops it once the run is no longer active;
- the replay records the end offsets of what it sent to the bench topic;
- the engine records its start and, once its committed position reaches the end
  offsets on every partition, `ingested`. Dagster then runs Silver/Gold on the bench
  tables, measures, and closes the run.

Status of a run:
    requested  the engine may start
    running    the engine started (its read position was reset to the topic's start)
    ingested   every replayed message is in the bench Bronze table: the engine stops
    done       measured and closed by Dagster
    failed     stopped on an error, or past the night's deadline (incomplete)
"""

import json
import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime

import psycopg2.extras
from loguru import logger

from src import config
from src.alternation import calendar as cal

RUN_ENV = "BENCH_RUN"

REQUESTED, RUNNING, INGESTED = "requested", "running", "ingested"
DONE, FAILED = "done", "failed"
# A run in these statuses may have an engine running: never two at a time per branch
ACTIVE_STATUSES = (REQUESTED, RUNNING)
RATES = ("1x", "4x", "max")
MEASURE_COLUMNS = (
    "bronze_messages",
    "first_processed_at",
    "last_processed_at",
    "throughput_msg_s",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_relevant",
    "ingest_usage",
    "transform_usage",
)

DDL = f"""
CREATE TABLE IF NOT EXISTS bench_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    branch          TEXT NOT NULL CHECK (branch IN ('{cal.BRANCH_SPARK}', '{cal.BRANCH_DUCKDB}')),
    night           DATE NOT NULL,
    rate            TEXT NOT NULL CHECK (rate IN ({", ".join(f"'{r}'" for r in RATES)})),
    repetition      INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN (
                        '{REQUESTED}', '{RUNNING}', '{INGESTED}', '{DONE}', '{FAILED}')),
    -- End offsets of the bench topic, recorded by the replay once everything is sent
    end_offsets     JSONB,
    requested_at    TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    -- Phases, for the measures read from the collector's samples (D36): the replay
    -- sends from replay_started_at (1x, 4x: the engine already runs; max: before it
    -- starts), Silver/Gold run between the transform timestamps
    replay_started_at       TIMESTAMPTZ,
    replay_finished_at      TIMESTAMPTZ,
    replay_messages         BIGINT,
    transform_started_at    TIMESTAMPTZ,
    transform_finished_at   TIMESTAMPTZ,
    transform_passes        INTEGER,
    -- Measures (src/benchmark/run_measures.py): from the bench Bronze, then per phase
    -- and per layer from the collector's samples; ratios in the view bench_ratios
    bronze_messages         BIGINT,
    first_processed_at      TIMESTAMPTZ,
    last_processed_at       TIMESTAMPTZ,
    throughput_msg_s        DOUBLE PRECISION,
    latency_p50_ms          DOUBLE PRECISION,
    latency_p95_ms          DOUBLE PRECISION,
    latency_relevant        BOOLEAN,
    ingest_usage            JSONB,
    transform_usage         JSONB,
    note            TEXT,
    UNIQUE (branch, night, rate, repetition)
)
"""

# Per-message ratios of the measured runs: ingestion costs of the streaming engine
# (the code server replays during that phase, so it is left out), Silver/Gold costs of
# the transform and catalog layers. A formula changes here without recomputing anything
VIEW = f"""
CREATE OR REPLACE VIEW bench_ratios AS
WITH usage AS (
    SELECT r.*,
           (r.ingest_usage -> 'streaming' ->> 'cpu_ms')::float AS ingest_cpu_ms,
           (r.ingest_usage -> 'streaming' ->> 'ram_mib_s')::float AS ingest_ram_mib_s,
           (r.ingest_usage -> 'streaming' ->> 'peak_anon_mib')::float AS ingest_peak_mib,
           coalesce((r.transform_usage -> 'transform' ->> 'cpu_ms')::float, 0)
             + coalesce((r.transform_usage -> 'catalog' ->> 'cpu_ms')::float, 0)
             AS transform_cpu_ms,
           coalesce((r.transform_usage -> 'transform' ->> 'ram_mib_s')::float, 0)
             + coalesce((r.transform_usage -> 'catalog' ->> 'ram_mib_s')::float, 0)
             AS transform_ram_mib_s,
           extract(epoch FROM r.transform_finished_at - r.transform_started_at)
             AS transform_seconds
    FROM bench_runs AS r
    WHERE r.status = '{DONE}'
)
SELECT run_id, branch, night, rate, repetition, bronze_messages, throughput_msg_s,
       CASE WHEN latency_relevant THEN latency_p50_ms END AS latency_p50_ms,
       CASE WHEN latency_relevant THEN latency_p95_ms END AS latency_p95_ms,
       ingest_cpu_ms / nullif(bronze_messages, 0) AS ingest_cpu_ms_per_message,
       ingest_ram_mib_s * 10000 / nullif(bronze_messages, 0)
         AS ingest_ram_mib_s_per_10k_messages,
       ingest_peak_mib,
       transform_cpu_ms / nullif(bronze_messages, 0) AS transform_cpu_ms_per_message,
       transform_ram_mib_s * 10000 / nullif(bronze_messages, 0)
         AS transform_ram_mib_s_per_10k_messages,
       transform_seconds,
       bronze_messages / nullif(transform_seconds, 0) AS transform_rows_per_s,
       transform_passes
FROM usage
"""


@dataclass(frozen=True)
class BenchRunRow:
    """One row of bench_runs."""

    run_id: int
    branch: str
    night: date
    rate: str
    repetition: int
    status: str
    end_offsets: cal.Offsets | None
    requested_at: datetime
    started_at: datetime | None = None
    ingested_at: datetime | None = None
    finished_at: datetime | None = None
    replay_started_at: datetime | None = None
    replay_finished_at: datetime | None = None
    replay_messages: int | None = None
    transform_started_at: datetime | None = None
    transform_finished_at: datetime | None = None
    transform_passes: int | None = None
    bronze_messages: int | None = None
    first_processed_at: datetime | None = None
    last_processed_at: datetime | None = None
    throughput_msg_s: float | None = None
    latency_p50_ms: float | None = None
    latency_p95_ms: float | None = None
    latency_relevant: bool | None = None
    ingest_usage: dict | None = None
    transform_usage: dict | None = None
    note: str | None = None


def _row(record: dict) -> BenchRunRow:
    return BenchRunRow(
        **{
            **record,
            "end_offsets": cal.offsets_from_json(record["end_offsets"], config.BENCH_TOPIC),
        }
    )


class BenchStore:
    """bench_runs in the benchmark database. Each method is one transaction."""

    def __init__(self, conn) -> None:
        self._conn = conn

    @property
    def connection(self):
        """The benchmark database session (the measures read the samples with it)."""
        return self._conn

    @contextmanager
    def _transaction(self) -> Iterator:
        with self._conn:  # commits, or rolls back on error (autocommit is restored)
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur

    def ensure_table(self) -> None:
        with self._transaction() as cur:
            cur.execute(DDL)
            cur.execute(VIEW)

    def get(self, run_id: int) -> BenchRunRow | None:
        with self._transaction() as cur:
            cur.execute("SELECT * FROM bench_runs WHERE run_id = %s", (run_id,))
            record = cur.fetchone()
        return _row(record) if record else None

    def active_for(self, branch: str) -> BenchRunRow | None:
        """The run the engine of `branch` must work on now, if any (supervisor): the
        oldest requested or running one."""
        with self._transaction() as cur:
            cur.execute(
                "SELECT * FROM bench_runs WHERE branch = %s AND status = ANY(%s) "
                "ORDER BY run_id LIMIT 1",
                (branch, list(ACTIVE_STATUSES)),
            )
            record = cur.fetchone()
        return _row(record) if record else None

    # Decisions: Dagster only

    def request(
        self,
        branch: str,
        night: date,
        rate: str,
        repetition: int,
        now: datetime,
        end_offsets: cal.Offsets | None = None,
    ) -> BenchRunRow:
        """A new run; refused while another run of the branch is active (one at a
        time: the engine works on a single run). A run at "max" is requested once the
        replay filled the topic, with its end offsets already known."""
        if self.active_for(branch) is not None:
            raise cal.CalendarError(f"A run of the {branch} branch is already active")
        end = cal.offsets_to_json(end_offsets, config.BENCH_TOPIC) if end_offsets else None
        with self._transaction() as cur:
            cur.execute(
                "INSERT INTO bench_runs (branch, night, rate, repetition, status, requested_at, "
                "end_offsets) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (branch, night, rate, repetition) DO UPDATE SET "
                "status = EXCLUDED.status, requested_at = EXCLUDED.requested_at, "
                "end_offsets = EXCLUDED.end_offsets, started_at = NULL, ingested_at = NULL, "
                "finished_at = NULL, replay_started_at = NULL, replay_finished_at = NULL, "
                "replay_messages = NULL, transform_started_at = NULL, "
                "transform_finished_at = NULL, transform_passes = NULL, note = NULL, "
                + ", ".join(f"{column} = NULL" for column in MEASURE_COLUMNS)
                + " "
                "RETURNING *",
                (branch, night, rate, repetition, REQUESTED, now, end),
            )
            return _row(cur.fetchone())

    def record_replay(
        self, run_id: int, started: datetime, finished: datetime, messages: int
    ) -> None:
        with self._transaction() as cur:
            cur.execute(
                "UPDATE bench_runs SET replay_started_at = %s, replay_finished_at = %s, "
                "replay_messages = %s WHERE run_id = %s",
                (started, finished, messages, run_id),
            )

    def record_transform(
        self, run_id: int, started: datetime, finished: datetime, passes: int
    ) -> None:
        with self._transaction() as cur:
            cur.execute(
                "UPDATE bench_runs SET transform_started_at = %s, transform_finished_at = %s, "
                "transform_passes = %s WHERE run_id = %s",
                (started, finished, passes, run_id),
            )

    def record_measures(self, run_id: int, measures: dict) -> None:
        """Measures of a run (src/benchmark/run_measures.py); usages stored as JSON."""
        values = {
            k: json.dumps(v) if k.endswith("_usage") else v
            for k, v in measures.items()
            if k in MEASURE_COLUMNS
        }
        if not values:
            return
        assignments = ", ".join(f"{column} = %s" for column in values)
        with self._transaction() as cur:
            cur.execute(
                f"UPDATE bench_runs SET {assignments} WHERE run_id = %s",
                (*values.values(), run_id),
            )

    def night(self, branch: str, night: date) -> list[BenchRunRow]:
        """Every run of a branch's night, in request order."""
        with self._transaction() as cur:
            cur.execute(
                "SELECT * FROM bench_runs WHERE branch = %s AND night = %s ORDER BY run_id",
                (branch, night),
            )
            return [_row(r) for r in cur.fetchall()]

    def finish(self, run_id: int, status: str, now: datetime, note: str | None = None) -> None:
        """Close a run: `done` once measured, `failed` on an error or past the deadline."""
        if status not in (DONE, FAILED):
            raise ValueError(f"Not a final status: {status!r}")
        with self._transaction() as cur:
            cur.execute(
                "UPDATE bench_runs SET status = %s, finished_at = %s, "
                "note = coalesce(%s, note) WHERE run_id = %s",
                (status, now, note, run_id),
            )

    # Observations: the replay and the engine

    def set_end(self, run_id: int, end_offsets: cal.Offsets) -> None:
        """End offsets of the bench topic once the replay sent everything."""
        with self._transaction() as cur:
            cur.execute(
                "UPDATE bench_runs SET end_offsets = %s WHERE run_id = %s",
                (cal.offsets_to_json(end_offsets, config.BENCH_TOPIC), run_id),
            )

    def mark_running(self, run_id: int, now: datetime) -> bool:
        """The engine's first launch on this run; False on a restart (already running)."""
        with self._transaction() as cur:
            cur.execute(
                "UPDATE bench_runs SET status = %s, started_at = %s "
                "WHERE run_id = %s AND status = %s",
                (RUNNING, now, run_id, REQUESTED),
            )
            return cur.rowcount == 1

    def mark_ingested(self, run_id: int, now: datetime) -> bool:
        with self._transaction() as cur:
            cur.execute(
                "UPDATE bench_runs SET status = %s, ingested_at = %s "
                "WHERE run_id = %s AND status = %s",
                (INGESTED, now, run_id, RUNNING),
            )
            return cur.rowcount == 1


Store = Callable[[], BenchStore]


def _default_store() -> BenchStore:
    return BenchStore(cal.connect_benchmark())


class BenchRun:
    """The engine's side of a controlled-test run, with the interface of EngineDay
    (prepare, in_bounds, bounds, record_commit, check_complete): the engines handle a
    day and a run the same way.

    The bench topic is recreated empty for each run, so a run reads it from offset 0.
    Its end offsets appear once the replay has sent everything: until then every
    message passes; afterwards nothing past them exists anyway (the replay is done).
    """

    def __init__(
        self,
        run_id: int,
        store: Store = _default_store,
        poll_seconds: float = config.BENCH_BOUNDS_POLL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.run_id = run_id
        self._store_factory = store
        self._store: BenchStore | None = None
        self._poll_seconds = poll_seconds
        self._clock = clock
        self._now = now
        self._last_read = float("-inf")
        self.start: cal.Offsets = {p: 0 for p in range(config.RAW_EVENTS_PARTITIONS)}
        self.end: cal.Offsets | None = None
        # No catch-up detection in a run: its latency is measured from the send times
        self.caught_up = True
        self.done = False

    @classmethod
    def from_env(cls) -> "BenchRun | None":
        """The run set by the supervisor; None outside the controlled test."""
        value = os.getenv(RUN_ENV)
        return cls(int(value)) if value else None

    def _bench(self) -> BenchStore:
        if self._store is None:
            self._store = self._store_factory()
        return self._store

    def _load(self) -> BenchRunRow:
        row = self._bench().get(self.run_id)
        if row is None:
            raise cal.CalendarError(f"Run {self.run_id} has no bench_runs row")
        self.end = row.end_offsets
        self._last_read = self._clock()
        return row

    def prepare(self, seek: Callable[[cal.Offsets], None]) -> None:
        """Before the engine consumes: on the run's first launch, `seek` to the topic's
        start, then record it. A restart resumes from the engine's own commits."""
        self._load()
        if self._bench().mark_running(self.run_id, self._now()):
            seek(self.start)
            logger.info(f"Bench run {self.run_id}: first launch, reading from {self.start}")
        else:
            logger.info(f"Bench run {self.run_id}: restart, resuming from the engine's commits")

    def refresh(self) -> None:
        """Re-read the end offsets every poll interval until the replay records them."""
        if self.end is not None or self._clock() - self._last_read < self._poll_seconds:
            return
        try:
            self._load()
        except Exception as e:  # noqa: BLE001 - keep consuming, retried later
            logger.warning(f"Reading bench run {self.run_id} failed ({e}), retrying later")
            self._store = None
            self._last_read = self._clock()

    def bounds(self) -> cal.Offsets | None:
        self.refresh()
        return self.end

    def in_bounds(self, partition: int, offset: int) -> bool:
        end = self.bounds()
        return end is None or offset < end.get(partition, 0)

    def record_commit(self, newest_event: datetime | None) -> None:
        """No catch-up mark in a run (see `caught_up`)."""

    def check_complete(self, position: cal.Offsets) -> bool:
        """Mark the run ingested once the committed position covers the end offsets."""
        self.refresh()
        if self.done or self.end is None or not cal.reached(position, self.end):
            return self.done
        try:
            self._bench().mark_ingested(self.run_id, self._now())
        except Exception as e:  # noqa: BLE001 - retried at the next check
            logger.warning(f"Marking bench run {self.run_id} ingested failed ({e})")
            self._store = None
            return False
        self.done = True
        logger.info(f"Bench run {self.run_id}: every replayed message is in Bronze")
        return True
