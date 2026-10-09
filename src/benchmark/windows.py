"""
5-minute windows of a closed benchmark day (phase 7, decision D34), built at night from
the collector's raw samples (src/benchmark/collector.py) and the branch's own Bronze.

- service_windows: per window and measured container, CPU time, RAM·time (integral of
  `anon`, trapezoids, like cloud billing), swap·time, memory stall time, major faults and
  peak real memory. A window where the container restarted (its counters reset) is
  flagged, and so is one the samples do not cover (collector gap);
- branch_windows: per window, the branch's messages (distinct Kafka offsets of the day,
  first copy), end-to-end latency p50/p95 (processed_at − event_time), collection mix,
  regime (catch-up before the day's caught_up_at, live after), rate bucket, and the host's
  health over the window (CPU steal, PSI), which only flags contaminated windows (D32).

Ratios (CPU ms per message, MiB·s per 10 000 messages) are not stored: the view
`benchmark_ratios` computes them, so a formula changes without recomputing anything.
A day is rebuilt from scratch on every run (idempotent).

Kept free of Dagster and of any engine: the assets hand in the Bronze rows.
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from src import config
from src.alternation import calendar as cal

WINDOW = timedelta(seconds=config.BENCHMARK_WINDOW_SECONDS)
MIB = 2**20


def window_start(ts: datetime) -> datetime:
    """Start of the 5-minute window holding `ts` (UTC, aligned on the epoch)."""
    seconds = int(ts.timestamp())
    return datetime.fromtimestamp(seconds - seconds % int(WINDOW.total_seconds()), UTC)


@dataclass(frozen=True)
class Sample:
    """One collector sample of a container (cumulative counters, current levels)."""

    ts: datetime
    container_id: str
    cpu_usec: int | None
    anon_bytes: int | None
    swap_bytes: int | None
    pgmajfault: int | None
    mem_psi_full_us: int | None


@dataclass
class ServiceWindow:
    cpu_ms: float = 0.0
    ram_mib_s: float = 0.0
    swap_mib_s: float = 0.0
    psi_mem_ms: float = 0.0
    majfaults: float = 0.0
    peak_anon_mib: float = 0.0
    covered_s: float = 0.0
    restarted: bool = False


def _split(a: datetime, b: datetime) -> list[tuple[datetime, float]]:
    """The windows an interval [a, b] overlaps, with the share of the interval in each."""
    total = (b - a).total_seconds()
    parts, cursor = [], a
    while cursor < b:
        start = window_start(cursor)
        end = min(start + WINDOW, b)
        parts.append((start, (end - cursor).total_seconds() / total))
        cursor = end
    return parts


def _delta(a: int | None, b: int | None) -> float | None:
    if a is None or b is None or b < a:
        return None
    return float(b - a)


def service_windows(
    samples: list[Sample], max_gap: timedelta = timedelta(seconds=30)
) -> dict[datetime, ServiceWindow]:
    """Windows of one container from its consecutive samples.

    Each pair of samples spreads its counter deltas and its RAM integral over the windows
    it overlaps, in proportion to time. A pair across a restart (new container id) or a
    gap longer than `max_gap` (the collector missed samples) is not used: the window is
    flagged restarted, or left with less coverage.
    """
    windows: dict[datetime, ServiceWindow] = defaultdict(ServiceWindow)
    ordered = sorted(samples, key=lambda s: s.ts)
    for a, b in zip(ordered, ordered[1:], strict=False):
        if a.container_id != b.container_id:
            windows[window_start(b.ts)].restarted = True
            continue
        seconds = (b.ts - a.ts).total_seconds()
        if seconds <= 0 or b.ts - a.ts > max_gap:
            continue
        cpu = _delta(a.cpu_usec, b.cpu_usec)
        psi = _delta(a.mem_psi_full_us, b.mem_psi_full_us)
        faults = _delta(a.pgmajfault, b.pgmajfault)
        anon = None
        if a.anon_bytes is not None and b.anon_bytes is not None:
            anon = (a.anon_bytes + b.anon_bytes) / 2 * seconds / MIB
        swap = None
        if a.swap_bytes is not None and b.swap_bytes is not None:
            swap = (a.swap_bytes + b.swap_bytes) / 2 * seconds / MIB
        for start, share in _split(a.ts, b.ts):
            w = windows[start]
            w.covered_s += seconds * share
            w.cpu_ms += (cpu or 0.0) * share / 1000
            w.psi_mem_ms += (psi or 0.0) * share / 1000
            w.majfaults += (faults or 0.0) * share
            w.ram_mib_s += (anon or 0.0) * share
            w.swap_mib_s += (swap or 0.0) * share
            for level in (a.anon_bytes, b.anon_bytes):
                if level is not None:
                    w.peak_anon_mib = max(w.peak_anon_mib, level / MIB)
    return dict(windows)


@dataclass(frozen=True)
class HostSample:
    ts: datetime
    cpu_total_ticks: int
    cpu_steal_ticks: int
    mem_available_bytes: int
    mem_psi_full_us: int | None
    cpu_psi_some_us: int | None
    io_psi_full_us: int | None


@dataclass
class HostWindow:
    steal_ticks: float = 0.0
    total_ticks: float = 0.0
    psi_mem_ms: float = 0.0
    psi_cpu_ms: float = 0.0
    psi_io_ms: float = 0.0
    min_available_mib: float | None = None

    @property
    def steal_pct(self) -> float | None:
        return 100 * self.steal_ticks / self.total_ticks if self.total_ticks else None


def host_windows(samples: list[HostSample]) -> dict[datetime, HostWindow]:
    """Host health per window: CPU steal share, stall times, lowest available memory."""
    windows: dict[datetime, HostWindow] = defaultdict(HostWindow)
    ordered = sorted(samples, key=lambda s: s.ts)
    for a, b in zip(ordered, ordered[1:], strict=False):
        for start, share in _split(a.ts, b.ts):
            w = windows[start]
            w.steal_ticks += (_delta(a.cpu_steal_ticks, b.cpu_steal_ticks) or 0.0) * share
            w.total_ticks += (_delta(a.cpu_total_ticks, b.cpu_total_ticks) or 0.0) * share
            w.psi_mem_ms += (_delta(a.mem_psi_full_us, b.mem_psi_full_us) or 0.0) * share / 1000
            w.psi_cpu_ms += (_delta(a.cpu_psi_some_us, b.cpu_psi_some_us) or 0.0) * share / 1000
            w.psi_io_ms += (_delta(a.io_psi_full_us, b.io_psi_full_us) or 0.0) * share / 1000
            available = min(a.mem_available_bytes, b.mem_available_bytes) / MIB
            w.min_available_mib = (
                available if w.min_available_mib is None else min(w.min_available_mib, available)
            )
    return dict(windows)


def host_contaminated(window: HostWindow | None) -> bool:
    """A window the neighbours may have biased (thresholds provisional until the freeze)."""
    if window is None:
        return True  # no host data: cannot be vouched for
    steal = window.steal_pct or 0.0
    stalled_ms = window.psi_mem_ms + window.psi_io_ms
    return (
        steal > config.HOST_STEAL_PCT_MAX
        or stalled_ms > config.HOST_STALL_MS_MAX
        or (window.min_available_mib or 0.0) < config.HOST_MIN_AVAILABLE_MIB
    )


def rate_bucket(messages: int) -> str:
    """Traffic level of a window (msg/s), to compare the branches at equal throughput."""
    rate = messages / WINDOW.total_seconds()
    for upper, label in config.BENCHMARK_RATE_BUCKETS:
        if rate < upper:
            return label
    return config.BENCHMARK_RATE_BUCKETS[-1][1]


@dataclass
class MessageWindow:
    """Bronze side of a window, computed by the branch's engine (see the queries)."""

    messages: int
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    collections: dict[str, int] = field(default_factory=dict)


# --- Bronze queries, one per engine: same contract (distinct offsets of the day, first
# copy, exact percentiles), different SQL dialects ---
#
# Percentiles come from a histogram of latencies in whole milliseconds, not from
# quantile_cont / percentile: Spark's exact `percentile` keeps every value of a group in
# the JVM heap (a catch-up window holds millions), and the Thrift server died of it on
# 2026-10-08. Counting by (window, latency) is a plain aggregation that spills to disk.
# The result is the same linear interpolation as quantile_cont: at the 0-based position
# h = (n - 1) * q, lo = value of rank floor(h), hi = value of rank ceil(h),
# p = lo + (h - floor(h)) * (hi - lo); the value of rank k is the first latency whose
# running count exceeds k. The histogram SQL is shared, the first copy is per dialect.


def _day_offsets(day: cal.CalendarDay) -> str:
    if day.end_offsets is None:
        raise cal.CalendarError(f"Day {day.day} has no end offsets yet")
    return " OR ".join(
        f"(kafka_partition = {int(p)} AND kafka_offset >= {int(day.start_offsets.get(p, end))} "
        f"AND kafka_offset < {int(end)})"
        for p, end in sorted(day.end_offsets.items())
    )


def _percentiles_from(latencies: str) -> str:
    """Rest of the query over `latencies` (window_epoch, collection, latency_ms): rows of
    window start, collection (NULL for the total), messages, p50, p95. Valid in DuckDB
    and Spark SQL."""
    return f"""
        by_collection AS (
            SELECT window_epoch, collection, latency_ms, count(*) AS n
            FROM {latencies}
            GROUP BY window_epoch, collection, latency_ms
        ),
        histogram AS (
            SELECT window_epoch, collection, latency_ms, n FROM by_collection
            UNION ALL
            SELECT window_epoch, NULL AS collection, latency_ms, sum(n) AS n
            FROM by_collection
            GROUP BY window_epoch, latency_ms
        ),
        running AS (
            SELECT window_epoch, collection, latency_ms,
                   sum(n) OVER (PARTITION BY window_epoch, collection ORDER BY latency_ms
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum,
                   sum(n) OVER (PARTITION BY window_epoch, collection) AS total
            FROM histogram
        ),
        ranks AS (
            SELECT window_epoch, collection, latency_ms, cum, total,
                   (total - 1) * 0.5 AS h50, (total - 1) * 0.95 AS h95
            FROM running
        ),
        bounds AS (
            SELECT window_epoch, collection, max(total) AS messages,
                   max(h50) AS h50, max(h95) AS h95,
                   min(CASE WHEN cum > floor(h50) THEN latency_ms END) AS lo50,
                   min(CASE WHEN cum > ceil(h50) THEN latency_ms END) AS hi50,
                   min(CASE WHEN cum > floor(h95) THEN latency_ms END) AS lo95,
                   min(CASE WHEN cum > ceil(h95) THEN latency_ms END) AS hi95
            FROM ranks
            GROUP BY window_epoch, collection
        )
        SELECT window_epoch, collection, messages,
               lo50 + (h50 - floor(h50)) * (hi50 - lo50) AS p50,
               lo95 + (h95 - floor(h95)) * (hi95 - lo95) AS p95
        FROM bounds
    """


def messages_query_duckdb(table: str, day: cal.CalendarDay) -> str:
    """Rows: window start (epoch s), collection, messages, p50 and p95 latency (ms)."""
    return f"""
        WITH first_copy AS (
            SELECT kafka_partition, kafka_offset, min(processed_at) AS processed_at,
                   min(event_time) AS event_time, any_value(collection) AS collection
            FROM {table} WHERE {_day_offsets(day)}
            GROUP BY kafka_partition, kafka_offset
        ),
        latencies AS (
            SELECT floor(epoch(processed_at))::BIGINT // {int(WINDOW.total_seconds())}
                       * {int(WINDOW.total_seconds())} AS window_epoch,
                   collection, epoch_ms(processed_at) - epoch_ms(event_time) AS latency_ms
            FROM first_copy
        ),
        {_percentiles_from("latencies")}
    """


def messages_query_spark(table: str, day: cal.CalendarDay) -> str:
    """Same as messages_query_duckdb, in Spark SQL (whole milliseconds, as epoch_ms)."""
    return f"""
        WITH first_copy AS (
            SELECT kafka_partition, kafka_offset, min(processed_at) AS processed_at,
                   min(event_time) AS event_time, first(collection) AS collection
            FROM {table} WHERE {_day_offsets(day)}
            GROUP BY kafka_partition, kafka_offset
        ),
        latencies AS (
            SELECT div(unix_timestamp(processed_at), {int(WINDOW.total_seconds())})
                       * {int(WINDOW.total_seconds())} AS window_epoch,
                   collection, unix_millis(processed_at) - unix_millis(event_time) AS latency_ms
            FROM first_copy
        ),
        {_percentiles_from("latencies")}
    """


def message_windows(rows: list[tuple]) -> dict[datetime, MessageWindow]:
    """Rows of a messages query -> windows (the row without collection is the total)."""
    windows: dict[datetime, MessageWindow] = {}
    collections: dict[datetime, dict[str, int]] = defaultdict(dict)
    for window_epoch, collection, messages, p50, p95 in rows:
        start = datetime.fromtimestamp(int(window_epoch), UTC)
        if collection is None:
            windows[start] = MessageWindow(
                int(messages),
                float(p50) if p50 is not None else None,
                float(p95) if p95 is not None else None,
            )
        else:
            collections[start][collection] = int(messages)
    for start, mix in collections.items():
        if start in windows:
            windows[start].collections = dict(sorted(mix.items()))
    return windows


def regime(start: datetime, day: cal.CalendarDay) -> str:
    """`catchup` until the engine caught up with live traffic, `live` after (D28)."""
    caught_up = day.caught_up_at
    return "live" if caught_up is not None and start >= window_start(caught_up) else "catchup"


# --- Storage (benchmark database, next to the collector's samples) ---

DDL = [
    """CREATE TABLE IF NOT EXISTS service_windows (
        day             DATE NOT NULL,
        window_start    TIMESTAMPTZ NOT NULL,
        service         TEXT NOT NULL,
        branch          TEXT,
        layer           TEXT NOT NULL,
        cpu_ms          DOUBLE PRECISION,
        ram_mib_s       DOUBLE PRECISION,
        swap_mib_s      DOUBLE PRECISION,
        psi_mem_ms      DOUBLE PRECISION,
        majfaults       DOUBLE PRECISION,
        peak_anon_mib   DOUBLE PRECISION,
        covered_s       DOUBLE PRECISION,
        restarted       BOOLEAN NOT NULL,
        PRIMARY KEY (window_start, service)
    )""",
    """CREATE TABLE IF NOT EXISTS branch_windows (
        day             DATE NOT NULL,
        branch          TEXT NOT NULL,
        window_start    TIMESTAMPTZ NOT NULL,
        messages        BIGINT NOT NULL,
        latency_p50_ms  DOUBLE PRECISION,
        latency_p95_ms  DOUBLE PRECISION,
        collections     JSONB,
        regime          TEXT NOT NULL,
        rate_bucket     TEXT NOT NULL,
        host_steal_pct  DOUBLE PRECISION,
        host_psi_mem_ms DOUBLE PRECISION,
        host_psi_cpu_ms DOUBLE PRECISION,
        host_psi_io_ms  DOUBLE PRECISION,
        host_min_available_mib DOUBLE PRECISION,
        host_contaminated BOOLEAN NOT NULL,
        PRIMARY KEY (window_start, branch)
    )""",
    # Ratios per message and layer, only on clean windows (fully sampled, no restart, host
    # not contaminated): a formula changes here, without recomputing a single window
    f"""CREATE OR REPLACE VIEW benchmark_ratios AS
    SELECT b.day, b.branch, b.window_start, b.regime, b.rate_bucket, b.messages,
           b.latency_p50_ms, b.latency_p95_ms, s.layer,
           sum(s.cpu_ms) / nullif(b.messages, 0) AS cpu_ms_per_message,
           sum(s.ram_mib_s) * 10000 / nullif(b.messages, 0) AS ram_mib_s_per_10k_messages,
           sum(s.swap_mib_s) * 10000 / nullif(b.messages, 0) AS swap_mib_s_per_10k_messages,
           sum(s.psi_mem_ms) AS psi_mem_ms,
           max(s.peak_anon_mib) AS peak_anon_mib
    FROM branch_windows AS b
    JOIN service_windows AS s ON s.window_start = b.window_start AND s.branch = b.branch
    WHERE NOT b.host_contaminated AND NOT s.restarted
      AND s.covered_s >= {config.BENCHMARK_WINDOW_SECONDS} * {config.BENCHMARK_MIN_COVERAGE}
    GROUP BY b.day, b.branch, b.window_start, b.regime, b.rate_bucket, b.messages,
             b.latency_p50_ms, b.latency_p95_ms, s.layer""",
]


def day_range(day: cal.CalendarDay) -> tuple[datetime, datetime]:
    """Samples of a day: from its opening (07:00) to its engine's stop, or the next
    opening for a late day. Transform runs after the stop are part of the day's cost."""
    start = day.opened_at or cal.at(day.day, config.ALTERNATION_OPEN_TIME)
    end = (day.stopped_at or cal.hard_stop_time(day.day)) + timedelta(
        seconds=config.TRANSFORM_TAIL_SECONDS
    )
    return window_start(start), end


def rebuild_day(
    conn,
    day: cal.CalendarDay,
    message_rows: list[tuple],
) -> dict[str, int]:
    """Rebuild the day's windows from the stored samples and the branch's Bronze rows."""
    start, end = day_range(day)
    branch = day.effective_branch
    with conn, conn.cursor() as cur:
        for statement in DDL:
            cur.execute(statement)
        cur.execute("DELETE FROM service_windows WHERE day = %s", (day.day,))
        cur.execute("DELETE FROM branch_windows WHERE day = %s AND branch = %s", (day.day, branch))
        cur.execute(
            "SELECT service, branch, layer, ts, container_id, cpu_usec, anon_bytes, swap_bytes, "
            "pgmajfault, mem_psi_full_us FROM container_samples WHERE ts >= %s AND ts < %s",
            (start, end),
        )
        by_service: dict[str, list[Sample]] = defaultdict(list)
        layers: dict[str, tuple[str | None, str]] = {}
        for service, sbranch, layer, ts, cid, cpu, anon, swap, faults, psi in cur.fetchall():
            by_service[service].append(Sample(ts, cid, cpu, anon, swap, faults, psi))
            layers[service] = (sbranch, layer)
        service_rows = 0
        for service, samples in by_service.items():
            sbranch, layer = layers[service]
            for wstart, w in service_windows(samples).items():
                if not start <= wstart < end:
                    continue
                cur.execute(
                    "INSERT INTO service_windows VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s)",
                    (
                        day.day,
                        wstart,
                        service,
                        sbranch,
                        layer,
                        w.cpu_ms,
                        w.ram_mib_s,
                        w.swap_mib_s,
                        w.psi_mem_ms,
                        w.majfaults,
                        w.peak_anon_mib,
                        w.covered_s,
                        w.restarted,
                    ),
                )
                service_rows += 1
        cur.execute(
            "SELECT ts, cpu_total_ticks, cpu_steal_ticks, mem_available_bytes, mem_psi_full_us, "
            "cpu_psi_some_us, io_psi_full_us FROM host_samples WHERE ts >= %s AND ts < %s",
            (start, end),
        )
        hosts = host_windows([HostSample(*row) for row in cur.fetchall()])
        branch_rows = 0
        for wstart, m in sorted(message_windows(message_rows).items()):
            h = hosts.get(wstart)
            cur.execute(
                "INSERT INTO branch_windows VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s)",
                (
                    day.day,
                    branch,
                    wstart,
                    m.messages,
                    m.latency_p50_ms,
                    m.latency_p95_ms,
                    json.dumps(m.collections),
                    regime(wstart, day),
                    rate_bucket(m.messages),
                    h.steal_pct if h else None,
                    h.psi_mem_ms if h else None,
                    h.psi_cpu_ms if h else None,
                    h.psi_io_ms if h else None,
                    h.min_available_mib if h else None,
                    host_contaminated(h),
                ),
            )
            branch_rows += 1
    return {"service_windows": service_rows, "branch_windows": branch_rows}
