"""
Measures of one controlled-test run (decision D36), computed at the end of the run,
before the next one empties the bench tables:

- messages, throughput and latency from the branch's bench Bronze: every replayed
  message's first `processed_at`, by (partition, offset);
  - throughput = messages / (last - first processed_at): the write window, without the
    engine's start-up (counted in H6), the same way for both branches (H3);
  - latency = processed_at - the message's send time (its Kafka timestamp in the replay
    topic, set by the replay), p50/p95 with the interpolation of `quantile_cont`, like
    the live windows. Only meaningful at 1x and 4x: at "max" the messages wait in the
    topic before the engine starts, so the run is flagged `latency_relevant = false`;
- per phase and per layer, the branch's containers' usage from the collector's samples
  (CPU time, RAM·time of `anon`, peak `anon`, swap·time, major faults), with the
  formulas of the live windows: ingestion (from the replay's start, or the engine's at
  "max", to `ingested_at`) and Silver/Gold (transform timestamps). The ratios per
  message are computed by the view `bench_ratios`.

Samples are taken every 10 s: a phase of 15-20 s (DuckDB at "max") gets an approximate
CPU and RAM; its throughput, read from Bronze, stays exact (accepted, D36).

Kept free of Dagster and of any engine: the branch hands in its Bronze arrivals.
"""

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, KafkaException, TopicPartition

from src import config
from src.benchmark import runs
from src.benchmark.sample import MAX_EMPTY_POLLS, POLL_TIMEOUT_SECONDS
from src.benchmark.windows import MIB, Sample
from src.resources import redpanda

# (partition, offset, first processed_at in epoch milliseconds) of each Bronze message
Arrival = tuple[int, int, int]
# Samples just outside a phase bracket its edges
SAMPLE_MARGIN = timedelta(seconds=30)
# A gap longer than this between two samples is not integrated (collector missed some)
MAX_SAMPLE_GAP = timedelta(seconds=30)


@dataclass
class Usage:
    """A container's usage over an interval."""

    cpu_ms: float = 0.0
    ram_mib_s: float = 0.0
    swap_mib_s: float = 0.0
    peak_anon_mib: float = 0.0
    majfaults: float = 0.0
    covered_s: float = 0.0
    restarted: bool = False

    def add(self, other: "Usage") -> None:
        self.cpu_ms += other.cpu_ms
        self.ram_mib_s += other.ram_mib_s
        self.swap_mib_s += other.swap_mib_s
        self.peak_anon_mib += other.peak_anon_mib
        self.majfaults += other.majfaults
        self.covered_s = max(self.covered_s, other.covered_s)
        self.restarted = self.restarted or other.restarted


def interval_usage(samples: list[Sample], start: datetime, end: datetime) -> Usage:
    """Usage of one container over [start, end]: each pair of consecutive samples counts
    in proportion to its overlap with the interval (the live windows' formulas)."""
    usage = Usage()
    ordered = sorted(samples, key=lambda s: s.ts)
    for a, b in zip(ordered, ordered[1:], strict=False):
        if a.container_id != b.container_id:
            if start <= b.ts <= end:
                usage.restarted = True
            continue
        seconds = (b.ts - a.ts).total_seconds()
        if seconds <= 0 or b.ts - a.ts > MAX_SAMPLE_GAP:
            continue
        overlap = (min(b.ts, end) - max(a.ts, start)).total_seconds()
        if overlap <= 0:
            continue
        share = overlap / seconds
        usage.covered_s += overlap
        if a.cpu_usec is not None and b.cpu_usec is not None and b.cpu_usec >= a.cpu_usec:
            usage.cpu_ms += (b.cpu_usec - a.cpu_usec) * share / 1000
        if a.pgmajfault is not None and b.pgmajfault is not None and b.pgmajfault >= a.pgmajfault:
            usage.majfaults += (b.pgmajfault - a.pgmajfault) * share
        if a.anon_bytes is not None and b.anon_bytes is not None:
            usage.ram_mib_s += (a.anon_bytes + b.anon_bytes) / 2 * overlap / MIB
            usage.peak_anon_mib = max(usage.peak_anon_mib, a.anon_bytes / MIB, b.anon_bytes / MIB)
        if a.swap_bytes is not None and b.swap_bytes is not None:
            usage.swap_mib_s += (a.swap_bytes + b.swap_bytes) / 2 * overlap / MIB
    return usage


def phase_usage(conn, branch: str, start: datetime, end: datetime) -> dict[str, dict]:
    """Usage of the branch's containers over a phase, summed per layer."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT service, layer, ts, container_id, cpu_usec, anon_bytes, swap_bytes, "
            "pgmajfault, mem_psi_full_us FROM container_samples "
            "WHERE branch = %s AND ts >= %s AND ts <= %s",
            (branch, start - SAMPLE_MARGIN, end + SAMPLE_MARGIN),
        )
        rows = cur.fetchall()
    by_service: dict[str, list[Sample]] = defaultdict(list)
    layers: dict[str, str] = {}
    for service, layer, ts, cid, cpu, anon, swap, faults, psi in rows:
        by_service[service].append(Sample(ts, cid, cpu, anon, swap, faults, psi))
        layers[service] = layer
    by_layer: dict[str, Usage] = defaultdict(Usage)
    for service, samples in by_service.items():
        by_layer[layers[service]].add(interval_usage(samples, start, end))
    return {layer: asdict(u) for layer, u in sorted(by_layer.items())}


def send_times(topic: str = config.BENCH_TOPIC) -> dict[tuple[int, int], int]:
    """Kafka timestamp (ms) of every message of the replay topic: its send time."""
    bounds = {p: hl for p, hl in redpanda.watermarks(topic).items() if hl[1] > hl[0]}
    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "bluesky-bench-measure",
            "enable.auto.commit": False,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    times: dict[tuple[int, int], int] = {}
    pending = set(bounds)
    try:
        consumer.assign([TopicPartition(topic, p, bounds[p][0]) for p in sorted(pending)])
        empty_polls = 0
        while pending:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            if message is None:
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    raise RuntimeError(f"Replay topic read stalled on {sorted(pending)}")
                continue
            empty_polls = 0
            if message.error():
                raise KafkaException(message.error())
            p, offset = message.partition(), message.offset()
            times[(p, offset)] = message.timestamp()[1]
            if offset + 1 >= bounds[p][1]:
                pending.discard(p)
    finally:
        consumer.close()
    return times


def percentile(ordered: list[float], q: float) -> float | None:
    """Linear interpolation between the closest ranks, as DuckDB's quantile_cont."""
    if not ordered:
        return None
    h = (len(ordered) - 1) * q
    lo = int(h)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (h - lo) * (ordered[hi] - ordered[lo])


def arrival_measures(arrivals: list[Arrival], sends: dict[tuple[int, int], int] | None) -> dict:
    """Messages, write window, throughput and (with send times) latency p50/p95."""
    first: dict[tuple[int, int], int] = {}
    for p, offset, ms in arrivals:
        key = (p, offset)
        if key not in first or ms < first[key]:
            first[key] = ms
    if not first:
        return {"bronze_messages": 0}
    lo, hi = min(first.values()), max(first.values())
    seconds = (hi - lo) / 1000
    measures = {
        "bronze_messages": len(first),
        "first_processed_at": datetime.fromtimestamp(lo / 1000, UTC),
        "last_processed_at": datetime.fromtimestamp(hi / 1000, UTC),
        "throughput_msg_s": len(first) / seconds if seconds > 0 else None,
    }
    if sends:
        latencies = sorted(ms - sends[key] for key, ms in first.items() if key in sends)
        measures["latency_p50_ms"] = percentile(latencies, 0.5)
        measures["latency_p95_ms"] = percentile(latencies, 0.95)
    return measures


def measure_run(conn, run: runs.BenchRunRow, arrivals: list[Arrival], sends) -> dict:
    """Every measure of a closed run, ready for BenchStore.record_measures."""
    measures = arrival_measures(arrivals, sends)
    measures["latency_relevant"] = run.rate != "max"
    ingest_start = run.started_at if run.rate == "max" else run.replay_started_at
    if ingest_start and run.ingested_at:
        measures["ingest_usage"] = phase_usage(conn, run.branch, ingest_start, run.ingested_at)
    if run.transform_started_at and run.transform_finished_at:
        measures["transform_usage"] = phase_usage(
            conn, run.branch, run.transform_started_at, run.transform_finished_at
        )
    return measures
