"""
FastAPI application of the portfolio dashboard (phase 8): both branches live and over
rolling days.

Read only, from the benchmark database, Redpanda's offsets and Dagster: the API never
queries the lakehouses, which would load the engines it reports on. Live figures (branch,
input rate, CPU and memory per layer) are at most a few seconds old; per-message figures
(cost, latency) come from the 5-minute windows built at each day's close (D34). Answers
are cached, so the dashboard's visitors cost nothing measurable; the API itself is
sampled by the collector as a service of the benchmark.

No verdict on the hypotheses during the measurement period (decision of Julien on
2026-10-22): the figures only.

Security: X-API-Key header on the data endpoints, as velib's API.
"""

import secrets
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from typing import Any

import psycopg2.errors
import requests
from fastapi import Depends, FastAPI, HTTPException, Query, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader

from src import config
from src.alternation import calendar as cal

app = FastAPI(title="Bluesky Streamhouse API", docs_url=None, redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


def verify_api_key(key: str = Security(api_key_header)) -> None:
    if not config.API_KEY or not secrets.compare_digest(key, config.API_KEY):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API Key")


# --- Cache ----------------------------------------------------------------------

_cache: dict[tuple, tuple[float, Any]] = {}


def cached(seconds: int) -> Callable:
    """Keep a function's answer per arguments for `seconds`."""

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = (fn.__name__, args, tuple(sorted(kwargs.items())))
            hit = _cache.get(key)
            if hit is not None and time.monotonic() - hit[0] < seconds:
                return hit[1]
            value = fn(*args, **kwargs)
            _cache[key] = (time.monotonic(), value)
            return value

        return wrapper

    return decorator


# --- Sources --------------------------------------------------------------------


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Rows of the benchmark database as dicts (a short session per call). A table not
    created yet (bucket_samples before its first night, windows before a first close, a
    database just reset) gives no rows, not an error."""
    conn = cal.connect_benchmark()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, params)
            except psycopg2.errors.UndefinedTable:
                return []
            columns = [c.name for c in cur.description]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    finally:
        conn.close()


def dagster(graphql: str) -> dict:
    response = requests.post(config.DAGSTER_GRAPHQL_URL, json={"query": graphql}, timeout=10)
    response.raise_for_status()
    return response.json()["data"]


def topic_offsets() -> int:
    """Sum of the high watermarks of raw_events (messages ever written)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "bluesky-api",
            "enable.auto.commit": False,
            **config.KAFKA_CLIENT_CONFIG,
        }
    )
    try:
        topic = consumer.list_topics(config.RAW_EVENTS_TOPIC, timeout=10).topics[
            config.RAW_EVENTS_TOPIC
        ]
        return sum(
            consumer.get_watermark_offsets(
                TopicPartition(config.RAW_EVENTS_TOPIC, partition), timeout=10
            )[1]
            for partition in topic.partitions
        )
    finally:
        consumer.close()


# --- Pure computations (tested without the stack) -------------------------------


def rate(previous: tuple[float, int], current: tuple[float, int]) -> float | None:
    """Messages per second between two (time, offsets) readings."""
    seconds = current[0] - previous[0]
    if seconds <= 0 or current[1] < previous[1]:
        return None
    return (current[1] - previous[1]) / seconds


def resources_series(rows: list[dict]) -> list[dict]:
    """Per minute, branch and layer: CPU in cores and mean real memory (MiB), from the
    collector's cumulative samples (a container restart, new id, is not a delta). A
    layer's figures are the sums of its services'."""
    by_service: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_service[row["service"]].append(row)
    # (minute, branch, layer, service) -> [CPU seconds, elapsed seconds, memory samples]
    per_service: dict[tuple, list] = defaultdict(lambda: [0.0, 0.0, []])
    for service, samples in by_service.items():
        samples.sort(key=lambda r: r["ts"])
        for a, b in zip(samples, samples[1:], strict=False):
            if a["container_id"] != b["container_id"] or None in (a["cpu_usec"], b["cpu_usec"]):
                continue
            dt = (b["ts"] - a["ts"]).total_seconds()
            if dt <= 0 or b["cpu_usec"] < a["cpu_usec"]:
                continue
            entry = per_service[
                (b["ts"].replace(second=0, microsecond=0), b["branch"], b["layer"], service)
            ]
            entry[0] += (b["cpu_usec"] - a["cpu_usec"]) / 1e6
            entry[1] += dt
            if b["anon_bytes"] is not None:
                entry[2].append(b["anon_bytes"] / 2**20)
    layers: dict[tuple, list[float]] = defaultdict(lambda: [0.0, 0.0])
    for (minute, branch, layer, _service), (cpu_s, elapsed, memory) in per_service.items():
        layer_entry = layers[(minute, branch, layer)]
        layer_entry[0] += cpu_s / elapsed
        if memory:
            layer_entry[1] += sum(memory) / len(memory)
    return [
        {
            "minute": minute,
            "branch": branch,
            "layer": layer,
            "cpu_cores": round(cores, 3),
            "anon_mib": round(mib, 1),
        }
        for (minute, branch, layer), (cores, mib) in sorted(layers.items())
    ]


def _days(days: int) -> int:
    if days not in config.API_ROLLING_DAYS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"days must be one of {config.API_ROLLING_DAYS}",
        )
    return days


# --- Frozen SQL (the dashboard's figures; over the measurement period, those of the
# hypotheses) --------------------------------------------------------------------

COMPARE_SQL = """
SELECT branch, layer, regime, rate_bucket,
       count(*) AS windows,
       sum(messages) AS messages,
       sum(cpu_ms_per_message * messages) / nullif(sum(messages), 0) AS cpu_ms_per_message,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY cpu_ms_per_message) AS cpu_ms_per_message_p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY cpu_ms_per_message) AS cpu_ms_per_message_p95,
       sum(ram_mib_s_per_10k_messages * messages) / nullif(sum(messages), 0)
           AS ram_mib_s_per_10k_messages,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_p50_ms) AS latency_p50_ms,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_p95_ms) AS latency_p95_ms,
       max(peak_anon_mib) AS peak_anon_mib
FROM benchmark_ratios
WHERE day >= current_date - %s
GROUP BY branch, layer, regime, rate_bucket
ORDER BY branch, layer, regime, rate_bucket
"""

# Each catalog's estimated CPU (share of the shared Postgres) per message of its branch,
# on the windows the ratios keep
CATALOG_SQL = """
SELECT c.branch, b.regime, b.rate_bucket,
       count(*) AS windows,
       sum(c.cpu_ms_estimate) / nullif(sum(b.messages), 0) AS cpu_ms_per_message_estimate,
       avg(c.active_share) AS active_share
FROM catalog_windows AS c
JOIN branch_windows AS b ON b.window_start = c.window_start AND b.branch = c.branch
WHERE c.day >= current_date - %s AND NOT b.host_contaminated
  AND c.cpu_ms_estimate IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM service_windows AS r
    WHERE r.window_start = b.window_start AND r.branch = b.branch AND r.restarted)
GROUP BY c.branch, b.regime, b.rate_bucket
ORDER BY c.branch, b.regime, b.rate_bucket
"""

# Windows of each branch and why some are left out of the ratios
WINDOWS_SQL = """
SELECT b.branch,
       count(*) AS windows,
       count(*) FILTER (WHERE b.host_contaminated) AS contaminated,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM service_windows AS r
           WHERE r.window_start = b.window_start AND r.branch = b.branch AND r.restarted))
           AS restarted
FROM branch_windows AS b
WHERE b.day >= current_date - %s
GROUP BY b.branch
ORDER BY b.branch
"""

DAILY_SQL = """
SELECT * FROM branch_daily WHERE day >= current_date - %s
ORDER BY day, branch, layer, regime, rate_bucket
"""

BENCH_SQL = """
SELECT * FROM bench_ratios WHERE night >= current_date - %s
ORDER BY night, branch, rate, repetition
"""

STORAGE_SQL = """
SELECT ts, prefix, branch, bytes, objects FROM bucket_samples
WHERE ts >= now() - make_interval(days => %s)
ORDER BY ts, prefix
"""

CALENDAR_SQL = """
SELECT day, coalesce(forced_branch, branch) AS branch, status, opened_at, caught_up_at,
       closed_at, stopped_at, late_at
FROM branch_calendar ORDER BY day DESC LIMIT 2
"""

RESOURCES_SQL = """
SELECT ts, service, branch, layer, container_id, cpu_usec, anon_bytes
FROM container_samples
WHERE ts >= now() - make_interval(mins => %s) AND branch IS NOT NULL
ORDER BY ts
"""

LAST_PASSES_GRAPHQL = """
query {
  %s
}
"""
_PASS = (
    '%s: runsOrError(filter: {pipelineName: "%s", statuses: [SUCCESS, FAILURE]}, limit: 1)'
    " { ... on Runs { results { status startTime endTime } } }"
)
SILVER_GOLD_JOBS = {"duckdb": "bluesky_silver_gold", "spark": "bluesky_spark_silver_gold"}


# --- Endpoints ------------------------------------------------------------------


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@cached(config.API_LIVE_CACHE_SECONDS)
def _live_status() -> dict:
    days = query(CALENDAR_SQL)
    passes: dict[str, Any] = {}
    try:
        data = dagster(
            LAST_PASSES_GRAPHQL % " ".join(_PASS % (b, job) for b, job in SILVER_GOLD_JOBS.items())
        )
        for branch in SILVER_GOLD_JOBS:
            results = data[branch]["results"]
            if results:
                run = results[0]
                passes[branch] = {
                    "status": run["status"],
                    "started_at": datetime.fromtimestamp(run["startTime"], UTC),
                    "minutes": round((run["endTime"] - run["startTime"]) / 60, 1)
                    if run["endTime"]
                    else None,
                }
    except Exception:  # noqa: BLE001 - Dagster down: the status without the passes
        passes = {}
    return {
        "now": datetime.now(UTC),
        "active_branch": days[0]["branch"] if days else None,
        "days": days,
        "last_silver_gold": passes,
    }


@app.get("/live/status", dependencies=[Depends(verify_api_key)])
def live_status() -> dict:
    """The day that runs, the previous one, and each branch's last Silver/Gold pass."""
    return _live_status()


_last_reading: list[tuple[float, int]] = []


def _clock() -> float:
    return time.time()


def _pause(seconds: float) -> None:
    time.sleep(seconds)


@cached(config.API_LIVE_CACHE_SECONDS)
def _live_throughput() -> dict:
    now = (_clock(), topic_offsets())
    previous = _last_reading[-1] if _last_reading else None
    if previous is None or now[0] - previous[0] > 600:
        # No recent reading: a second one a few seconds later
        _pause(3)
        previous, now = now, (_clock(), topic_offsets())
    _last_reading[:] = [now]
    return {"at": datetime.fromtimestamp(now[0], UTC), "messages_per_second": rate(previous, now)}


@app.get("/live/throughput", dependencies=[Depends(verify_api_key)])
def live_throughput() -> dict:
    """Messages per second entering Redpanda (the same input for both branches)."""
    return _live_throughput()


@cached(config.API_LIVE_CACHE_SECONDS)
def _live_resources() -> list[dict]:
    return resources_series(query(RESOURCES_SQL, (config.API_LIVE_MINUTES,)))


@app.get("/live/resources", dependencies=[Depends(verify_api_key)])
def live_resources() -> dict:
    """CPU (cores) and real memory per branch and layer, minute by minute."""
    return {"minutes": config.API_LIVE_MINUTES, "series": _live_resources()}


@cached(config.API_ROLLING_CACHE_SECONDS)
def _compare(days: int) -> dict:
    return {
        "days": days,
        "layers": query(COMPARE_SQL, (days,)),
        "catalogs": query(CATALOG_SQL, (days,)),
        "windows": query(WINDOWS_SQL, (days,)),
    }


@app.get("/branches/compare", dependencies=[Depends(verify_api_key)])
def branches_compare(days: int = Query(7)) -> dict:
    """Per branch, layer, regime and rate bucket over the last `days`: cost per message,
    memory, latency; the catalogs' estimate; the windows kept and left out."""
    return _compare(_days(days))


@cached(config.API_ROLLING_CACHE_SECONDS)
def _daily(days: int) -> list[dict]:
    return query(DAILY_SQL, (days,))


@app.get("/branches/daily", dependencies=[Depends(verify_api_key)])
def branches_daily(days: int = Query(7)) -> dict:
    """The same figures day by day."""
    return {"days": days, "rows": _daily(_days(days))}


@cached(config.API_ROLLING_CACHE_SECONDS)
def _bench(days: int) -> list[dict]:
    return query(BENCH_SQL, (days,))


@app.get("/bench/nights", dependencies=[Depends(verify_api_key)])
def bench_nights(days: int = Query(30)) -> dict:
    """Controlled-test nights: each run's throughput, latency, CPU and memory."""
    return {"days": days, "runs": _bench(_days(days))}


@cached(config.API_ROLLING_CACHE_SECONDS)
def _storage(days: int) -> list[dict]:
    return query(STORAGE_SQL, (days,))


@app.get("/storage", dependencies=[Depends(verify_api_key)])
def storage(days: int = Query(30)) -> dict:
    """Bucket usage per prefix and branch, each night before the maintenances."""
    return {"days": days, "samples": _storage(_days(days))}
