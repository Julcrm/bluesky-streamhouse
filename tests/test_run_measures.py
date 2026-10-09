"""Tests for the measures of a controlled-test run (src.benchmark.run_measures, D36)."""

import random
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from src.benchmark import run_measures as rm
from src.benchmark import runs
from src.benchmark.windows import MIB, Sample

T0 = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


def _s(seconds: float, cpu_ms: float, anon_mib: float, cid: str = "c1") -> Sample:
    return Sample(
        T0 + timedelta(seconds=seconds), cid, int(cpu_ms * 1000), int(anon_mib * MIB), 0, 0, 0
    )


def test_interval_takes_each_pair_in_proportion_to_its_overlap() -> None:
    """10 s pairs, 100 ms of CPU each, 100 MiB flat; [5 s, 25 s] covers half of the first
    pair, the second, half of the third."""
    samples = [_s(0, 0, 100), _s(10, 100, 100), _s(20, 200, 100), _s(30, 300, 100)]
    usage = rm.interval_usage(samples, T0 + timedelta(seconds=5), T0 + timedelta(seconds=25))
    assert usage.cpu_ms == pytest.approx(200)
    assert usage.ram_mib_s == pytest.approx(100 * 20)
    assert usage.covered_s == pytest.approx(20)
    assert usage.peak_anon_mib == pytest.approx(100)


def test_restart_and_gap_are_not_integrated() -> None:
    samples = [_s(0, 0, 100), _s(10, 50, 100, cid="c2"), _s(60, 80, 100, cid="c2")]
    usage = rm.interval_usage(samples, T0, T0 + timedelta(seconds=60))
    assert usage.restarted and usage.cpu_ms == 0 and usage.covered_s == 0


def test_percentile_matches_duckdb_quantile_cont() -> None:
    values = sorted(random.Random(7).randint(0, 5000) for _ in range(1001))
    duck = duckdb.connect()
    duck.execute("CREATE TABLE v AS SELECT unnest(?) AS x", [values])
    for q in (0.5, 0.95):
        expected = duck.execute(f"SELECT quantile_cont(x, {q}) FROM v").fetchone()[0]
        assert rm.percentile(values, q) == pytest.approx(expected)


def test_throughput_and_latency_from_the_first_copy() -> None:
    """Throughput over the write window; a duplicate counts once at its first write."""
    arrivals = [(0, i, 10_000 + 10 * i) for i in range(101)] + [(0, 5, 99_999)]
    sends = {(0, i): 10_000 + 10 * i - 200 for i in range(101)}
    m = rm.arrival_measures(arrivals, sends)
    assert m["bronze_messages"] == 101
    assert m["throughput_msg_s"] == pytest.approx(101 / 1.0)
    assert m["latency_p50_ms"] == 200 and m["latency_p95_ms"] == 200


def test_no_latency_without_send_times() -> None:
    m = rm.arrival_measures([(0, 0, 1000), (1, 0, 3000)], None)
    assert m["throughput_msg_s"] == pytest.approx(1.0) and "latency_p50_ms" not in m


def test_store_records_measures_and_the_view_computes_ratios() -> None:
    from tests.test_bench_runs import _store

    store, conn = _store()
    try:
        run = store.request("duckdb", T0.date(), "4x", 1, T0)
        store.record_transform(run.run_id, T0, T0 + timedelta(seconds=100), 2)
        store.record_measures(
            run.run_id,
            {
                "bronze_messages": 10_000,
                "throughput_msg_s": 1600.0,
                "latency_p50_ms": 900.0,
                "latency_p95_ms": 2000.0,
                "latency_relevant": True,
                "ingest_usage": {"streaming": {"cpu_ms": 5000.0, "ram_mib_s": 30000.0}},
                "transform_usage": {"transform": {"cpu_ms": 20000.0, "ram_mib_s": 1000.0}},
                "unknown": 1,
            },
        )
        store.finish(run.run_id, runs.DONE, T0)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ingest_cpu_ms_per_message, ingest_ram_mib_s_per_10k_messages, "
                "transform_cpu_ms_per_message, transform_rows_per_s, latency_p95_ms "
                "FROM bench_ratios WHERE run_id = %s",
                (run.run_id,),
            )
            row = cur.fetchone()
        assert row == pytest.approx((0.5, 30000.0, 2.0, 100.0, 2000.0))
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS bench_ratios")
            cur.execute("DROP TABLE IF EXISTS bench_runs")
        conn.close()
