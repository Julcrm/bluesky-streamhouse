"""Tests for src.benchmark.windows: window arithmetic checked by hand, and the Bronze
messages query run by DuckDB (the Spark version is checked against the local Thrift
server separately, it needs the Spark stack)."""

from datetime import UTC, date, datetime, timedelta

import duckdb
import pytest

from src import config
from src.alternation import calendar as cal
from src.benchmark import windows as w

T0 = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)  # a window start
MIB = 2**20


def _s(seconds: int, cpu: int, anon_mib: float, cid: str = "a", psi: int = 0) -> w.Sample:
    return w.Sample(T0 + timedelta(seconds=seconds), cid, cpu, int(anon_mib * MIB), 0, 0, psi)


def test_window_start_is_aligned() -> None:
    assert w.window_start(T0 + timedelta(seconds=299)) == T0
    assert w.window_start(T0 + timedelta(seconds=300)) == T0 + timedelta(minutes=5)


def test_cpu_and_ram_integral_by_hand() -> None:
    """100 MiB then 200 MiB over 10 s: trapezoid = 150 MiB × 10 s = 1 500 MiB·s;
    2 s of CPU in µs = 2 000 ms."""
    windows = w.service_windows([_s(0, 0, 100), _s(10, 2_000_000, 200)])
    win = windows[T0]
    assert win.cpu_ms == pytest.approx(2000)
    assert win.ram_mib_s == pytest.approx(1500)
    assert win.covered_s == pytest.approx(10)
    assert win.peak_anon_mib == pytest.approx(200)
    assert not win.restarted


def test_pair_across_two_windows_is_split_by_time() -> None:
    """Samples at 295 s and 305 s (a late tick): half of everything in each window."""
    windows = w.service_windows([_s(295, 0, 100), _s(305, 1_000_000, 100)])
    first, second = windows[T0], windows[T0 + timedelta(minutes=5)]
    assert first.cpu_ms == pytest.approx(500) and second.cpu_ms == pytest.approx(500)
    assert first.ram_mib_s == pytest.approx(500) and second.ram_mib_s == pytest.approx(500)


def test_restart_is_flagged_and_not_counted() -> None:
    """A new container id resets the counters: the pair is dropped, the window flagged."""
    windows = w.service_windows([_s(0, 5_000_000, 100), _s(10, 1_000, 100, cid="b")])
    assert windows[T0].restarted and windows[T0].cpu_ms == 0


def test_collector_gap_lowers_coverage() -> None:
    windows = w.service_windows([_s(0, 0, 100), _s(10, 0, 100), _s(120, 0, 100)])
    assert windows[T0].covered_s == pytest.approx(10)


def test_host_steal_share_and_contamination(monkeypatch) -> None:
    def h(seconds: int, total: int, steal: int, avail_mib: int = 4096) -> w.HostSample:
        return w.HostSample(T0 + timedelta(seconds=seconds), total, steal, avail_mib * MIB, 0, 0, 0)

    clean = w.host_windows([h(0, 0, 0), h(300, 1000, 10)])[T0]
    assert clean.steal_pct == pytest.approx(1.0)
    assert not w.host_contaminated(clean)
    stolen = w.host_windows([h(0, 0, 0), h(300, 1000, 80)])[T0]
    assert w.host_contaminated(stolen)  # 8 % > 5 %
    tight = w.host_windows([h(0, 0, 0), h(300, 1000, 0, avail_mib=300)])[T0]
    assert w.host_contaminated(tight)
    assert w.host_contaminated(None)


def test_rate_buckets() -> None:
    assert w.rate_bucket(150 * 300) == "0-200"
    assert w.rate_bucket(250 * 300) == "200-500"
    assert w.rate_bucket(600 * 300) == "500+"


def _day(**kwargs) -> cal.CalendarDay:
    base = dict(
        day=date(2026, 10, 8),
        branch="spark",
        forced_branch=None,
        start_offsets={0: 0, 1: 0},
        end_offsets={0: 3, 1: 2},
        offsets_source="watermark",
        status=cal.DONE,
    )
    return cal.CalendarDay(**{**base, **kwargs})


def test_regime_switches_at_caught_up() -> None:
    day = _day(caught_up_at=T0 + timedelta(minutes=7))
    assert w.regime(T0, day) == "catchup"
    assert w.regime(T0 + timedelta(minutes=5), day) == "live"
    assert w.regime(T0, _day()) == "catchup"


def test_messages_query_in_duckdb_counts_distinct_offsets_of_the_day() -> None:
    """A replayed duplicate counts once (first copy), offsets outside the day not at all."""
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE bronze (kafka_partition INT, kafka_offset BIGINT, collection TEXT, "
        "event_time TIMESTAMPTZ, processed_at TIMESTAMPTZ)"
    )
    rows = [
        (0, 0, "like", "2026-10-08 12:00:00+00", "2026-10-08 12:00:01+00"),  # 1 s
        (0, 1, "like", "2026-10-08 12:00:00+00", "2026-10-08 12:00:02+00"),  # 2 s
        (0, 1, "like", "2026-10-08 12:00:00+00", "2026-10-08 12:03:00+00"),  # replay
        (0, 2, "post", "2026-10-08 12:00:00+00", "2026-10-08 12:00:03+00"),  # 3 s
        (1, 1, "post", "2026-10-08 12:04:00+00", "2026-10-08 12:06:00+00"),  # next window
        (1, 5, "post", "2026-10-08 12:00:00+00", "2026-10-08 12:00:01+00"),  # not the day's
    ]
    conn.executemany("INSERT INTO bronze VALUES (?, ?, ?, ?, ?)", rows)
    result = w.message_windows(conn.execute(w.messages_query_duckdb("bronze", _day())).fetchall())
    first = result[T0]
    assert first.messages == 3
    assert first.latency_p50_ms == pytest.approx(2000)
    assert first.latency_p95_ms == pytest.approx(2900)  # linear interpolation 2 s .. 3 s
    assert first.collections == {"like": 2, "post": 1}
    assert result[T0 + timedelta(minutes=5)].messages == 1


def test_histogram_percentiles_equal_quantile_cont() -> None:
    """The histogram (which keeps Spark's heap bounded) gives exactly quantile_cont over
    the same whole-millisecond latencies, ties and odd sizes included."""
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE bronze AS SELECT 0 AS kafka_partition, i::BIGINT AS kafka_offset, "
        "CASE WHEN i % 3 = 0 THEN 'post' ELSE 'like' END AS collection, "
        "TIMESTAMPTZ '2026-10-08 12:00:00+00' + INTERVAL (i) SECOND AS event_time, "
        "TIMESTAMPTZ '2026-10-08 12:00:00+00' + INTERVAL (i) SECOND "
        "+ INTERVAL ((hash(i) % 5000)::INT) MILLISECOND AS processed_at "
        "FROM range(997) t(i)"
    )
    day = _day(start_offsets={0: 0}, end_offsets={0: 997})
    got = w.message_windows(conn.execute(w.messages_query_duckdb("bronze", day)).fetchall())
    expected = conn.execute(
        "SELECT floor(epoch(processed_at))::BIGINT // 300 * 300, collection, count(*), "
        "quantile_cont(epoch_ms(processed_at) - epoch_ms(event_time), 0.5), "
        "quantile_cont(epoch_ms(processed_at) - epoch_ms(event_time), 0.95) "
        "FROM bronze GROUP BY GROUPING SETS ((1), (1, 2))"
    ).fetchall()
    assert len(got) >= 3
    for window_epoch, collection, messages, p50, p95 in expected:
        window = got[datetime.fromtimestamp(window_epoch, UTC)]
        if collection is None:
            assert (window.messages, window.latency_p50_ms, window.latency_p95_ms) == (
                messages,
                pytest.approx(p50),
                pytest.approx(p95),
            )
        else:
            assert window.collections[collection] == messages


def test_ratio_view_only_uses_clean_windows() -> None:
    view = w.DDL[-1]
    assert "NOT b.host_contaminated AND NOT s.restarted" in view
    assert f"{config.BENCHMARK_WINDOW_SECONDS} * {config.BENCHMARK_MIN_COVERAGE}" in view


def test_rebuild_day_writes_windows_and_the_ratio_view(monkeypatch) -> None:
    """Samples of one Spark container and the host over 10 min, Bronze rows of 2 windows:
    rows in both tables, ratios in the view, and a second rebuild gives the same rows."""
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    from src.benchmark import collector

    test_db = "bluesky_benchmark_test"
    admin = cal.connect_benchmark("postgres")
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {test_db}")
    admin.close()
    monkeypatch.setattr(config, "BENCHMARK_DB", test_db)
    conn = collector.connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS benchmark_ratios")
            for table in (
                "container_samples",
                "host_samples",
                "catalog_samples",
                "service_windows",
                "branch_windows",
            ):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            for statement in collector.DDL:
                cur.execute(statement)
            for i in range(61):  # every 10 s over 10 min: 1 CPU second per tick, 500 MiB
                ts = T0 + timedelta(seconds=10 * i)
                cur.execute(
                    "INSERT INTO container_samples VALUES (%s, 'spark', 'spark', 'streaming', "
                    "'c1', %s, %s, 0, 0, 0, 0, 0, 0)",
                    (ts, i * 1_000_000, 500 * MIB),
                )
                cur.execute(
                    "INSERT INTO host_samples VALUES (%s, %s, %s, %s, 0, 0, 0, 0)",
                    (ts, i * 100, 0, 4096 * MIB),
                )
        day = _day(opened_at=T0, stopped_at=T0 + timedelta(minutes=10))
        epoch = int(T0.timestamp())
        rows = [
            (epoch, None, 600, 900.0, 1500.0),
            (epoch, "app.bsky.feed.like", 600, None, None),
            (epoch + 300, None, 300, 800.0, 1200.0),
        ]
        first = w.rebuild_day(conn, day, rows)
        assert first == w.rebuild_day(conn, day, rows)  # idempotent
        with conn.cursor() as cur:
            cur.execute(
                "SELECT window_start, layer, messages, cpu_ms_per_message, "
                "ram_mib_s_per_10k_messages FROM benchmark_ratios ORDER BY window_start"
            )
            ratios = cur.fetchall()
        # 30 CPU seconds and 500 MiB × 300 s per 5-minute window
        assert [r[2] for r in ratios] == [600, 300]
        assert ratios[0][3] == pytest.approx(30_000 / 600)
        assert ratios[0][4] == pytest.approx(500 * 300 * 10_000 / 600)
        assert ratios[1][3] == pytest.approx(30_000 / 300)
    finally:
        conn.close()
