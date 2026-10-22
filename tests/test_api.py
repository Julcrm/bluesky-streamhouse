"""Tests for src.serving.api: the dashboard API, its pure computations and its frozen SQL
(run against the local Postgres when the stack is up)."""

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from src import config
from src.alternation import calendar as cal
from src.serving import api

T0 = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
KEY = {"X-API-Key": "test-key"}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(config, "API_KEY", "test-key")
    api._cache.clear()
    return TestClient(api.app)


def test_health_without_key(client) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_data_endpoints_need_the_key(client) -> None:
    # 401 without the header, 403 with a wrong key: refused either way
    assert client.get("/branches/compare").status_code in (401, 403)
    assert client.get("/branches/compare", headers={"X-API-Key": "wrong"}).status_code == 403


def test_no_key_configured_refuses_everyone(client, monkeypatch) -> None:
    monkeypatch.setattr(config, "API_KEY", "")
    assert client.get("/storage", headers={"X-API-Key": ""}).status_code in (401, 403)


def test_only_the_rolling_windows_of_the_dashboard(client, monkeypatch) -> None:
    monkeypatch.setattr(api, "query", lambda sql, params=(): [])
    assert client.get("/branches/daily?days=7", headers=KEY).status_code == 200
    assert client.get("/branches/daily?days=3650", headers=KEY).status_code == 422


def test_no_hypotheses_endpoint_during_the_period(client) -> None:
    """Decision of Julien on 2026-10-22: the figures only, no verdict published live."""
    assert client.get("/hypotheses", headers=KEY).status_code == 404


def test_rate_between_two_readings() -> None:
    assert api.rate((100.0, 1_000), (110.0, 5_640)) == pytest.approx(464)
    assert api.rate((100.0, 1_000), (100.0, 2_000)) is None
    assert api.rate((100.0, 5_000), (110.0, 1_000)) is None  # topic recreated


def _sample(seconds: int, service: str, cpu_s: float, mib: float, cid: str = "a") -> dict:
    branch, layer = config.BENCHMARK_SERVICES[service]
    return {
        "ts": T0 + timedelta(seconds=seconds),
        "service": service,
        "branch": branch,
        "layer": layer,
        "container_id": cid,
        "cpu_usec": int(cpu_s * 1e6),
        "anon_bytes": int(mib * 2**20),
    }


def test_resources_add_the_services_of_a_layer() -> None:
    """Over one minute: spark-thrift at 1 core and bluesky-spark at 0.5 core make the
    transform layer's 1.5 cores; memories add up too."""
    rows = [_sample(t, "spark-thrift", t * 1.0, 1000) for t in range(0, 60, 10)]
    rows += [_sample(t, "bluesky-spark", t * 0.5, 200) for t in range(0, 60, 10)]
    [point] = api.resources_series(rows)
    assert (point["branch"], point["layer"]) == ("spark", "transform")
    assert point["cpu_cores"] == pytest.approx(1.5)
    assert point["anon_mib"] == pytest.approx(1200)


def test_resources_skip_a_container_restart() -> None:
    rows = [_sample(0, "quix", 0, 300), _sample(10, "quix", 5, 300)]
    rows += [_sample(20, "quix", 0.1, 300, cid="b"), _sample(30, "quix", 1.1, 300, cid="b")]
    [point] = api.resources_series(rows)
    # 5 s over the first 10 s, then 1 s over the last 10: the restart pair is not a delta
    assert point["cpu_cores"] == pytest.approx(6 / 20)


def test_live_status_without_dagster(client, monkeypatch) -> None:
    day = {"day": T0.date(), "branch": "spark", "status": "open"}
    monkeypatch.setattr(api, "query", lambda sql, params=(): [day])

    def down(_query: str) -> dict:
        raise ConnectionError("dagster down")

    monkeypatch.setattr(api, "dagster", down)
    body = client.get("/live/status", headers=KEY).json()
    assert body["active_branch"] == "spark" and body["last_silver_gold"] == {}


def test_live_status_with_the_last_passes(client, monkeypatch) -> None:
    monkeypatch.setattr(api, "query", lambda sql, params=(): [])
    start = int(T0.timestamp())
    monkeypatch.setattr(
        api,
        "dagster",
        lambda _query: {
            "duckdb": {
                "results": [{"status": "SUCCESS", "startTime": start, "endTime": start + 90}]
            },
            "spark": {"results": []},
        },
    )
    body = client.get("/live/status", headers=KEY).json()
    assert body["last_silver_gold"]["duckdb"]["minutes"] == 1.5
    assert "spark" not in body["last_silver_gold"]


def test_throughput_reads_twice_without_a_recent_reading(client, monkeypatch) -> None:
    readings = iter([1_000, 2_392])
    clock = iter([100.0, 103.0])
    monkeypatch.setattr(api, "topic_offsets", lambda: next(readings))
    monkeypatch.setattr(api, "_clock", lambda: next(clock))
    monkeypatch.setattr(api, "_pause", lambda s: None)
    api._last_reading.clear()
    body = client.get("/live/throughput", headers=KEY).json()
    assert body["messages_per_second"] == pytest.approx(464)


def test_answers_are_cached(client, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(api, "query", lambda sql, params=(): calls.append(sql) or [])
    client.get("/storage?days=30", headers=KEY)
    client.get("/storage?days=30", headers=KEY)
    assert len(calls) == 1


def test_frozen_sql_runs_on_the_benchmark_schema(monkeypatch) -> None:
    """Every query of the API against empty benchmark tables: catches a wrong column or a
    syntax error before the freeze."""
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    from src.benchmark import collector, runs, storage, windows

    test_db = "bluesky_benchmark_api_test"
    admin = cal.connect_benchmark("postgres")
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {test_db}")
    admin.close()
    monkeypatch.setattr(config, "BENCHMARK_DB", test_db)
    conn = cal.connect_benchmark()
    try:
        with conn.cursor() as cur:
            for statement in collector.DDL + windows.DDL:
                cur.execute(statement)
            cur.execute(storage.DDL)
        cal.CalendarStore(conn).ensure_table()
        runs.BenchStore(conn).ensure_table()
    finally:
        conn.close()
    for sql in (
        api.COMPARE_SQL,
        api.CATALOG_SQL,
        api.WINDOWS_SQL,
        api.DAILY_SQL,
        api.BENCH_SQL,
        api.STORAGE_SQL,
        api.RESOURCES_SQL,
    ):
        assert api.query(sql, (7,)) == []
    assert api.query(api.CALENDAR_SQL) == []


def test_a_table_not_created_yet_gives_no_rows(monkeypatch) -> None:
    """bucket_samples only exists after its first night: /storage answers empty, not 500."""
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    test_db = "bluesky_benchmark_api_empty"
    admin = cal.connect_benchmark("postgres")
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {test_db}")
    admin.close()
    monkeypatch.setattr(config, "BENCHMARK_DB", test_db)
    assert api.query(api.STORAGE_SQL, (30,)) == []
    assert api.query(api.COMPARE_SQL, (7,)) == []
