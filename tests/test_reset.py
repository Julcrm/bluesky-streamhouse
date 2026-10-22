"""Tests for the reset before the measurement period (src.benchmark.reset, plan of the
freeze, option B). The local-stack tests work on dedicated `reset_test` objects, never on
the local lakehouses."""

import pytest

from src import config
from src.benchmark import reset
from tests.test_resources_ducklake import _local_stack_up


@pytest.mark.parametrize(
    "name",
    [
        "branch_calendar",
        "raw_events",
        config.BENCH_SAMPLE_TOPIC,
        "default",
        config.DUCKLAKE_BENCH_BRONZE_METADATA_SCHEMA,
        config.DUCKLAKE_BENCH_TRANSFORM_DATA_PATH,
        "bench_silver",
        "velib_lakehouse",
    ],
)
def test_guard_refuses_protected_bench_and_velib_objects(name) -> None:
    with pytest.raises(ValueError):
        reset._guard((name,))


def test_defaults_pass_the_guard_and_name_only_bluesky_production() -> None:
    reset._guard(
        reset.DUCKLAKE_SCHEMAS
        + reset.DUCKLAKE_PATHS
        + reset.ICEBERG_NAMESPACES
        + reset.MEASURE_TABLES
    )
    assert "branch_calendar" not in reset.MEASURE_TABLES
    assert all("/ducklake/" in p and "/bench/" not in p for p in reset.DUCKLAKE_PATHS)


def _fake_parts(monkeypatch) -> list[str]:
    calls: list[str] = []
    for name in (
        "reset_ducklake",
        "ducklake_plan",
        "reset_iceberg",
        "iceberg_tables",
        "reset_measures",
        "measures_plan",
    ):
        monkeypatch.setattr(reset, name, lambda *a, _n=name, **k: calls.append(_n) or {})
    return calls


@pytest.mark.parametrize("confirm", [None, "yes", "spark"])
def test_dry_run_unless_the_part_is_repeated(monkeypatch, confirm) -> None:
    calls = _fake_parts(monkeypatch)
    result = reset.run("duckdb", confirm, with_dagster=False)
    assert not result["executed"]
    assert calls == ["ducklake_plan"]


def test_confirmed_part_deletes_only_that_part(monkeypatch) -> None:
    calls = _fake_parts(monkeypatch)
    assert reset.run("measures", "measures", with_dagster=False)["executed"]
    assert calls == ["reset_measures"]


def test_branch_parts_run_in_their_own_code_location(monkeypatch) -> None:
    _fake_parts(monkeypatch)
    monkeypatch.setattr(config, "DAGSTER_CODE_LOCATION", "bluesky_duckdb")
    with pytest.raises(SystemExit):
        reset.run("spark", None)


def test_reset_ducklake_drops_its_catalog_only() -> None:
    """On the local stack: a test catalog is dropped whole (metadata and files); the
    local production Bronze keeps its table."""
    from src.resources.ducklake import DuckLakeSettings, connect

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    test = DuckLakeSettings(
        metadata_schema="reset_test", data_path=f"s3://{config.BUCKET}/reset_test/ducklake/"
    )
    conn = connect(test)
    conn.execute("CREATE TABLE IF NOT EXISTS bronze.main.probe AS SELECT 1 AS x")
    conn.execute("CALL ducklake_flush_inlined_data('bronze')")
    conn.close()
    plan = reset.ducklake_plan(("reset_test",), (test.data_path,))
    assert plan["schema reset_test (metadata tables)"] > 0
    assert plan[f"files under {test.data_path}"] >= 1
    deleted = reset.reset_ducklake(("reset_test",), (test.data_path,))
    assert deleted[test.data_path] >= 1
    after = reset.ducklake_plan(("reset_test",), (test.data_path,))
    assert after == {"schema reset_test (metadata tables)": 0, f"files under {test.data_path}": 0}
    prod = connect(DuckLakeSettings(), read_only=True)
    try:
        tables = prod.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'bronze'"
        ).fetchall()
    finally:
        prod.close()
    assert ("bronze_events",) in tables


def _lakekeeper():
    import requests

    try:
        return reset._catalog()
    except requests.ConnectionError:
        pytest.skip("local Lakekeeper not running (make up)")


def test_reset_iceberg_drops_the_namespace_tables_only() -> None:
    """On the local stack: a table of a test namespace is dropped through the REST API;
    the namespace stays, and the Bronze namespace keeps its tables."""
    url, session = _lakekeeper()
    session.post(f"{url}/namespaces", json={"namespace": ["reset_test"]}, timeout=30)
    created = session.post(
        f"{url}/namespaces/reset_test/tables",
        json={
            "name": "probe",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [{"id": 1, "name": "x", "required": False, "type": "long"}],
            },
        },
        timeout=30,
    )
    assert created.status_code in (200, 409), created.text
    bronze_before = reset.iceberg_tables(("bronze",))
    assert reset.iceberg_tables(("reset_test",)) == [("reset_test", "probe")]
    assert reset.reset_iceberg(("reset_test",)) == ["reset_test.probe"]
    assert reset.iceberg_tables(("reset_test",)) == []
    assert session.get(f"{url}/namespaces/reset_test", timeout=30).status_code == 200
    assert reset.iceberg_tables(("bronze",)) == bronze_before
    session.delete(f"{url}/namespaces/reset_test", timeout=30)


def test_reset_measures_empties_its_tables_and_keeps_the_calendar() -> None:
    """On the local stack: a test table of the benchmark database is emptied; the
    alternation calendar keeps its rows."""
    from src.alternation import calendar as cal

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    conn = cal.connect_benchmark()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS reset_test_samples (x int)")
            cur.execute("INSERT INTO reset_test_samples VALUES (1), (2)")
            cur.execute("SELECT to_regclass('branch_calendar')")
            has_calendar = cur.fetchone()[0] is not None
            calendar_rows = None
            if has_calendar:
                cur.execute("SELECT count(*) FROM branch_calendar")
                calendar_rows = cur.fetchone()[0]
        assert reset.measures_plan(("reset_test_samples",))["reset_test_samples"] >= 2
        assert reset.reset_measures(("reset_test_samples", "absent_table")) == [
            "reset_test_samples"
        ]
        assert reset.measures_plan(("reset_test_samples",)) == {"reset_test_samples": 0}
        with conn.cursor() as cur:
            if has_calendar:
                cur.execute("SELECT count(*) FROM branch_calendar")
                assert cur.fetchone()[0] == calendar_rows
            cur.execute("DROP TABLE reset_test_samples")
    finally:
        conn.close()
