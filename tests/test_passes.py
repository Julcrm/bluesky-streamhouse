"""Tests for src.benchmark.passes: one row per finished Silver/Gold pass (H8a)."""

from datetime import UTC, datetime, timedelta

from src.benchmark import passes

T0 = datetime(2026, 10, 22, 8, 30, tzinfo=UTC)


def test_pass_row_with_its_duration() -> None:
    row = passes.pass_row(
        "r1", "spark", "bluesky_spark_silver_gold", "SUCCESS", False, T0, T0 + timedelta(minutes=7)
    )
    assert row == (
        "r1",
        "spark",
        "bluesky_spark_silver_gold",
        "SUCCESS",
        False,
        T0,
        T0 + timedelta(minutes=7),
        420.0,
    )


def test_a_run_that_never_started_is_no_pass() -> None:
    assert (
        passes.pass_row("r2", "duckdb", "bluesky_silver_gold", "FAILURE", False, None, T0) is None
    )


class _Conn:
    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        if params is not None:
            self.rows.append(params)


def test_record_writes_the_row() -> None:
    conn = _Conn()
    row = passes.pass_row("r1", "duckdb", "bluesky_silver_gold", "SUCCESS", True, T0, T0)
    passes.record(conn, row)
    assert conn.rows == [row]
