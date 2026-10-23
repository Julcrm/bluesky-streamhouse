"""Tests for src.benchmark.tipping: the fixed + marginal cost model (H7)."""

import pytest

from src.benchmark import tipping

RATES = (464.0, 1850.0, 30_000.0)  # 1×, 4× and max, as the controlled test


def _points(fixed: float, marginal: float) -> list[tuple[float, float]]:
    return [(r, fixed / r + marginal) for r in RATES]


def test_fit_recovers_the_fixed_and_marginal_costs() -> None:
    f = tipping.fit(_points(800, 0.25))
    assert f.fixed_ms_per_s == pytest.approx(800)
    assert f.marginal_ms_per_message == pytest.approx(0.25)
    assert f.r2 == pytest.approx(1)
    assert (f.min_rate, f.max_rate, f.runs) == (464, 30_000, 3)


def test_fit_needs_two_throughputs() -> None:
    assert tipping.fit([(464, 2.0), (464, 2.1)]) is None
    assert tipping.fit([]) is None


def test_crossing_within_the_measured_throughputs() -> None:
    """DuckDB: little fixed cost, dearer per message; Spark the reverse: Spark cheaper
    above (800 − 50) / (0.30 − 0.25) = 15 000 msg/s."""
    cross = tipping.crossing(tipping.fit(_points(50, 0.30)), tipping.fit(_points(800, 0.25)))
    assert cross.rate == pytest.approx(15_000)
    assert (cross.kind, cross.cheaper_below, cross.cheaper_above) == (
        "measured",
        "duckdb",
        "spark",
    )


def test_crossing_beyond_the_fastest_run_is_an_extrapolation() -> None:
    cross = tipping.crossing(tipping.fit(_points(50, 0.30)), tipping.fit(_points(800, 0.29)))
    assert cross.rate == pytest.approx(75_000)
    assert cross.kind == "extrapolated"


def test_no_crossing_when_one_branch_is_cheaper_everywhere() -> None:
    """H7's legitimate outcome on one node: DuckDB cheaper at every throughput."""
    cross = tipping.crossing(tipping.fit(_points(50, 0.20)), tipping.fit(_points(800, 0.25)))
    assert (cross.rate, cross.kind, cross.cheaper_below) == (None, "none", "duckdb")


def test_tipping_point_from_bench_rows() -> None:
    rows = [
        {
            "branch": branch,
            "throughput_msg_s": r,
            "ingest_cpu_ms_per_message": (fixed / r + marginal) / 2,
            "transform_cpu_ms_per_message": (fixed / r + marginal) / 2,
        }
        for branch, fixed, marginal in (("duckdb", 50, 0.30), ("spark", 800, 0.25))
        for r in RATES
    ]
    result = tipping.tipping_point(rows)
    assert result["crossing"]["rate"] == pytest.approx(15_000)
    assert result["fits"]["spark"]["fixed_ms_per_s"] == pytest.approx(800)


def test_unknown_without_runs_of_a_branch() -> None:
    result = tipping.tipping_point([])
    assert result["crossing"]["kind"] == "unknown"
    assert result["fits"] == {"duckdb": None, "spark": None}
