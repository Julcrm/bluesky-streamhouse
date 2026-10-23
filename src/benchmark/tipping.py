"""
Tipping point of the two branches (H7, added on 2026-10-22): the throughput above which
the Spark branch would cost less CPU per message than the DuckDB branch, if any.

Model (fixed cost + marginal cost, protocol point 3): over a controlled-test run of
throughput r (messages/s), the CPU per message is F / r + m, with F the fixed cost (CPU ms
per second, paid whatever the load: JVMs, catalogs, polling) and m the marginal cost (CPU
ms per message). A least-squares line of the CPU per message against 1 / r over a
branch's runs (1×, 4×, max) gives F (slope) and m (intercept). The two lines cross at
r* = (F_spark − F_duckdb) / (m_duckdb − m_spark) when that is positive; otherwise one
branch costs less at every throughput. A crossing beyond the fastest run measured is an
extrapolation, reported as such.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Fit:
    fixed_ms_per_s: float
    marginal_ms_per_message: float
    r2: float | None
    runs: int
    min_rate: float
    max_rate: float


def fit(points: list[tuple[float, float]]) -> Fit | None:
    """Least squares of the CPU per message on 1 / throughput, from (throughput msg/s,
    CPU ms per message) points; None with fewer than two distinct throughputs."""
    points = [(r, c) for r, c in points if r and r > 0 and c is not None]
    if len({round(r, 6) for r, _ in points}) < 2:
        return None
    xs = [1 / r for r, _ in points]
    ys = [c for _, c in points]
    n = len(points)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    slope = sxy / sxx
    intercept = mean_y - slope * mean_x
    syy = sum((y - mean_y) ** 2 for y in ys)
    residual = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys, strict=True))
    return Fit(
        fixed_ms_per_s=slope,
        marginal_ms_per_message=intercept,
        r2=1 - residual / syy if syy > 0 else None,
        runs=n,
        min_rate=min(r for r, _ in points),
        max_rate=max(r for r, _ in points),
    )


@dataclass(frozen=True)
class Crossing:
    # Throughput where Spark starts costing less per message; None without a crossing
    rate: float | None
    # "measured" (within both branches' measured throughputs), "extrapolated" (beyond
    # them), "none" (one branch cheaper at every throughput), "unknown" (a fit missing)
    kind: str
    cheaper_below: str | None
    cheaper_above: str | None


def crossing(duckdb: Fit | None, spark: Fit | None) -> Crossing:
    if duckdb is None or spark is None:
        return Crossing(None, "unknown", None, None)
    dm = duckdb.marginal_ms_per_message - spark.marginal_ms_per_message
    df = spark.fixed_ms_per_s - duckdb.fixed_ms_per_s
    if dm == 0 or df / dm <= 0:
        # Same order at every throughput: whichever costs less at the fastest run
        at = max(duckdb.max_rate, spark.max_rate)
        cheaper = (
            "duckdb"
            if duckdb.fixed_ms_per_s / at + duckdb.marginal_ms_per_message
            <= spark.fixed_ms_per_s / at + spark.marginal_ms_per_message
            else "spark"
        )
        return Crossing(None, "none", cheaper, cheaper)
    rate = df / dm
    measured = rate <= min(duckdb.max_rate, spark.max_rate)
    # Below the crossing, the branch with the smaller fixed cost is cheaper
    below = "duckdb" if duckdb.fixed_ms_per_s < spark.fixed_ms_per_s else "spark"
    above = "spark" if below == "duckdb" else "duckdb"
    return Crossing(rate, "measured" if measured else "extrapolated", below, above)


def tipping_point(runs: list[dict]) -> dict:
    """From bench_ratios rows: each branch's fit on its total CPU per message (ingestion +
    transformation) and the crossing."""
    fits: dict[str, Fit | None] = {}
    for branch in ("duckdb", "spark"):
        points = [
            (
                row["throughput_msg_s"],
                (row["ingest_cpu_ms_per_message"] or 0)
                + (row["transform_cpu_ms_per_message"] or 0),
            )
            for row in runs
            if row["branch"] == branch
        ]
        fits[branch] = fit(points)
    cross = crossing(fits["duckdb"], fits["spark"])
    return {
        "fits": {b: (f.__dict__ if f else None) for b, f in fits.items()},
        "crossing": cross.__dict__,
    }
