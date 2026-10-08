"""Tests for src.benchmark.parity on small Parquet dumps."""

from pathlib import Path

import duckdb

from src.benchmark.parity import column_differences, compare


def _dump(root: Path, table: str, sql: str) -> None:
    (root / table).mkdir(parents=True)
    duckdb.execute(f"COPY ({sql}) TO '{root / table / 'part.parquet'}' (FORMAT parquet)")


def test_equal_rows_ignore_lineage_and_list_order(tmp_path) -> None:
    """Snapshot ids and processed_at differ by engine; hashtag order is not contract."""
    a, b = tmp_path / "a", tmp_path / "b"
    _dump(a, "silver_posts", "SELECT 1 AS seq, ['x', 'y'] AS hashtags, 111 AS bronze_snapshot_id")
    _dump(b, "silver_posts", "SELECT 1 AS seq, ['y', 'x'] AS hashtags, 7 AS bronze_snapshot_id")
    (result,) = compare(a, b, ("silver_posts",))
    assert result.equal


def test_a_missing_or_changed_row_is_reported_both_ways(tmp_path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    _dump(a, "gold_langs_hour", "SELECT * FROM (VALUES ('fr', 3), ('en', 5)) AS t(lang, posts)")
    _dump(
        b,
        "gold_langs_hour",
        "SELECT * FROM (VALUES ('fr', 3), ('en', 6), ('de', 1)) AS t(lang, posts)",
    )
    (result,) = compare(a, b, ("gold_langs_hour",))
    assert (result.rows_a, result.rows_b, result.only_in_a, result.only_in_b) == (2, 3, 1, 2)
    assert not result.equal


def test_duplicates_count(tmp_path) -> None:
    """Compared as multisets: a duplicated row is a difference."""
    a, b = tmp_path / "a", tmp_path / "b"
    _dump(a, "silver_likes", "SELECT * FROM (VALUES (1), (1)) AS t(seq)")
    _dump(b, "silver_likes", "SELECT 1 AS seq")
    (result,) = compare(a, b, ("silver_likes",))
    assert (result.only_in_a, result.only_in_b) == (1, 0)


def test_column_differences(tmp_path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    _dump(a, "silver_follows", "SELECT 1 AS seq, 'd' AS subject_did")
    _dump(b, "silver_follows", "SELECT 1 AS seq")
    assert column_differences(a, b, ("silver_follows",)) == {
        "silver_follows": {"only_in_a": ["subject_did"], "only_in_b": []}
    }
