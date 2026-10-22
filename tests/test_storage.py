"""Tests for src.benchmark.storage: bucket usage per prefix (H5b)."""

from src.benchmark import storage


def test_prefixes_one_per_ducklake_catalog_and_one_for_iceberg() -> None:
    assert storage.prefix_of("ducklake/bronze/main/raw/x.parquet") == "ducklake/bronze"
    assert storage.prefix_of("ducklake/transform/main/silver/y.parquet") == "ducklake/transform"
    assert storage.prefix_of("iceberg/01a0f7df/data/z.parquet") == "iceberg"
    assert storage.prefix_of("bench/ducklake/bronze/b.parquet") == "bench"


def test_usage_sums_bytes_and_objects_per_prefix() -> None:
    figures = storage.usage(
        {
            "ducklake/bronze/a.parquet": 100,
            "ducklake/bronze/b.parquet": 50,
            "ducklake/transform/c.parquet": 7,
            "iceberg/t1/data/d.parquet": 1000,
            "iceberg/t1/metadata/e.json": 3,
        }
    )
    assert figures == {
        "ducklake/bronze": (150, 2),
        "ducklake/transform": (7, 1),
        "iceberg": (1003, 2),
    }


def test_branch_of_each_prefix() -> None:
    assert storage.branch_of("ducklake/bronze") == "duckdb"
    assert storage.branch_of("iceberg") == "spark"
    assert storage.branch_of("bench") is None


class _FakeFs:
    def find(self, path: str, detail: bool = False) -> dict:
        assert path == "bluesky-streamhouse" and detail
        return {
            "bluesky-streamhouse/iceberg/t1/data/d.parquet": {"type": "file", "size": 10},
            "bluesky-streamhouse/ducklake/bronze": {"type": "directory", "size": 0},
            "bluesky-streamhouse/ducklake/bronze/a.parquet": {"type": "file", "size": 4},
        }


def test_list_bucket_keys_relative_to_the_bucket_without_directories() -> None:
    assert storage.list_bucket(_FakeFs(), "bluesky-streamhouse") == {
        "iceberg/t1/data/d.parquet": 10,
        "ducklake/bronze/a.parquet": 4,
    }


class _Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        if params is not None:
            self.rows.append(params)


class _Conn:
    def __init__(self) -> None:
        self.cur = _Cursor()

    def cursor(self):
        return self.cur


def test_record_one_row_per_prefix_with_its_branch() -> None:
    conn = _Conn()
    assert storage.record(conn, "ts", {"iceberg": (1003, 2), "ducklake/bronze": (150, 2)}) == 2
    assert conn.cur.rows == [
        ("ts", "ducklake/bronze", "duckdb", 150, 2),
        ("ts", "iceberg", "spark", 1003, 2),
    ]
