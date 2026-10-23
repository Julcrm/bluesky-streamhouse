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


def test_iceberg_usage_from_the_current_snapshot_summary() -> None:
    metadata = {
        "current-snapshot-id": 2,
        "snapshots": [
            {"snapshot-id": 1, "summary": {"total-records": "10"}},
            {
                "snapshot-id": 2,
                "summary": {
                    "total-records": "1000",
                    "total-position-deletes": "40",
                    "total-files-size": "126000",
                    "total-data-files": "3",
                    "total-delete-files": "1",
                },
            },
        ],
    }
    usage = storage.iceberg_usage("bronze", "bronze_events", metadata)
    assert (usage.branch, usage.rows, usage.bytes, usage.files) == ("spark", 960, 126000, 4)


def test_iceberg_table_without_snapshot_is_empty() -> None:
    usage = storage.iceberg_usage("gold", "gold_langs_hour", {"current-snapshot-id": None})
    assert (usage.rows, usage.bytes, usage.files) == (0, 0, 0)


def test_ducklake_tables_of_the_local_catalogs() -> None:
    """Rows of the local catalogs: files and inlined rows, live tables only."""
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        import pytest

        pytest.skip("local stack not running (make up)")
    import psycopg2

    from src import config

    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.DUCKLAKE_CATALOG_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            bronze = storage.ducklake_tables(cur, config.DUCKLAKE_BRONZE_METADATA_SCHEMA)
            transform = storage.ducklake_tables(cur, config.DUCKLAKE_TRANSFORM_METADATA_SCHEMA)
    finally:
        conn.close()
    assert [t.table_name for t in bronze] == ["bronze_events"]
    assert bronze[0].rows > 0 and bronze[0].rows >= bronze[0].inlined_rows
    assert {"silver_posts", "gold_langs_hour"} <= {t.table_name for t in transform}
    assert all(t.branch == "duckdb" for t in bronze + transform)
