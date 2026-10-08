"""Tests for src.benchmark.collector: cgroup and /proc readers on fake files, container
mapping, and one collection into a throwaway database on the local Postgres (make up)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from src import config
from src.alternation import calendar as cal
from src.benchmark import collector

CGROUP_FILES = {
    "cpu.stat": "usage_usec 123456789\nuser_usec 100000000\nsystem_usec 23456789\n",
    "memory.stat": "anon 838860800\nfile 104857600\npgmajfault 42\nshmem 0\n",
    "memory.swap.current": "0\n",
    "memory.pressure": "some avg10=0.00 avg60=0.00 avg300=0.00 total=2000\n"
    "full avg10=0.00 avg60=0.00 avg300=0.00 total=1500\n",
    "cpu.pressure": "some avg10=0.10 avg60=0.00 avg300=0.00 total=777\n"
    "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
    "io.pressure": "some avg10=0.00 avg60=0.00 avg300=0.00 total=10\n"
    "full avg10=0.00 avg60=0.00 avg300=0.00 total=5\n",
}
PROC_STAT = "cpu  800 1 400 6000 20 0 60 37 0 0\ncpu0 1 1 1 1 1 1 1 1 0 0\n"
MEMINFO = "MemTotal: 8000000 kB\nMemAvailable: 2400000 kB\nSwapTotal: 8000 kB\nSwapFree: 7000 kB\n"


def _cgroup(root: Path, container_id: str) -> Path:
    path = collector.cgroup_dir(container_id, root)
    path.mkdir(parents=True)
    for name, content in CGROUP_FILES.items():
        (path / name).write_text(content)
    return path


def _proc(root: Path) -> Path:
    (root / "pressure").mkdir(parents=True)
    (root / "stat").write_text(PROC_STAT)
    (root / "meminfo").write_text(MEMINFO)
    for name in ("memory", "cpu", "io"):
        (root / "pressure" / name).write_text(CGROUP_FILES[f"{name}.pressure"])
    return root


def test_container_reader_takes_anon_not_the_page_cache(tmp_path) -> None:
    sample = collector.read_container(_cgroup(tmp_path, "abc"))
    assert sample.cpu_usec == 123456789
    assert sample.anon_bytes == 800 * 2**20 and sample.file_bytes == 100 * 2**20
    assert (sample.swap_bytes, sample.pgmajfault) == (0, 42)
    assert (sample.mem_psi_full_us, sample.cpu_psi_some_us, sample.io_psi_full_us) == (
        1500,
        777,
        5,
    )


def test_missing_cgroup_files_give_gaps_not_errors(tmp_path) -> None:
    sample = collector.read_container(tmp_path / "gone")
    assert sample.cpu_usec is None and sample.anon_bytes is None


def test_host_reader_counts_steal_and_memory(tmp_path) -> None:
    host = collector.read_host(_proc(tmp_path))
    assert host.cpu_steal_ticks == 37
    assert host.cpu_total_ticks == 800 + 1 + 400 + 6000 + 20 + 0 + 60 + 37
    assert host.mem_available_bytes == 2400000 * 1024
    assert host.swap_used_bytes == 1000 * 1024
    assert host.mem_psi_full_us == 1500


def test_measured_containers_by_compose_service() -> None:
    listing = [
        {"Id": "1" * 64, "Labels": {"com.docker.compose.service": "spark"}},
        {"Id": "2" * 64, "Labels": {"com.docker.compose.service": "velib-api"}},
        {"Id": "3" * 64, "Labels": {}},
    ]
    assert collector.measured_containers(listing) == {"spark": "1" * 64}


def test_branches_are_the_calendars() -> None:
    branches = {branch for branch, _ in config.BENCHMARK_SERVICES.values()} - {None}
    assert branches == set(cal.BRANCHES)


def test_collect_once_into_the_benchmark_tables(tmp_path, monkeypatch) -> None:
    from tests.test_resources_ducklake import _local_stack_up

    if not _local_stack_up():
        pytest.skip("local stack not running (make up)")
    test_db = "bluesky_benchmark_test"
    admin = cal.connect_benchmark("postgres")
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,))
        if cur.fetchone() is None:
            cur.execute(f"CREATE DATABASE {test_db}")
    admin.close()
    monkeypatch.setattr(config, "BENCHMARK_DB", test_db)
    monkeypatch.setattr(config, "COLLECTOR_CGROUP_ROOT", str(tmp_path / "cgroup"))
    monkeypatch.setattr(config, "COLLECTOR_PROC_ROOT", str(_proc(tmp_path / "proc")))
    monkeypatch.setattr(config, "CATALOG_DATABASES", ("postgres",))
    _cgroup(tmp_path / "cgroup", "f" * 64)
    conn = collector.connect()
    try:
        with conn, conn.cursor() as cur:
            for table in ("container_samples", "host_samples", "catalog_samples"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            for statement in collector.DDL:
                cur.execute(statement)
        now = datetime(2026, 10, 8, 14, 0, tzinfo=UTC)
        assert collector.collect_once(conn, now, {"spark": "f" * 64}) == 3
        # The same tick twice is ignored (a restart within the interval)
        collector.collect_once(conn, now, {"spark": "f" * 64})
        with conn.cursor() as cur:
            cur.execute(
                "SELECT service, branch, layer, container_id, anon_bytes FROM container_samples"
            )
            assert cur.fetchall() == [("spark", "spark", "streaming", "f" * 12, 800 * 2**20)]
            cur.execute("SELECT cpu_steal_ticks FROM host_samples")
            assert cur.fetchall() == [(37,)]
            cur.execute("SELECT db FROM catalog_samples")
            assert cur.fetchall() == [("postgres",)]
        collector.purge(conn, datetime(2026, 12, 1, tzinfo=UTC))
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM container_samples")
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()
