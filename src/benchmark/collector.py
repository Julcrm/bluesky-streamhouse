"""
Metrics collector of the benchmark (phase 7, decision D34), run by the neutral
`bench-collector` container.

Every COLLECTOR_INTERVAL_SECONDS it stores raw samples in the benchmark database:
- container_samples: for each measured container (config.BENCHMARK_SERVICES), its cgroup
  counters: cumulative CPU time, real memory (`anon`), page cache, swap, major page
  faults and pressure stall time (PSI). Real memory is `anon`, not `memory.current`,
  which the reclaimable page cache fills up to the limit at no cost (D31);
- host_samples: CPU steal (time the hypervisor gave to other tenants), host PSI, available
  memory and swap: they only flag contaminated windows, they are no result (D32);
- catalog_samples: pg_stat_database counters of the catalog databases in the shared
  Postgres, an estimate of each catalog's cost (D12).

Counters are stored raw and cumulative: the 5-minute windows (deltas, Mo·s, ratios per
message) are computed afterwards by a Dagster asset, so an analysis mistake is fixed by
recomputing, without losing a measured day. A container restart resets its counters:
the container id is stored with each sample to tell runs apart.

Container ids come from a Docker socket proxy that only allows listing containers; the
cgroup tree and /proc of the host are mounted read-only. Standard library and psycopg2
only: the collector's own cost stays small, and it is measured too (`bench-collector`).
"""

import json
import signal
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg2
from loguru import logger

from src import config
from src.healthcheck import touch

DDL = [
    """CREATE TABLE IF NOT EXISTS container_samples (
        ts              TIMESTAMPTZ NOT NULL,
        service         TEXT NOT NULL,
        branch          TEXT,
        layer           TEXT NOT NULL,
        container_id    TEXT NOT NULL,
        cpu_usec        BIGINT,
        anon_bytes      BIGINT,
        file_bytes      BIGINT,
        swap_bytes      BIGINT,
        pgmajfault      BIGINT,
        mem_psi_full_us BIGINT,
        cpu_psi_some_us BIGINT,
        io_psi_full_us  BIGINT,
        PRIMARY KEY (ts, service)
    )""",
    """CREATE TABLE IF NOT EXISTS host_samples (
        ts              TIMESTAMPTZ PRIMARY KEY,
        cpu_total_ticks BIGINT,
        cpu_steal_ticks BIGINT,
        mem_available_bytes BIGINT,
        swap_used_bytes BIGINT,
        mem_psi_full_us BIGINT,
        cpu_psi_some_us BIGINT,
        io_psi_full_us  BIGINT
    )""",
    """CREATE TABLE IF NOT EXISTS catalog_samples (
        ts              TIMESTAMPTZ NOT NULL,
        db              TEXT NOT NULL,
        xact_commit     BIGINT,
        xact_rollback   BIGINT,
        blks_read       BIGINT,
        blks_hit        BIGINT,
        active_time_ms  DOUBLE PRECISION,
        PRIMARY KEY (ts, db)
    )""",
]


@dataclass(frozen=True)
class ContainerSample:
    cpu_usec: int | None
    anon_bytes: int | None
    file_bytes: int | None
    swap_bytes: int | None
    pgmajfault: int | None
    mem_psi_full_us: int | None
    cpu_psi_some_us: int | None
    io_psi_full_us: int | None


@dataclass(frozen=True)
class HostSample:
    cpu_total_ticks: int
    cpu_steal_ticks: int
    mem_available_bytes: int
    swap_used_bytes: int
    mem_psi_full_us: int | None
    cpu_psi_some_us: int | None
    io_psi_full_us: int | None


# --- Readers (pure, tested on fake files) ---


def _keyed(path: Path) -> dict[str, int]:
    """`key value` lines (cpu.stat, memory.stat)."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return {}
    return {k: int(v) for k, v in (line.split()[:2] for line in lines if line.strip())}


def _single(path: Path) -> int | None:
    try:
        return int(path.read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return None


def psi_total(path: Path, line: str) -> int | None:
    """Cumulative stall time in µs of a PSI file's `some` or `full` line."""
    try:
        for row in path.read_text().splitlines():
            if row.startswith(line + " "):
                return int(row.rsplit("total=", 1)[1])
    except (OSError, ValueError, IndexError):
        pass
    return None


def read_container(cgroup: Path) -> ContainerSample:
    """Counters of one container's cgroup (v2, systemd driver)."""
    cpu = _keyed(cgroup / "cpu.stat")
    memory = _keyed(cgroup / "memory.stat")
    return ContainerSample(
        cpu_usec=cpu.get("usage_usec"),
        anon_bytes=memory.get("anon"),
        file_bytes=memory.get("file"),
        swap_bytes=_single(cgroup / "memory.swap.current"),
        pgmajfault=memory.get("pgmajfault"),
        mem_psi_full_us=psi_total(cgroup / "memory.pressure", "full"),
        cpu_psi_some_us=psi_total(cgroup / "cpu.pressure", "some"),
        io_psi_full_us=psi_total(cgroup / "io.pressure", "full"),
    )


def read_host(proc: Path) -> HostSample:
    """CPU ticks (steal included), available memory, swap and PSI of the host."""
    cpu = next(line for line in (proc / "stat").read_text().splitlines() if line.startswith("cpu "))
    ticks = [int(v) for v in cpu.split()[1:]]
    meminfo = {
        k.rstrip(":"): int(v) * 1024
        for k, v, *_ in (line.split() for line in (proc / "meminfo").read_text().splitlines())
    }
    return HostSample(
        # user nice system idle iowait irq softirq steal (guest time is already in user)
        cpu_total_ticks=sum(ticks[:8]),
        cpu_steal_ticks=ticks[7],
        mem_available_bytes=meminfo["MemAvailable"],
        swap_used_bytes=meminfo["SwapTotal"] - meminfo["SwapFree"],
        mem_psi_full_us=psi_total(proc / "pressure" / "memory", "full"),
        cpu_psi_some_us=psi_total(proc / "pressure" / "cpu", "some"),
        io_psi_full_us=psi_total(proc / "pressure" / "io", "full"),
    )


def measured_containers(containers: list[dict]) -> dict[str, str]:
    """{compose service: container id} of the measured containers, from the Docker API
    listing (Coolify suffixes container names, the compose service label is stable)."""
    found = {}
    for container in containers:
        service = (container.get("Labels") or {}).get("com.docker.compose.service")
        if service in config.BENCHMARK_SERVICES:
            found[service] = container["Id"]
    return found


def cgroup_dir(container_id: str, root: Path) -> Path:
    """The container's cgroup: systemd driver on the VPS (`system.slice/docker-<id>.scope`),
    cgroupfs driver on Docker Desktop (`docker/<id>`)."""
    systemd = root / "system.slice" / f"docker-{container_id}.scope"
    cgroupfs = root / "docker" / container_id
    return cgroupfs if not systemd.exists() and cgroupfs.exists() else systemd


# --- Collection ---


def list_containers(url: str = config.COLLECTOR_DOCKER_URL) -> list[dict]:
    with urllib.request.urlopen(f"{url}/containers/json", timeout=5) as response:
        return json.load(response)


def catalog_rows(cur) -> list[tuple]:
    cur.execute(
        "SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit, active_time "
        "FROM pg_stat_database WHERE datname = ANY(%s)",
        (list(config.CATALOG_DATABASES),),
    )
    return cur.fetchall()


def collect_once(conn, now: datetime, containers: dict[str, str]) -> int:
    """One sample of every measured container, the host and the catalogs; rows written."""
    cgroups = Path(config.COLLECTOR_CGROUP_ROOT)
    rows = 0
    with conn, conn.cursor() as cur:
        for service, container_id in sorted(containers.items()):
            sample = read_container(cgroup_dir(container_id, cgroups))
            branch, layer = config.BENCHMARK_SERVICES[service]
            cur.execute(
                "INSERT INTO container_samples VALUES (%(ts)s, %(service)s, %(branch)s, "
                "%(layer)s, %(container_id)s, %(cpu_usec)s, %(anon_bytes)s, %(file_bytes)s, "
                "%(swap_bytes)s, %(pgmajfault)s, %(mem_psi_full_us)s, %(cpu_psi_some_us)s, "
                "%(io_psi_full_us)s) ON CONFLICT DO NOTHING",
                {
                    "ts": now,
                    "service": service,
                    "branch": branch,
                    "layer": layer,
                    "container_id": container_id[:12],
                    **asdict(sample),
                },
            )
            rows += 1
        host = read_host(Path(config.COLLECTOR_PROC_ROOT))
        cur.execute(
            "INSERT INTO host_samples VALUES (%(ts)s, %(cpu_total_ticks)s, %(cpu_steal_ticks)s, "
            "%(mem_available_bytes)s, %(swap_used_bytes)s, %(mem_psi_full_us)s, "
            "%(cpu_psi_some_us)s, %(io_psi_full_us)s) ON CONFLICT DO NOTHING",
            {"ts": now, **asdict(host)},
        )
        rows += 1
        for db, *counters in catalog_rows(cur):
            cur.execute(
                "INSERT INTO catalog_samples VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                (now, db, *counters),
            )
            rows += 1
    return rows


def purge(conn, now: datetime) -> None:
    """Samples older than COLLECTOR_RETENTION_DAYS go (the windows are kept)."""
    with conn, conn.cursor() as cur:
        for table in ("container_samples", "host_samples", "catalog_samples"):
            cur.execute(
                f"DELETE FROM {table} WHERE ts < %s - make_interval(days => %s)",
                (now, config.COLLECTOR_RETENTION_DAYS),
            )


def connect():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.BENCHMARK_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        connect_timeout=10,
        application_name="bench-collector",
    )


def run() -> None:
    """Sample every COLLECTOR_INTERVAL_SECONDS, aligned on the interval; one bad tick (Docker
    or Postgres unreachable) is logged and skipped, never fatal."""
    stopping = False

    def on_signal(signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    conn = None
    last_purge = 0.0
    interval = config.COLLECTOR_INTERVAL_SECONDS
    logger.info(f"Collector: every {interval} s, services {sorted(config.BENCHMARK_SERVICES)}")
    while not stopping:
        now = datetime.now(UTC).replace(microsecond=0)
        try:
            if conn is None or conn.closed:
                conn = connect()
                with conn, conn.cursor() as cur:
                    for statement in DDL:
                        cur.execute(statement)
            containers = measured_containers(list_containers())
            collect_once(conn, now, containers)
            if time.monotonic() - last_purge > 3600:
                purge(conn, now)
                last_purge = time.monotonic()
            touch(config.HEARTBEAT_FILE)
        except Exception as e:  # noqa: BLE001 - a missed sample is a gap, not a crash
            logger.warning(f"Sample at {now:%H:%M:%S} skipped: {e}")
            if conn is not None and not conn.closed:
                conn.close()
            conn = None
        time.sleep(max(0.0, interval - time.time() % interval))
    if conn is not None:
        conn.close()
    logger.info("Collector stopped")


if __name__ == "__main__":
    run()
