"""Tests for src.healthcheck: heartbeat age and TCP checks, and the CLI exit codes."""

import os
import socket
import time

from src import healthcheck


def test_heartbeat_fresh_stale_or_missing(tmp_path) -> None:
    beat = tmp_path / "heartbeat"
    assert not healthcheck.heartbeat_ok(str(beat), 120)
    healthcheck.touch(str(beat))
    assert healthcheck.heartbeat_ok(str(beat), 120)
    old = time.time() - 300
    os.utime(beat, (old, old))
    assert not healthcheck.heartbeat_ok(str(beat), 120)


def test_touch_never_raises(tmp_path) -> None:
    healthcheck.touch(str(tmp_path / "missing-dir" / "heartbeat"))


def test_tcp_open_and_closed_port() -> None:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        port = server.getsockname()[1]
        assert healthcheck.tcp_ok(port)
    assert not healthcheck.tcp_ok(port, timeout=0.5)


def test_cli_exit_codes(tmp_path) -> None:
    beat = tmp_path / "heartbeat"
    assert healthcheck.main(["heartbeat", str(beat), "120"]) == 1
    healthcheck.touch(str(beat))
    assert healthcheck.main(["heartbeat", str(beat), "120"]) == 0
    assert healthcheck.main(["bogus"]) == 2


def test_healthcheck_loads_nothing_of_the_project() -> None:
    """It runs every 30 s in measured containers: standard library only."""
    import subprocess
    import sys

    loaded = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, src.healthcheck; "
            "print(sorted(m for m in sys.modules if m.startswith('src.') or m in "
            "('dagster', 'pyspark', 'duckdb', 'loguru')))",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert loaded == "['src.healthcheck']"
