"""
Container healthchecks, for Coolify's status (a container without one shows "no
healthcheck": alive, but nothing says it works).

They run inside the measured containers every 30 s, so they stay tiny and identical in
both branches: standard library only, no import of the project (no Dagster, no engine).

    python -m src.healthcheck heartbeat <file> <max_age_seconds>
        the process writes the file at each loop (supervisors, producer): healthy while
        it is younger than max_age_seconds
    python -m src.healthcheck tcp <port>
        something accepts connections on localhost:<port> (Dagster gRPC code servers)
"""

import socket
import sys
import time
from pathlib import Path


def touch(path: str) -> None:
    """Write the heartbeat file (called by the process being checked)."""
    try:
        Path(path).write_text(str(time.time()))
    except OSError:  # a full or read-only /tmp must never stop the process itself
        pass


def heartbeat_ok(path: str, max_age_seconds: float, now: float | None = None) -> bool:
    try:
        age = (now or time.time()) - Path(path).stat().st_mtime
    except OSError:
        return False
    return age <= max_age_seconds


def tcp_ok(port: int, host: str = "127.0.0.1", timeout: float = 3) -> bool:
    try:
        with socket.create_connection((host, port), timeout):
            return True
    except OSError:
        return False


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[0] == "heartbeat":
        return 0 if heartbeat_ok(argv[1], float(argv[2])) else 1
    if len(argv) == 2 and argv[0] == "tcp":
        return 0 if tcp_ok(int(argv[1])) else 1
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
