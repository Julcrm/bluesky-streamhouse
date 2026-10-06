"""Tests for src.resources.bronze_lock, on the local Postgres (make up)."""

import threading
import time

import pytest

from src.resources.bronze_lock import SharedBronzeLock, exclusive_bronze_lock
from tests.test_resources_ducklake import _local_stack_up

pytestmark = pytest.mark.skipif(not _local_stack_up(), reason="local stack not running")

# Not the production key: a running local Quix must not interfere
KEY = 4242


def test_sink_is_locked_out_while_the_maintenance_holds_bronze() -> None:
    """Shared holds work alone; none while the exclusive one is held, again after it."""
    sink = SharedBronzeLock(key=KEY)
    try:
        assert sink.try_acquire()
        sink.release()
        with exclusive_bronze_lock(key=KEY):
            assert not sink.try_acquire()
        assert sink.try_acquire()
        sink.release()
    finally:
        sink.close()


def test_maintenance_waits_for_the_commit_in_progress() -> None:
    """The exclusive lock is granted only once the sink releases its shared hold."""
    sink = SharedBronzeLock(key=KEY)
    acquired_at: list[float] = []

    def maintenance() -> None:
        with exclusive_bronze_lock(key=KEY):
            acquired_at.append(time.monotonic())

    try:
        assert sink.try_acquire()
        thread = threading.Thread(target=maintenance)
        thread.start()
        time.sleep(0.5)
        assert acquired_at == []
        # A waiting maintenance already keeps new commits out (never starved)
        other = SharedBronzeLock(key=KEY)
        assert not other.try_acquire()
        other.close()
        released_at = time.monotonic()
        sink.release()
        thread.join(timeout=5)
        assert acquired_at and acquired_at[0] >= released_at
    finally:
        sink.close()


def test_a_lost_session_releases_the_lock() -> None:
    """A crashed sink (closed session) never leaves Bronze locked."""
    sink = SharedBronzeLock(key=KEY)
    assert sink.try_acquire()
    sink.close()
    done = threading.Event()

    def maintenance() -> None:
        with exclusive_bronze_lock(key=KEY):
            done.set()

    thread = threading.Thread(target=maintenance)
    thread.start()
    assert done.wait(timeout=5)
    thread.join()
