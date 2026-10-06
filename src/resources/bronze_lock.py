"""
Write lock of the Bronze DuckLake catalog, shared by the Quix sink and the maintenance.

DuckLake refuses a retention DELETE on a table another transaction inserted into while
it ran, and the Quix sink commits every 5 s: the DELETE (~10 s on 7 days of Bronze)
failed two nights in prod (2026-10-05/06). A Postgres advisory lock on the catalog
database serializes them: every sink commit holds it shared, the Bronze maintenance
holds it exclusive. While the maintenance runs, the sink pauses (backpressure) and
catches up from Redpanda afterwards.

Until the alternation (phase 6, D28) stops the engines outside 07:00-19:00, this is the
only thing keeping Quix out of the nightly maintenance. The lock dies with its Postgres
session, so a crashed holder never leaves Bronze locked.

Kept free of Dagster imports: the Quix image does not install Dagster.
"""

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2

from src import config
from src.resources.ducklake import DuckLakeSettings


def _connect(settings: DuckLakeSettings):
    """Autocommit session on the catalog database (advisory locks are per database)."""
    conn = psycopg2.connect(
        host=settings.catalog_host,
        port=settings.catalog_port,
        dbname=settings.catalog_db,
        user=settings.catalog_user,
        password=settings.catalog_password,
        connect_timeout=10,
        application_name="bronze_write_lock",
    )
    conn.autocommit = True
    return conn


@contextmanager
def exclusive_bronze_lock(
    settings: DuckLakeSettings | None = None, key: int = config.BRONZE_WRITE_LOCK_KEY
) -> Iterator[None]:
    """Hold the Bronze write lock alone: waits for the sink commit in progress, then
    keeps every new one out until the block exits."""
    conn = _connect(settings or DuckLakeSettings())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (key,))
        try:
            yield
        finally:
            # Explicit: the backend of a closed session releases its locks only once it
            # has exited, a little after close() returns
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
    finally:
        # Releases the lock anyway if the unlock was never reached
        conn.close()


class SharedBronzeLock:
    """The sink's side: held shared around each commit, never waited for."""

    def __init__(
        self, settings: DuckLakeSettings | None = None, key: int = config.BRONZE_WRITE_LOCK_KEY
    ) -> None:
        self._settings = settings or DuckLakeSettings()
        self._key = key
        self._conn = None

    def try_acquire(self) -> bool:
        """True if the lock is now held shared; False while the maintenance holds or
        waits for it (Postgres queues new shared requests behind a waiting exclusive
        one, so the maintenance is never starved). Connection errors propagate."""
        try:
            if self._conn is None:
                self._conn = _connect(self._settings)
            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock_shared(%s)", (self._key,))
                return cur.fetchone()[0]
        except psycopg2.Error:
            self.close()
            raise

    def release(self) -> None:
        """Release the shared hold; a lost session has released it already."""
        if self._conn is None:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock_shared(%s)", (self._key,))
        except psycopg2.Error:
            self.close()

    def close(self) -> None:
        """Drop the session (and any hold); never raises."""
        if self._conn is not None:
            try:
                self._conn.close()
            except psycopg2.Error:
                pass
            finally:
                self._conn = None
