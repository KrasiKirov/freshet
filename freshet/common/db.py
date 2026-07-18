"""Postgres connection helper. The compose stack publishes Postgres on host
port 5433 (5432 is left free for a local Postgres).

connect() returns a ResilientConnection: a thin wrapper that transparently
reconnects and retries execute() on connection-level failures (bounded, with
backoff), so a dropped connection degrades to a brief stall instead of
bricking the API process or crashing a worker on a transient blip. Query-level
errors (bad SQL, constraint violations) are NOT retried — they raise
immediately, unchanged."""

from __future__ import annotations

import contextlib
import os
import time

import psycopg

DEFAULT_DSN = "postgresql://freshet:freshet@localhost:5433/freshet"

_RETRY_ATTEMPTS = 3
_RETRY_BASE_DELAY_S = 0.5


class ResilientConnection:
    """Wraps a psycopg connection; execute() retries connection-level errors
    (psycopg.OperationalError / InterfaceError) by reconnecting, up to
    _RETRY_ATTEMPTS with exponential backoff. Everything else passes through
    untouched (attribute access proxies to the underlying connection)."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = psycopg.connect(dsn, autocommit=True)

    def _reconnect(self) -> None:
        with contextlib.suppress(Exception):
            self._conn.close()
        self._conn = psycopg.connect(self._dsn, autocommit=True)

    def execute(self, *args, **kwargs):
        delay = _RETRY_BASE_DELAY_S
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                return self._conn.execute(*args, **kwargs)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                if attempt == _RETRY_ATTEMPTS - 1:
                    raise
                time.sleep(delay)
                delay *= 2
                with contextlib.suppress(Exception):
                    self._reconnect()  # next execute attempt raises if still down

    def close(self) -> None:
        self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def connect(dsn: str | None = None) -> ResilientConnection:
    return ResilientConnection(dsn or os.environ.get("FRESHET_DSN", DEFAULT_DSN))


def make_pool(dsn: str | None = None, max_size: int = 8):
    """A psycopg connection pool for the API, so concurrent requests (FastAPI
    serves sync endpoints from a threadpool) each get their own connection
    instead of serializing on one shared connection. The pool health-checks
    connections on checkout and transparently replaces broken ones, so it also
    subsumes ResilientConnection's reconnect job for the API process. Workers
    stay single-connection (connect()); they are sequential by design."""
    from psycopg_pool import ConnectionPool

    return ConnectionPool(
        dsn or os.environ.get("FRESHET_DSN", DEFAULT_DSN),
        max_size=max_size,
        kwargs={"autocommit": True},
        check=ConnectionPool.check_connection,
        open=True,
    )
