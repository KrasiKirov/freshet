"""Postgres connection helper. The compose stack publishes Postgres on host
port 5433 (5432 is left free for a local Postgres)."""

from __future__ import annotations

import os

import psycopg

DEFAULT_DSN = "postgresql://freshet:freshet@localhost:5433/freshet"


def connect(dsn: str | None = None) -> psycopg.Connection:
    return psycopg.connect(dsn or os.environ.get("FRESHET_DSN", DEFAULT_DSN), autocommit=True)
