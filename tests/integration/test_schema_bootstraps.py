"""db/init.sql is mounted at docker-entrypoint-initdb.d and runs on a FRESH volume.

A statement that only works against an already-evolved database makes the
Postgres container unhealthy on a clean machine — `make up` then fails before a
single test runs, which is exactly how CI broke. `ALTER TABLE ... ADD COLUMN IF
NOT EXISTS` guards the COLUMN, not the table, so ordering is load-bearing.
"""
import pathlib
import uuid

import psycopg
import pytest

pytestmark = pytest.mark.integration

ADMIN = "postgresql://freshet:freshet@localhost:5433/postgres"
SCHEMA = (pathlib.Path(__file__).resolve().parents[2] / "db/init.sql").read_text()


def test_the_schema_applies_to_an_empty_database():
    name = f"bootstrap_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(ADMIN.replace("/postgres", f"/{name}"), autocommit=True) as c:
            c.execute(SCHEMA)          # raises if any statement is out of order
            cols = {r[0] for r in c.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'incidents'")}
            assert {"brief_due_at", "brief_delivered_at", "primary_service"} <= cols
            vcols = {r[0] for r in c.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'vector_records'")}
            assert {"title", "model"} <= vcols
    finally:
        with psycopg.connect(ADMIN, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}"')


def test_the_schema_is_idempotent():
    """`make db-init` is run repeatedly against an existing database."""
    name = f"bootstrap_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(ADMIN.replace("/postgres", f"/{name}"), autocommit=True) as c:
            c.execute(SCHEMA)
            c.execute(SCHEMA)          # second application must be a no-op
    finally:
        with psycopg.connect(ADMIN, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
