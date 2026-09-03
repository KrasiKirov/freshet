"""db/init.sql is mounted at docker-entrypoint-initdb.d and runs on a FRESH volume.

A statement that only works against an already-evolved database makes the
Postgres container unhealthy on a clean machine — `make up` then fails before a
single test runs, which is exactly how CI broke. `ALTER TABLE ... ADD COLUMN IF
NOT EXISTS` guards the COLUMN, not the table, so ordering is load-bearing.
"""
import pathlib
import subprocess
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


def test_vector_records_is_indexed_by_incident():
    """`WHERE incident_id = %s` is the sole filter of the query behind every
    brief, postmortem and thread reply. Nothing indexed it, so each one scanned
    the whole table."""
    name = f"bootstrap_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(ADMIN.replace("/postgres", f"/{name}"), autocommit=True) as c:
            c.execute(SCHEMA)
            names = {r[0] for r in c.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'vector_records'")}
            assert "vector_records_incident_idx" in names, names
    finally:
        with psycopg.connect(ADMIN, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}"')


def test_the_schema_records_its_version():
    """The file was a stack of unversioned ALTERs and permanent one-time
    backfills. Recording an applied version makes drift observable instead of
    archaeological."""
    name = f"bootstrap_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(ADMIN.replace("/postgres", f"/{name}"), autocommit=True) as c:
            c.execute(SCHEMA)
            [(version,)] = c.execute("SELECT max(version) FROM schema_version").fetchall()
            assert version >= 1
    finally:
        with psycopg.connect(ADMIN, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}"')


def test_the_additive_alters_are_folded_into_their_create():
    """A CREATE TABLE followed by a stack of ADD COLUMN IF NOT EXISTS encodes
    history, not a schema. Only the LEGACY section may still carry them, for
    volumes created before this consolidation."""
    body, marker, legacy = SCHEMA.partition("-- Legacy migrations")
    assert marker, "the legacy section must exist and carry that exact header"
    assert legacy.strip(), "the legacy section must not be empty"
    # Statements only: the header comment discusses ADD COLUMN by name, and a
    # test that cannot tell prose from DDL would fail on its own documentation.
    statements = "\n".join(line for line in body.splitlines()
                           if not line.lstrip().startswith("--"))
    assert "ADD COLUMN" not in statements.upper(), \
        "additive ALTERs belong in their CREATE TABLE, not above the legacy section"


def test_a_fresh_database_and_an_evolved_one_reach_the_same_schema():
    """The whole point of the legacy tail. A column folded into a CREATE TABLE
    but dropped from the tail would leave already-evolved volumes missing it,
    and nothing else in the suite would notice."""
    fresh, evolved = (f"cmp_{uuid.uuid4().hex[:8]}" for _ in range(2))
    old_schema = subprocess.run(
        ["git", "show", "master:db/init.sql"], capture_output=True, text=True,
        cwd=pathlib.Path(__file__).resolve().parents[2], check=True).stdout
    with psycopg.connect(ADMIN, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{fresh}"')
        admin.execute(f'CREATE DATABASE "{evolved}"')
    try:
        with psycopg.connect(ADMIN.replace("/postgres", f"/{fresh}"), autocommit=True) as c:
            c.execute(SCHEMA)                    # virgin volume, new file only
        with psycopg.connect(ADMIN.replace("/postgres", f"/{evolved}"), autocommit=True) as c:
            c.execute(old_schema)                # volume built by the OLD file...
            c.execute(SCHEMA)                    # ...then upgraded by the new one
        assert _columns(fresh) == _columns(evolved)
    finally:
        with psycopg.connect(ADMIN, autocommit=True) as admin:
            for db in (fresh, evolved):
                admin.execute(f'DROP DATABASE IF EXISTS "{db}"')


def _columns(db: str) -> set:
    with psycopg.connect(ADMIN.replace("/postgres", f"/{db}"), autocommit=True) as c:
        return set(c.execute(
            "SELECT table_name, column_name, data_type, is_nullable"
            " FROM information_schema.columns WHERE table_schema = 'public'").fetchall())
