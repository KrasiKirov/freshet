"""Shared fixtures for integration tests.

`emb` is the embedder for flow-level tests (autopilot, commit signal): they
exercise pipeline logic, not embedding semantics, so any schema-compatible
embedder works. FRESHET_TEST_EMBEDDER selects it — default bge (the real
retriever); CI sets `stub` to skip the model download. Tests that DO depend on
real embedding semantics (abstention, retrieval quality) construct bge
explicitly and guard with importorskip("sentence_transformers") instead.
"""
import os
from pathlib import Path

import psycopg
import pytest

# Integration tests wipe and reseed the corpus, so they MUST NOT run against the
# working database — a test run would otherwise destroy whatever the live poller
# had indexed. Everything here targets a dedicated database instead, created on
# first use from the same db/init.sql the real one uses.
ADMIN_DSN = "postgresql://freshet:freshet@localhost:5433/postgres"
TEST_DB = os.environ.get("FRESHET_TEST_DB", "freshet_test")
TEST_DSN = os.environ.get(
    "FRESHET_TEST_DSN", f"postgresql://freshet:freshet@localhost:5433/{TEST_DB}")


def _ensure_test_db() -> str:
    """Create the test database and apply the schema if it does not exist."""
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,)).fetchone()
        if not exists:
            admin.execute(f'CREATE DATABASE "{TEST_DB}"')
    schema = Path("db/init.sql").read_text()
    with psycopg.connect(TEST_DSN, autocommit=True) as fresh:
        fresh.execute(schema)
    return TEST_DSN


@pytest.fixture
def conn():
    """A connection to the dedicated test database. Safe to wipe."""
    from freshet.common.db import connect

    c = connect(_ensure_test_db())
    yield c
    c.close()


@pytest.fixture
def emb():
    from freshet.pipeline.embedding import make_embedder
    return make_embedder(os.environ.get("FRESHET_TEST_EMBEDDER", "bge"))


@pytest.fixture
def llm():
    """A deterministic stand-in for the LLM composer.

    Generation is mandatory in production, so there is no keyless path to fall
    back on — tests inject this instead of requiring an API key in CI. It cites a
    real event id so `verify_citations` keeps the citation rather than stripping it.
    """
    class FakeComposer:
        def compose(self, question: str, hits) -> str:
            first = hits[0]
            return (f"Summary for the question: {question} "
                    f"[{first.event_id} @ {first.ts:%Y-%m-%d %H:%M:%S}]")

    return FakeComposer()
