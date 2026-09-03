"""The centered abstention path, end to end against real pgvector.

Unit tests can prove the SQL is emitted and the branch is taken; only the real
database can prove the expression computes a cosine.
"""

import pytest

pytestmark = pytest.mark.integration


def test_centered_similarity_matches_a_hand_computed_cosine(conn):
    """1 - ((d - mu) <=> (q - mu)) is the cosine of the centered vectors."""
    import math

    from freshet.rag.retrieval import vector_sql

    d = [1.0, 0.0] + [0.0] * 766
    q = [0.0, 1.0] + [0.0] * 766
    mu = [0.25, 0.25] + [0.0] * 766
    dc = [a - b for a, b in zip(d, mu, strict=True)]
    qc = [a - b for a, b in zip(q, mu, strict=True)]
    dot = sum(a * b for a, b in zip(dc, qc, strict=True))
    expected = dot / (math.sqrt(sum(x * x for x in dc)) * math.sqrt(sum(x * x for x in qc)))

    row = conn.execute(
        "SELECT 1 - ((%(d)s::vector - %(centroid)s::vector)"
        " <=> (%(qvec)s::vector - %(centroid)s::vector))",
        {"d": str(d).replace(" ", ""), "qvec": str(q).replace(" ", ""),
         "centroid": str(mu).replace(" ", "")}).fetchone()
    assert abs(row[0] - expected) < 1e-6
    # and the builder actually emits that expression
    assert "(embedding - %(centroid)s::vector)" in vector_sql(None, None, centered=True)


def test_refresh_centroid_round_trips(conn):
    from freshet.pipeline.index_stats import (
        clear_cache,
        get_centroid,
        refresh_centroid,
    )

    clear_cache()
    n = refresh_centroid(conn, "BAAI/bge-base-en-v1.5")
    if n == 0:
        pytest.skip("no bge rows in this test database")
    stored = get_centroid(conn, "BAAI/bge-base-en-v1.5")
    assert stored is not None and stored.startswith("[")
    assert len(stored.split(",")) == 768
