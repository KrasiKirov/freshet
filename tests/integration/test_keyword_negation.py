"""The &->| swap must not invert negation — provable only against real Postgres.

Every other keyword-arm test asserts on SQL *strings* against a fake connection,
which is exactly why this survived: the bug is in what the tsquery MEANS, not in
how the SQL reads. So this test seeds a known corpus and counts rows.
"""

from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.integration

# Three groups. The distinguishing rows are UNRELATED: they mention neither
# word, so `'outag' & !'mainten'` correctly excludes them while the naive
# `'outag' | !'mainten'` admits them purely for lacking "maintenance". That gap
# is the bug — on the live index it grew to 88% of all rows.
_MAINTENANCE = 3
_OUTAGE = 27
_UNRELATED = 10


@pytest.fixture
def seeded(conn):
    from freshet.common.schemas import EventSource, VectorRecord
    from freshet.pipeline.embedder import upsert_record
    from freshet.pipeline.embedding import StubEmbedder

    conn.execute("DELETE FROM vector_records WHERE chunk_id LIKE 'neg_%'")
    emb, now = StubEmbedder(), datetime.now(UTC)
    texts = (
        [f"scheduled maintenance window number {i} completed" for i in range(_MAINTENANCE)]
        + [f"outage affecting api requests in region {i}" for i in range(_OUTAGE)]
        + [f"database latency elevated in cluster {i}" for i in range(_UNRELATED)]
    )
    for i, text in enumerate(texts):
        rec = VectorRecord(
            chunk_id=f"neg_{i}_0", event_id=f"neg_{i}", incident_id=None,
            service="acme", ts=now, indexed_at=now, text=text, title="t",
            source=EventSource.ALERT, severity=None, type="status_update",
        )
        upsert_record(conn, rec, emb.encode([text])[0], emb.name)
    yield conn
    conn.execute("DELETE FROM vector_records WHERE chunk_id LIKE 'neg_%'")


def _rows(conn, expr, q):
    return conn.execute(
        f"SELECT count(*) FROM vector_records"
        f" WHERE chunk_id LIKE 'neg_%%' AND text_tsv @@ {expr}",
        {"q": q}).fetchone()[0]


def test_negated_query_does_not_match_the_whole_corpus(seeded):
    from freshet.rag.retrieval import _OR_TSQUERY, _WS_TSQUERY

    naive = "replace(websearch_to_tsquery('english', %(q)s)::text, '&', '|')::tsquery"
    q = "outage -maintenance"

    shipped = _rows(seeded, _OR_TSQUERY, q)
    # the shipped expression agrees with plain websearch on a negated query:
    # only the outage rows, none of which mention maintenance
    assert shipped == _rows(seeded, _WS_TSQUERY, q) == _OUTAGE
    # ...while the naive swap also drags in every UNRELATED row, purely for
    # lacking the negated word. On the live index that reached 88% of all rows.
    assert _rows(seeded, naive, q) == _OUTAGE + _UNRELATED


def test_plain_query_still_gets_or_breadth(seeded):
    """The swap exists to buy recall; only negated queries opt out of it."""
    from freshet.rag.retrieval import _OR_TSQUERY, _WS_TSQUERY

    q = "maintenance outage"
    # AND matches nothing here (no row has both words); OR matches both groups
    assert _rows(seeded, _WS_TSQUERY, q) == 0
    assert _rows(seeded, _OR_TSQUERY, q) == _MAINTENANCE + _OUTAGE
