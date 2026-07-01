"""End-to-end vertical-slice test: generator -> Kafka -> normalizer ->
embedder (stub) -> pgvector -> search + freshness. Requires the compose stack
(make up) with the schema applied. Run via: make test-integration.

Uses run-unique topics/groups so it is isolated from prior stack activity,
and clears vector_records (the dev stack's table) for deterministic counts.
"""

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

BROKERS = os.environ.get("FRESHET_BROKERS", "localhost:9092")


@pytest.fixture
def conn():
    from freshet.common.db import connect

    c = connect()
    c.execute("DELETE FROM vector_records")
    yield c
    c.close()


def test_slice_end_to_end(conn):
    from freshet.api.retrieval import hybrid_search
    from freshet.eval.freshness import freshness_report
    from freshet.generator.generator import EventGenerator, KafkaSink, live_stream
    from freshet.pipeline import embedder, normalizer
    from freshet.pipeline.embedding import StubEmbedder

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.events.it{run_id}"
    norm_topic = f"normalized.events.it{run_id}"

    # produce 20 noise + 9 scripted = 29 live-stamped events. incident_after
    # must be < count or the scripted incident never injects (default is 20).
    sink = KafkaSink(BROKERS, raw_topic)
    produced = 0
    for ev in live_stream(EventGenerator(seed=3, incident_after=10), count=20, spacing_s=0):
        sink.write(ev)
        produced += 1
    sink.close()
    assert produced == 29

    n = normalizer.run(
        BROKERS, group=f"norm-{run_id}", max_messages=29,
        raw_topic=raw_topic, normalized_topic=norm_topic,
    )
    assert n == 29

    n = embedder.run(
        BROKERS, group=f"emb-{run_id}", max_messages=29,
        topic=norm_topic, embedder=StubEmbedder(),
    )
    assert n == 29

    total, distinct = conn.execute(
        "SELECT count(*), count(DISTINCT event_id) FROM vector_records"
    ).fetchone()
    assert total == 29 and distinct == 29

    # idempotency: a fresh group re-reads the topic; row count must not change
    n = embedder.run(
        BROKERS, group=f"emb2-{run_id}", max_messages=29,
        topic=norm_topic, embedder=StubEmbedder(),
    )
    assert n == 29
    assert conn.execute("SELECT count(*) FROM vector_records").fetchone()[0] == 29

    # all three timestamps flowed through; freshness is small and non-negative
    lats = [
        r[0]
        for r in conn.execute(
            "SELECT EXTRACT(EPOCH FROM (indexed_at - ts))::float8 FROM vector_records"
        ).fetchall()
    ]
    rep = freshness_report(lats)
    assert rep["count"] == 29
    assert 0 <= rep["p50_s"] < 120

    # query path: scored, timestamped hits in descending score order.
    # min_similarity=0.0 disables abstention (stub vectors give noise similarity).
    result = hybrid_search(
        conn, StubEmbedder(), "error spike on scheduler-api", k=5, min_similarity=0.0
    )
    assert len(result.hits) == 5
    scores = [h.score for h in result.hits]
    assert scores == sorted(scores, reverse=True)

    # metadata filter narrows to the incident service
    result = hybrid_search(
        conn, StubEmbedder(), "error spike", k=5, service="scheduler-api", min_similarity=0.0
    )
    assert all(h.service == "scheduler-api" for h in result.hits)
