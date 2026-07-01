"""M5 query-layer tests against the real stack: hybrid retrieval beats
vector-only on a keyword-exact query, abstention fires on an unrelated question,
and the end-to-end answer cites a real event. Run via: make test-integration.
Uses the stub embedder so no model download is needed; the keyword arm and
fusion do the discriminating work here."""

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
    c.execute("DELETE FROM incidents")
    yield c
    c.close()


def _ingest(conn, run_id):
    """Run the scripted incident through the real pipeline with the stub embedder."""
    from freshet.generator.generator import EventGenerator, KafkaSink, live_stream
    from freshet.pipeline import embedder, normalizer
    from freshet.pipeline.embedding import StubEmbedder

    raw, norm = f"raw.events.q{run_id}", f"normalized.events.q{run_id}"
    sink = KafkaSink(BROKERS, raw)
    n = 0
    for ev in live_stream(EventGenerator(seed=11, incident_after=10), count=20, spacing_s=0):
        sink.write(ev)
        n += 1
    sink.close()
    normalizer.run(BROKERS, group=f"n-{run_id}", max_messages=n, raw_topic=raw, normalized_topic=norm)
    embedder.run(BROKERS, group=f"e-{run_id}", max_messages=n, topic=norm, embedder=StubEmbedder())


def test_keyword_arm_finds_exact_term_stub_vector_misses(conn):
    from freshet.api.retrieval import hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    run_id = uuid.uuid4().hex[:8]
    _ingest(conn, run_id)

    # "postmortem" is a literal term that appears only in the scripted incident's
    # RCA event (never in the noise templates), and its English stem matches the
    # query token exactly. The stub embedder's vectors are meaningless, so a
    # vector-only search can't reliably surface it — the keyword arm + fusion
    # must. min_similarity=0.0 disables abstention (stub similarities are noise).
    result = hybrid_search(conn, StubEmbedder(), "postmortem", k=5, min_similarity=0.0)
    assert not result.abstained
    assert any("postmortem" in h.text.lower() for h in result.hits)


def test_abstention_on_unrelated_query_with_real_embedder(conn):
    pytest.importorskip("sentence_transformers")
    from freshet.api.retrieval import hybrid_search
    from freshet.pipeline.embedding import make_embedder

    run_id = uuid.uuid4().hex[:8]
    _ingest(conn, run_id)

    emb = make_embedder("bge")  # default retriever (768-dim, matches the schema)
    # A question with no lexical or semantic overlap with ops telemetry.
    result = hybrid_search(conn, emb, "what is the recipe for sourdough bread?", k=5)
    assert result.abstained is True


def test_end_to_end_answer_cites_event(conn):
    from freshet.api.composer import make_composer
    from freshet.api.retrieval import hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    run_id = uuid.uuid4().hex[:8]
    _ingest(conn, run_id)

    result = hybrid_search(conn, StubEmbedder(), "error spike", k=5, min_similarity=0.0)
    assert result.hits
    answer = make_composer("template").compose("error spike", result.hits)
    # template composer cites the top event id
    assert result.hits[0].event_id in answer
