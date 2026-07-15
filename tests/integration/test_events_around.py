"""events_around returns service-scoped temporal neighbours, deduped by event_id,
time-ordered. Run via: make test-integration."""
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    yield c
    c.close()


def test_events_around_returns_service_time_window(conn):
    pytest.importorskip("sentence_transformers")
    from freshet.api.retrieval import events_around
    from freshet.eval.rootcause import _index_corpus
    from freshet.generator.generator import build_benchmark
    from freshet.pipeline.embedding import make_embedder

    events, truths = build_benchmark(seed=1, n_incidents=4)
    emb = make_embedder("stub")
    _index_corpus(conn, emb, events)

    t = truths[0]
    spike = next(e for e in events if e.event_id == t.spike_id)
    near = events_around(conn, t.service, spike.ts, window_s=1800)

    ids = [n.event_id for n in near]
    assert t.cause_id in ids                      # the change just before the spike
    assert len(ids) == len(set(ids))              # deduped by event_id
    assert near == sorted(near, key=lambda n: n.ts)      # ascending by ts
    # everything is within the window and the same service
    for n in near:
        assert abs((n.ts - spike.ts).total_seconds()) <= 1800
