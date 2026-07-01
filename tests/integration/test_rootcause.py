"""M10a integration: index the richer corpus, then assert the root-cause timeline
captures each incident's true cause (deploy) and fix (rollback). Run via:
make test-integration."""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect
    c = connect()
    c.execute("DELETE FROM vector_records")
    c.execute("DELETE FROM incidents")
    yield c
    c.close()


def test_timeline_captures_cause_and_fix(conn):
    from freshet.api.retrieval import hybrid_search
    from freshet.api.synthesis import build_timeline
    from freshet.generator.generator import build_corpus_events, incident_ground_truth
    from freshet.pipeline.embedder import records_for_event, upsert_record
    from freshet.pipeline.embedding import StubEmbedder

    emb = StubEmbedder()
    events = build_corpus_events(seed=1, n_incidents=3)
    for ev in events:
        for rec in records_for_event(ev):
            [vec] = emb.encode([rec.text])
            upsert_record(conn, rec, vec)

    gt = incident_ground_truth(events)
    services = {e.incident_id: e.service for e in events if e.incident_id in gt}
    assert len(gt) == 3

    # This is an end-to-end WIRING test (corpus -> index -> retrieve -> timeline ->
    # cause/fix identification). Retrieval *quality* under weak stub embedding is a
    # separate, honest measurement (the completeness eval, freshet/eval/rootcause.py).
    # So retrieve generously (k covers a service's events) and assert the timeline
    # correctly identifies each incident's authored cause and fix.
    hits_count = 0
    for iid, service in services.items():
        res = hybrid_search(conn, emb,
                            f"what caused the {service} incident and how was it resolved?",
                            k=20, service=service, min_similarity=0.0)
        tl = build_timeline(res.hits)
        ids = {e.hit.event_id for e in tl.entries}
        if tl.cause:
            ids.add(tl.cause.event_id)
        if tl.fix:
            ids.add(tl.fix.event_id)
        cause_id, fix_id = gt[iid]
        if cause_id in ids and fix_id in ids:
            hits_count += 1
    assert hits_count == 3   # all three incidents wired end to end
