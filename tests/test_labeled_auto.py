from freshet.eval.labeled import build_labeled_queries, relevant_event_ids
from freshet.generator.generator import build_benchmark


def test_auto_queries_scale_and_resolve():
    events, truths = build_benchmark(seed=1, n_incidents=10)
    queries = build_labeled_queries(events, truths)
    assert len(queries) == 40                       # 4 templates x 10 incidents
    assert all(relevant_event_ids(q, events) for q in queries)
    iids = {t.incident_id for t in truths}
    assert all(q.incident_id in iids for q in queries)
