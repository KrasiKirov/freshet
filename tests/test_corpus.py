from freshet.common.schemas import EventSource, EventType
from freshet.generator.generator import build_corpus_events, incident_ground_truth, SERVICES


def test_corpus_has_runbooks_and_n_incidents():
    events = build_corpus_events(seed=1, n_incidents=4, noise_between=5)
    runbooks = [e for e in events if e.source is EventSource.RUNBOOK]
    assert len(runbooks) == len(SERVICES)
    incident_ids = {e.incident_id for e in events if e.incident_id}
    assert len(incident_ids) == 4
    for iid in incident_ids:
        types = {e.type for e in events if e.incident_id == iid}
        assert EventType.DEPLOY_STARTED in types and EventType.ROLLBACK in types


def test_corpus_is_deterministic():
    a = [e.event_id for e in build_corpus_events(seed=7, n_incidents=3)]
    b = [e.event_id for e in build_corpus_events(seed=7, n_incidents=3)]
    assert a == b and len(a) == len(set(a))


def test_ground_truth_maps_cause_and_fix():
    events = build_corpus_events(seed=1, n_incidents=3)
    gt = incident_ground_truth(events)
    assert len(gt) == 3
    by_id = {e.event_id: e for e in events}
    for iid, (cause_id, fix_id) in gt.items():
        assert by_id[cause_id].type == EventType.DEPLOY_STARTED
        assert by_id[fix_id].type == EventType.ROLLBACK
        assert by_id[cause_id].incident_id == iid and by_id[fix_id].incident_id == iid
