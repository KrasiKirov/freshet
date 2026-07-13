from freshet.common.schemas import CHANGE_TYPES
from freshet.generator.generator import build_hard_benchmark


def _events_for(events, service):
    return sorted((e for e in events if e.service == service), key=lambda e: e.ts)


def test_hard_benchmark_deterministic():
    a, _ = build_hard_benchmark(seed=1, n_incidents=6)
    b, _ = build_hard_benchmark(seed=1, n_incidents=6)
    assert [(e.service, e.type, e.text, e.ts) for e in a] == \
           [(e.service, e.type, e.text, e.ts) for e in b]


def test_cause_is_the_bad_change_not_the_benign_decoy():
    events, truths = build_hard_benchmark(seed=1, n_incidents=6)
    by_id = {e.event_id: e for e in events}
    for t in truths:
        cause = by_id[t.cause_id]
        assert cause.type in CHANGE_TYPES        # the cause is a change event
        assert cause.incident_id == t.incident_id
        # the recorded cause is NOT the interposed benign decoy
        assert "benign" not in (cause.structured or {})


def test_benign_decoy_is_the_last_change_before_the_spike():
    """The property that makes the eval discriminating: naive last-before-spike
    would pick the benign decoy, not the true (bad) cause."""
    events, truths = build_hard_benchmark(seed=1, n_incidents=6)
    by_id = {e.event_id: e for e in events}
    for t in truths:
        focus = _events_for(events, t.service)
        spike = by_id[t.spike_id]
        changes_before = [e for e in focus
                          if e.type in CHANGE_TYPES and e.ts <= spike.ts]
        assert len(changes_before) >= 2                      # bad + interposed benign (+ volume)
        assert changes_before[-1].event_id != t.cause_id     # last-before-spike is NOT the cause
        assert (changes_before[-1].structured or {}).get("benign") is True


def test_in_scope_events_exceed_k_so_retrieval_matters():
    events, truths = build_hard_benchmark(seed=1, n_incidents=6)
    for t in truths:
        n = sum(1 for e in events if e.service == t.service)
        assert n > 12   # eval uses k=12; retrieval must select
