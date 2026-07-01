from freshet.common.schemas import CHANGE_TYPES, REMEDIATION_TYPES, EventSource
from freshet.generator.generator import build_benchmark, SERVICES


def test_benchmark_has_n_incidents_across_archetypes():
    events, truths = build_benchmark(seed=1, n_incidents=40)
    assert len(truths) == 40
    assert len({t.archetype for t in truths}) >= 5
    assert len([e for e in events if e.source is EventSource.RUNBOOK]) == len(SERVICES)


def test_ground_truth_ids_resolve_to_cause_and_fix_types():
    events, truths = build_benchmark(seed=1, n_incidents=12)
    by_id = {e.event_id: e for e in events}
    for t in truths:
        assert by_id[t.cause_id].type in CHANGE_TYPES
        assert by_id[t.fix_id].type in REMEDIATION_TYPES
        assert by_id[t.cause_id].incident_id == t.incident_id
        assert by_id[t.fix_id].incident_id == t.incident_id


def test_benchmark_is_deterministic():
    a = [e.event_id for e in build_benchmark(seed=7, n_incidents=6)[0]]
    b = [e.event_id for e in build_benchmark(seed=7, n_incidents=6)[0]]
    assert a == b and len(a) == len(set(a))
