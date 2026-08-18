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


def test_trap_positions_are_randomised_not_fixed():
    """THE property that makes the benchmark ungameable. Earlier revisions planted
    every trap at a constant offset, so a blind rule ('second-to-last change',
    'second remediation') scored 1.000 and beat the LLM while understanding nothing.
    The cause's index from the end must VARY across incidents — including cases
    where it is last, which is what also defeats an inverse skip-one rule."""
    from freshet.common.schemas import REMEDIATION_TYPES

    events, truths = build_hard_benchmark(seed=1, n_incidents=40)
    by_id = {e.event_id: e for e in events}
    cause_idx, fix_idx = set(), set()
    for t in truths:
        if not t.cause_recoverable:
            continue
        focus = _events_for(events, t.service)
        spike = by_id[t.spike_id]
        changes = [e for e in focus if e.type in CHANGE_TYPES and e.ts <= spike.ts]
        rems = [e for e in focus
                if e.type in REMEDIATION_TYPES and e.ts >= spike.ts]
        cause_idx.add(len(changes) - 1 - changes.index(by_id[t.cause_id]))
        fix_idx.add(rems.index(by_id[t.fix_id]))
    # more than one distinct position on both axes => no single index rule can win
    assert len(cause_idx) > 1, f"cause always at fixed position {cause_idx}"
    assert len(fix_idx) > 1, f"fix always at fixed position {fix_idx}"
    # and the naive rule must be right at least sometimes (the zero-trap case)
    assert 0 in cause_idx and 0 in fix_idx


def test_fix_is_not_always_the_last_remediation():
    """Guards the second leak the guard arm caught: with every failed attempt
    preceding the real fix, a blind 'last remediation' rule scored 0.93. Cleanup
    remediations after recovery must make the fix's index-from-the-end vary too."""
    from freshet.common.schemas import REMEDIATION_TYPES

    events, truths = build_hard_benchmark(seed=1, n_incidents=40)
    by_id = {e.event_id: e for e in events}
    from_end = set()
    for t in truths:
        focus = _events_for(events, t.service)
        spike = by_id[t.spike_id]
        rems = [e for e in focus
                if e.type in REMEDIATION_TYPES and e.ts >= spike.ts]
        from_end.add(len(rems) - 1 - rems.index(by_id[t.fix_id]))
    assert len(from_end) > 1, f"fix always {from_end} from the end"


def test_some_incidents_require_abstention():
    """Out-of-window causes: without these, an arm that always guesses is never
    penalised for confidently naming a bystander."""
    _, truths = build_hard_benchmark(seed=1, n_incidents=40)
    unrec = [t for t in truths if not t.cause_recoverable]
    assert 0 < len(unrec) < len(truths)


def test_distractors_are_not_self_labelling():
    """The decoys must not announce themselves; otherwise an arm wins by matching
    an adjective rather than by reasoning over evidence."""
    events, _ = build_hard_benchmark(seed=1, n_incidents=12)
    tells = ("docs-only", "unchanged", "no change", "no effect", "benign")
    for e in events:
        assert not any(t in e.text.lower() for t in tells), e.text


def test_in_scope_events_exceed_k_so_retrieval_matters():
    events, truths = build_hard_benchmark(seed=1, n_incidents=6)
    for t in truths:
        n = sum(1 for e in events if e.service == t.service)
        assert n > 12   # eval uses k=12; retrieval must select


def test_every_archetype_declares_its_hard_tier_fields():
    """These four used to live in parallel name-keyed dicts, so adding an
    archetype and forgetting one surfaced as a KeyError at corpus-generation time
    with nothing to catch it. They are constructor-required fields now; this test
    is the backstop that they are also non-empty and internally consistent."""
    from freshet.generator.scenarios import ARCHETYPES

    for a in ARCHETYPES:
        assert a.symptom, a.name
        assert a.cause_signature, a.name
        assert a.benign_decoy and len(a.benign_decoy) == 3, a.name
        assert a.ineffective_fix and len(a.ineffective_fix) == 3, a.name
        real_fix_type = next(s.type for s in a.steps if s.role == "remediation")
        # the ineffective remediation must never share the real fix's type,
        # otherwise ground truth for the fix task becomes ambiguous
        assert a.ineffective_fix[1] != real_fix_type, a.name
