from freshet.generator.impact_scenarios import ImpactTruth, build_impact_benchmark


def test_spans_all_three_labels():
    _events, truths = build_impact_benchmark(seed=1)
    labels = {t.label for t in truths}
    assert labels == {"Low", "Medium", "High"}


def test_deterministic_under_seed():
    e1, t1 = build_impact_benchmark(seed=1)
    e2, t2 = build_impact_benchmark(seed=1)
    assert [e.event_id for e in e1] == [e.event_id for e in e2]
    assert [(t.incident_id, t.label) for t in t1] == [(t.incident_id, t.label) for t in t2]


def test_breadth_varies_and_stated_pct_present():
    events, truths = build_impact_benchmark(seed=1)
    by_inc = {}
    for e in events:
        by_inc.setdefault(e.incident_id, []).append(e)
    breadths = {len({e.service for e in evs}) for evs in by_inc.values()}
    assert max(breadths) >= 3  # at least one multi-service incident
    # at least one incident states an error percentage in its text
    assert any("%" in e.text for e in events)


def test_truth_ids_match_events():
    events, truths = build_impact_benchmark(seed=1)
    ev_incidents = {e.incident_id for e in events}
    assert all(isinstance(t, ImpactTruth) and t.incident_id in ev_incidents for t in truths)
