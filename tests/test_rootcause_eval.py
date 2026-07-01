from freshet.eval.rootcause import completeness


def test_completeness_counts_cause_and_fix_capture():
    gt = {"INC-1": ("cause1", "fix1"), "INC-2": ("cause2", "fix2")}
    captured = {"INC-1": {"cause1", "fix1", "x"}, "INC-2": {"cause2"}}  # INC-2 missed fix
    m = completeness(gt, captured)
    assert m["cause_recall"] == 1.0
    assert m["fix_recall"] == 0.5
    assert m["key_event_recall"] == 0.75
    assert m["incidents"] == 2
