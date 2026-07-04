from freshet.eval.impact_eval import evaluate


def test_evaluate_is_deterministic_and_bounded():
    a = evaluate(seed=1)
    b = evaluate(seed=1)
    assert a == b
    assert a["n"] == 12
    assert 0.0 <= a["exact_agreement"] <= 1.0
    assert a["adjacent_agreement"] >= a["exact_agreement"]


def test_evaluate_has_some_agreement_and_some_misses():
    a = evaluate(seed=1)
    # the benchmark is designed so proxies recover most but not all labels
    assert a["exact_agreement"] > 0.5
    assert len(a["confusion"]) >= 1
