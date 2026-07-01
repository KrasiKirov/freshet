"""Unit tests for multiquery_eval.aggregate (keyless)."""


def test_aggregate_computes_means_and_lift():
    from freshet.eval.multiquery_eval import aggregate
    out = aggregate([0.0, 1.0, 0.5, 0.5], [1.0, 1.0, 1.0, 0.0])
    assert out["single_recall@5"] == 0.5
    assert out["multi_recall@5"] == 0.75
    assert out["lift"] == 0.25
    assert out["n"] == 4


def test_aggregate_empty():
    from freshet.eval.multiquery_eval import aggregate
    out = aggregate([], [])
    assert out["single_recall@5"] == 0.0
    assert out["multi_recall@5"] == 0.0
    assert out["n"] == 0
