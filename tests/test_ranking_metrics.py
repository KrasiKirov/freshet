import math

from freshet.eval.metrics import ndcg_at_k, precision_at_k, recall_at_k, reciprocal_rank


def test_recall_and_precision():
    ranked = ["a", "x", "b", "y", "z"]
    relevant = {"a", "b", "c"}  # 3 relevant; 2 of them in top-5
    assert recall_at_k(ranked, relevant, 5) == 2 / 3
    assert precision_at_k(ranked, relevant, 5) == 2 / 5
    # top-1 contains 1 relevant
    assert recall_at_k(ranked, relevant, 1) == 1 / 3
    assert precision_at_k(ranked, relevant, 1) == 1.0


def test_recall_empty_relevant_is_zero():
    assert recall_at_k(["a"], set(), 5) == 0.0


def test_reciprocal_rank():
    assert reciprocal_rank(["x", "a", "b"], {"a"}) == 1 / 2
    assert reciprocal_rank(["a", "x"], {"a"}) == 1.0
    assert reciprocal_rank(["x", "y"], {"a"}) == 0.0


def test_ndcg_perfect_and_imperfect():
    # perfect ranking: all relevant first
    assert ndcg_at_k(["a", "b", "x"], {"a", "b"}, 3) == 1.0
    # one relevant at rank 2 only: DCG = 1/log2(3); IDCG = 1/log2(2)=1
    got = ndcg_at_k(["x", "a", "y"], {"a"}, 3)
    assert abs(got - (1.0 / math.log2(3)) / 1.0) < 1e-9
    # no relevant retrieved
    assert ndcg_at_k(["x", "y"], {"a"}, 2) == 0.0
