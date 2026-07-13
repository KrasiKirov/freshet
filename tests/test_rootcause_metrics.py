from freshet.eval.rootcause import cause_accuracy, mrr, recall_at_k


def test_cause_accuracy_counts_exact_matches():
    gt = {"i1": "a", "i2": "b", "i3": "c"}
    sel = {"i1": "a", "i2": "x", "i3": None}
    assert cause_accuracy(gt, sel) == round(1 / 3, 3)


def test_mrr_uses_reciprocal_rank_of_true_cause():
    # true cause at rank 0 -> 1.0; rank 2 -> 1/3; absent -> 0
    gt = {"i1": "a", "i2": "b", "i3": "c"}
    ranked = {"i1": ["a", "z"], "i2": ["x", "y", "b"], "i3": ["p", "q"]}
    assert mrr(gt, ranked) == round((1.0 + 1 / 3 + 0.0) / 3, 3)


def test_recall_at_k_is_membership_in_retrieved_ids():
    gt = {"i1": "a", "i2": "b"}
    retrieved = {"i1": {"a", "z"}, "i2": {"x", "y"}}
    assert recall_at_k(gt, retrieved) == round(1 / 2, 3)
