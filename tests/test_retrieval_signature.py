import inspect

from freshet.api.retrieval import hybrid_search


def test_hybrid_search_has_no_rerank_or_decay_params():
    """The rerank and recency-decay features are deleted, so their parameters
    go with them. `since` stays: it is a time-bounded-search capability wired
    into the SQL builders, not part of either deleted feature."""
    params = set(inspect.signature(hybrid_search).parameters)
    for gone in ("reranker", "rerank_pool", "tau_s", "now"):
        assert gone not in params, f"{gone} should have been deleted"
    assert params == {"conn", "embedder", "question", "k", "service",
                      "since", "min_similarity"}
