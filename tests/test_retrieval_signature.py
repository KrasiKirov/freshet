import inspect

from freshet.rag.retrieval import hybrid_search


def test_hybrid_search_has_no_rerank_or_decay_params():
    """The rerank and recency-decay features are deleted, so their parameters
    go with them. `since` stays: it is a time-bounded-search capability wired
    into the SQL builders, not part of either deleted feature.

    `exclude_event_id` likewise stays: it drops the query's OWN document at the
    SQL level so ranking and abstention see one candidate set. Without it the
    live eval's abstention count was measuring "is this text in the index",
    which is trivially yes when the query is lifted from an indexed update.
    """
    params = set(inspect.signature(hybrid_search).parameters)
    for gone in ("reranker", "rerank_pool", "tau_s", "now"):
        assert gone not in params, f"{gone} should have been deleted"
    assert params == {"conn", "embedder", "question", "k", "service",
                      "since", "min_similarity", "exclude_event_id"}
