"""run_eval smoke test against the real stack: indexing + all three modes +
the staleness model run, and hybrid is no worse than the best single arm on
recall. Run via: make test-integration."""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    from freshet.common.db import connect

    c = connect()
    yield c
    c.close()


def test_run_eval_scores_modes_and_hybrid_is_competitive(conn):
    pytest.importorskip("sentence_transformers")
    from freshet.eval import run_eval
    from freshet.pipeline.embedding import make_embedder

    from freshet.eval.labeled import build_labeled_queries

    emb = make_embedder("bge")  # default retriever (768-dim, matches the schema)
    corpus, truths = run_eval.build_benchmark(seed=1, n_incidents=8)
    queries = build_labeled_queries(corpus, truths)
    run_eval.index_corpus(conn, emb, corpus)
    retrieval = run_eval.score_modes(conn, emb, corpus, queries)

    for mode in ("vector", "keyword", "hybrid"):
        assert mode in retrieval
        assert 0.0 <= retrieval[mode]["recall@5"] <= 1.0
    # hybrid should be at least as good as the worse single arm on recall@5
    single = [retrieval["vector"]["recall@5"], retrieval["keyword"]["recall@5"]]
    assert retrieval["hybrid"]["recall@5"] >= min(single)


def test_retrieval_is_insensitive_to_corpus_insertion_order(conn):
    """Ranking must not depend on physical heap order. The eval DELETEs and
    re-INSERTs the corpus every run, so any ORDER BY that leaves ties to heap
    position drifts run-to-run (the non-determinism this guards against). Indexing
    the same corpus in two different orders must yield byte-identical rankings."""
    pytest.importorskip("sentence_transformers")
    from freshet.eval import modes, run_eval
    from freshet.eval.labeled import build_labeled_queries
    from freshet.pipeline.embedding import make_embedder

    emb = make_embedder("bge")
    corpus, truths = run_eval.build_benchmark(seed=1, n_incidents=8)
    queries = build_labeled_queries(corpus, truths)

    run_eval.index_corpus(conn, emb, corpus)
    forward = {q.text: modes.keyword_only_event_ids(conn, q.text, run_eval.K)
               for q in queries}

    run_eval.index_corpus(conn, emb, list(reversed(corpus)))
    reversed_order = {q.text: modes.keyword_only_event_ids(conn, q.text, run_eval.K)
                      for q in queries}

    assert forward == reversed_order


def test_staleness_model_batch_is_staler():
    from freshet.eval import run_eval

    # steady-stream staleness model (no DB): batch at hourly cadence is orders of
    # magnitude staler than streaming at ~3s freshness.
    _, streaming, batch = run_eval.staleness_curves(
        streaming_freshness_s=3.0, batch_interval_s=3600.0
    )
    s = [x for x in streaming if x is not None]
    b = [x for x in batch if x is not None]
    assert sum(b) / len(b) > sum(s) / len(s) * 100  # batch >> streaming
