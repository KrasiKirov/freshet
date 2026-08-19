from datetime import UTC, datetime

from freshet.api.retrieval import keyword_sql, vector_sql


def test_vector_sql_has_similarity_and_order():
    sql = vector_sql(None, None)
    assert "1 - (embedding <=> %(qvec)s::vector) AS similarity" in sql
    assert "ORDER BY embedding <=> %(qvec)s::vector" in sql
    assert "WHERE" not in sql


def test_keyword_sql_uses_or_tsquery_and_rank():
    sql = keyword_sql(None, None)
    # user input is still parsed by websearch_to_tsquery (sanitized), then the
    # &-operators are swapped for | to make the candidate arm high-recall
    assert "websearch_to_tsquery('english', %(q)s)" in sql
    assert "replace(" in sql and "'&', '|'" in sql and "::tsquery" in sql
    assert "ts_rank(text_tsv," in sql and "AS rank" in sql
    assert "text_tsv @@" in sql
    assert "ORDER BY rank DESC" in sql


def test_filters_apply_to_both_arms():
    now = datetime.now(UTC)
    assert "service = %(service)s" in vector_sql("scheduler-api", None)
    assert "ts >= %(since)s" in vector_sql(None, now)
    kw = keyword_sql("scheduler-api", now)
    assert "service = %(service)s" in kw and "ts >= %(since)s" in kw


def test_rrf_rewards_agreement_across_arms():
    from freshet.api.retrieval import reciprocal_rank_fusion

    vec = ["a", "b", "c"]
    kw = ["b", "d"]
    fused = reciprocal_rank_fusion([vec, kw])
    # b appears high in both arms -> should rank first
    assert fused[0][0] == "b"
    # every id from both arms is present
    assert {cid for cid, _ in fused} == {"a", "b", "c", "d"}
    # scores are descending
    scores = [s for _, s in fused]
    assert scores == sorted(scores, reverse=True)




def test_should_abstain_on_weak_similarity():
    from freshet.api.retrieval import should_abstain

    assert should_abstain([], min_similarity=0.3) is True
    assert should_abstain([0.05, 0.1], min_similarity=0.3) is True
    assert should_abstain([0.42, 0.1], min_similarity=0.3) is False


def test_hybrid_search_fuses_arms_and_flags_abstention():
    from datetime import datetime

    from freshet.api.retrieval import HybridResult, hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    now = datetime.now(UTC)
    # vector rows: (..., similarity)   keyword rows: (..., rank, similarity)
    vec_rows = [
        ("chk_e1_0", "e1", "scheduler-api", now, now, "alert", "5xx error spike", "alert_fired", 0.81),
        ("chk_e2_0", "e2", "scheduler-api", now, now, "deploy", "deploy finished", "deploy_finished", 0.40),
    ]
    kw_rows = [
        ("chk_e2_0", "e2", "scheduler-api", now, now, "deploy", "deploy finished", "deploy_finished", 0.9, 0.55),
    ]

    class FakeConn:
        def __init__(self):
            self.calls = 0

        def execute(self, sql, params=None):
            self.calls += 1
            rows = kw_rows if "ts_rank" in sql else vec_rows

            class _Cur:
                def fetchall(self_inner):
                    return rows

            return _Cur()

    result = hybrid_search(FakeConn(), StubEmbedder(), "error spike", k=5)
    assert isinstance(result, HybridResult)
    assert result.abstained is False          # 0.81 >= default 0.3
    ids = [h.event_id for h in result.hits]
    assert set(ids) == {"e1", "e2"}           # union of both arms
    assert "e2" in ids                         # found by both -> survives fusion


def test_hybrid_search_uses_embedder_min_similarity():
    """The abstention floor defaults to the embedder's per-model attribute
    (bge's compressed cosine range needs a higher floor than MiniLM's)."""
    from datetime import datetime

    from freshet.api.retrieval import hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    class HighFloorEmbedder(StubEmbedder):
        min_similarity = 0.9

    now = datetime.now(UTC)
    rows = [("chk_e1_0", "e1", "scheduler-api", now, now, "alert", "5xx spike", "alert_fired", 0.81)]

    class FakeConn:
        def execute(self, sql, params=None):
            class _Cur:
                def fetchall(self_inner):
                    return [] if "ts_rank" in sql else rows   # keyword arm finds nothing

            return _Cur()

    # 0.81 clears StubEmbedder's default floor (0.3) but not the 0.9 attribute
    assert hybrid_search(FakeConn(), StubEmbedder(), "q", k=5).abstained is False
    assert hybrid_search(FakeConn(), HighFloorEmbedder(), "q", k=5).abstained is True
    # an explicit argument still wins over the embedder attribute
    assert hybrid_search(FakeConn(), HighFloorEmbedder(), "q", k=5,
                         min_similarity=0.0).abstained is False



def test_hybrid_search_abstains_when_similarity_weak():
    from datetime import datetime

    from freshet.api.retrieval import hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    now = datetime.now(UTC)
    weak = [("chk_e9_0", "e9", "auth", now, now, "metric", "cpu 12%", "metric", 0.04)]

    class FakeConn:
        def execute(self, sql, params=None):
            class _Cur:
                def fetchall(self_inner):
                    return [] if "ts_rank" in sql else weak   # keyword arm finds nothing

            return _Cur()

    result = hybrid_search(FakeConn(), StubEmbedder(), "unrelated question", k=5)
    assert result.abstained is True


def test_keyword_only_hits_carry_a_real_similarity_not_zero():
    """A hit found only by the lexical arm used to get similarity 0.0 — a MISSING
    value, not a measured one. Abstention keys off cosine, so an exact lexical
    match with no vector match was silently discarded as "no evidence"."""
    from freshet.api.retrieval import keyword_sql

    sql = keyword_sql(None, None)
    assert "embedding <=>" in sql, (
        "the keyword arm must compute cosine too, so every hit has a true "
        "similarity and abstention needs no invented threshold")
    assert "AS similarity" in sql


def test_abstention_uses_the_similarity_of_a_keyword_only_hit():
    from freshet.api.retrieval import should_abstain

    # a strong lexical match whose cosine was measured, not defaulted
    assert should_abstain([0.82], min_similarity=0.70) is False
    assert should_abstain([0.0], min_similarity=0.70) is True
