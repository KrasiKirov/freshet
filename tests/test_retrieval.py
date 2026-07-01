from datetime import datetime, timezone

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
    now = datetime.now(timezone.utc)
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


def test_recency_weight_decays_with_age():
    from freshet.api.retrieval import recency_weight

    now_w = recency_weight(0.0, tau_s=3600.0)
    old_w = recency_weight(7200.0, tau_s=3600.0)
    assert now_w == 1.0
    assert 0.0 < old_w < now_w
    assert abs(old_w - pow(2.718281828, -2.0)) < 1e-3


def test_should_abstain_on_weak_similarity():
    from freshet.api.retrieval import should_abstain

    assert should_abstain([], min_similarity=0.3) is True
    assert should_abstain([0.05, 0.1], min_similarity=0.3) is True
    assert should_abstain([0.42, 0.1], min_similarity=0.3) is False


def test_hybrid_search_fuses_arms_and_flags_abstention():
    from datetime import datetime, timezone

    from freshet.api.retrieval import HybridResult, hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    now = datetime.now(timezone.utc)
    # rows: (chunk_id, event_id, service, ts, indexed_at, source, text, type, score)
    vec_rows = [
        ("chk_e1_0", "e1", "scheduler-api", now, now, "alert", "5xx error spike", "alert_fired", 0.81),
        ("chk_e2_0", "e2", "scheduler-api", now, now, "deploy", "deploy finished", "deploy_finished", 0.40),
    ]
    kw_rows = [
        ("chk_e2_0", "e2", "scheduler-api", now, now, "deploy", "deploy finished", "deploy_finished", 0.9),
    ]

    class FakeConn:
        def __init__(self):
            self.calls = 0

        def execute(self, sql, params=None):
            self.calls += 1
            rows = vec_rows if "embedding <=>" in sql else kw_rows

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


def test_hybrid_search_abstains_when_similarity_weak():
    from datetime import datetime, timezone

    from freshet.api.retrieval import hybrid_search
    from freshet.pipeline.embedding import StubEmbedder

    now = datetime.now(timezone.utc)
    weak = [("chk_e9_0", "e9", "auth", now, now, "metric", "cpu 12%", "metric", 0.04)]

    class FakeConn:
        def execute(self, sql, params=None):
            class _Cur:
                def fetchall(self_inner):
                    return weak if "embedding <=>" in sql else []

            return _Cur()

    result = hybrid_search(FakeConn(), StubEmbedder(), "unrelated question", k=5)
    assert result.abstained is True
