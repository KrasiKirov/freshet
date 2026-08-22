from datetime import UTC, datetime

from freshet.rag.retrieval import keyword_sql, vector_sql


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
    from freshet.rag.retrieval import reciprocal_rank_fusion

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
    from freshet.rag.retrieval import should_abstain

    assert should_abstain([], min_similarity=0.3) is True
    assert should_abstain([0.05, 0.1], min_similarity=0.3) is True
    assert should_abstain([0.42, 0.1], min_similarity=0.3) is False


def test_hybrid_search_fuses_arms_and_flags_abstention():
    from datetime import datetime

    from freshet.pipeline import index_stats
    from freshet.pipeline.embedding import StubEmbedder
    from freshet.rag.retrieval import HybridResult, hybrid_search

    index_stats.clear_cache()
    now = datetime.now(UTC)
    # column order mirrors retrieval._COLS: (..., type, title) then the per-arm
    # score columns — vector: similarity, centered_similarity; keyword: rank,
    # similarity, centered_similarity. centered is NULL with no stored centroid.
    vec_rows = [
        ("chk_e1_0", "e1", "scheduler-api", now, now, "alert", "5xx error spike", "alert_fired", "Error spike in scheduler", 0.81, None),
        ("chk_e2_0", "e2", "scheduler-api", now, now, "deploy", "deploy finished", "deploy_finished", "Deploy of scheduler-api", 0.40, None),
    ]
    kw_rows = [
        ("chk_e2_0", "e2", "scheduler-api", now, now, "deploy", "deploy finished", "deploy_finished", "Deploy of scheduler-api", 0.9, 0.55, None),
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

                def fetchone(self_inner):
                    return None                   # index_stats holds no centroid

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

    from freshet.pipeline import index_stats
    from freshet.pipeline.embedding import StubEmbedder
    from freshet.rag.retrieval import hybrid_search

    class HighFloorEmbedder(StubEmbedder):
        min_similarity = 0.9

    index_stats.clear_cache()
    now = datetime.now(UTC)
    rows = [("chk_e1_0", "e1", "scheduler-api", now, now, "alert", "5xx spike",
             "alert_fired", "Error spike in scheduler", 0.81, None)]

    class FakeConn:
        def execute(self, sql, params=None):
            class _Cur:
                def fetchall(self_inner):
                    return [] if "ts_rank" in sql else rows   # keyword arm finds nothing

                def fetchone(self_inner):
                    return None                               # no stored centroid

            return _Cur()

    # 0.81 clears StubEmbedder's default floor (0.3) but not the 0.9 attribute
    assert hybrid_search(FakeConn(), StubEmbedder(), "q", k=5).abstained is False
    assert hybrid_search(FakeConn(), HighFloorEmbedder(), "q", k=5).abstained is True
    # an explicit argument still wins over the embedder attribute
    assert hybrid_search(FakeConn(), HighFloorEmbedder(), "q", k=5,
                         min_similarity=0.0).abstained is False



def test_hybrid_search_abstains_when_similarity_weak():
    from datetime import datetime

    from freshet.pipeline import index_stats
    from freshet.pipeline.embedding import StubEmbedder
    from freshet.rag.retrieval import hybrid_search

    index_stats.clear_cache()
    now = datetime.now(UTC)
    weak = [("chk_e9_0", "e9", "auth", now, now, "metric", "cpu 12%", "metric", None, 0.04, None)]

    class FakeConn:
        def execute(self, sql, params=None):
            class _Cur:
                def fetchall(self_inner):
                    return [] if "ts_rank" in sql else weak   # keyword arm finds nothing

                def fetchone(self_inner):
                    return None                               # no stored centroid

            return _Cur()

    result = hybrid_search(FakeConn(), StubEmbedder(), "unrelated question", k=5)
    assert result.abstained is True


def test_keyword_only_hits_carry_a_real_similarity_not_zero():
    """A hit found only by the lexical arm used to get similarity 0.0 — a MISSING
    value, not a measured one. Abstention keys off cosine, so an exact lexical
    match with no vector match was silently discarded as "no evidence"."""
    from freshet.rag.retrieval import keyword_sql

    sql = keyword_sql(None, None)
    assert "embedding <=>" in sql, (
        "the keyword arm must compute cosine too, so every hit has a true "
        "similarity and abstention needs no invented threshold")
    assert "AS similarity" in sql


def test_abstention_uses_the_similarity_of_a_keyword_only_hit():
    from freshet.rag.retrieval import should_abstain

    # a strong lexical match whose cosine was measured, not defaulted
    assert should_abstain([0.82], min_similarity=0.70) is False
    assert should_abstain([0.0], min_similarity=0.70) is True


def test_exclude_event_id_filters_both_arms():
    """The query's own document must leave the candidate set BEFORE ranking and
    abstention, not just before the eval's dedupe — otherwise a query lifted
    from an indexed update abstains on nothing, trivially."""
    from freshet.rag.retrieval import keyword_sql, vector_sql

    vec = vector_sql(None, None, exclude_event_id="prov:inc:upd")
    kw = keyword_sql(None, None, exclude_event_id="prov:inc:upd")
    assert "event_id <> %(exclude_event_id)s" in vec
    assert "event_id <> %(exclude_event_id)s" in kw
    # and it composes with the existing filters rather than replacing them
    both = vector_sql("acme", None, exclude_event_id="prov:inc:upd")
    assert "service = %(service)s" in both
    assert "event_id <> %(exclude_event_id)s" in both
    # absent by default, so every existing caller is unchanged
    assert "exclude_event_id" not in vector_sql(None, None)
    assert "exclude_event_id" not in keyword_sql(None, None)


def test_hybrid_search_binds_exclude_event_id():
    """The value must travel as a bound parameter, never interpolated."""
    from freshet.pipeline.embedding import StubEmbedder
    from freshet.rag.retrieval import hybrid_search

    seen = []

    class FakeConn:
        def execute(self, sql, params=None):
            seen.append((sql, params))

            class _Cur:
                def fetchall(self_inner):
                    return []

            return _Cur()

    hybrid_search(FakeConn(), StubEmbedder(), "q", k=5, exclude_event_id="prov:inc:upd")
    assert len(seen) == 2                       # both arms
    for sql, params in seen:
        assert "prov:inc:upd" not in sql        # not interpolated
        assert params["exclude_event_id"] == "prov:inc:upd"


def test_centered_sql_is_emitted_only_when_a_centroid_exists():
    """Row width is constant either way — the column is NULL when there is no
    centroid, so positional indices never shift under the caller."""
    from freshet.rag.retrieval import keyword_sql, vector_sql

    plain = vector_sql(None, None)
    assert "NULL::double precision AS centered_similarity" in plain
    assert "%(centroid)s" not in plain

    centered = vector_sql(None, None, centered=True)
    assert "(embedding - %(centroid)s::vector)" in centered
    assert "(%(qvec)s::vector - %(centroid)s::vector)" in centered
    # ranking still uses RAW cosine; only the abstention signal is centered
    assert "ORDER BY embedding <=> %(qvec)s::vector, chunk_id" in centered

    kw = keyword_sql(None, None, centered=True)
    assert "(embedding - %(centroid)s::vector)" in kw
    assert "ORDER BY rank DESC, chunk_id" in kw


def test_abstention_prefers_the_centered_signal():
    """With a centroid present, the floor is the centered one. The raw
    similarity here (0.81) clears the raw bge floor while the centered value
    (0.31) does not — which is the whole point: 12.2% of UNRELATED live chunk
    pairs clear 0.70 in raw space."""
    from freshet.pipeline import index_stats
    from freshet.pipeline.embedding import StubEmbedder
    from freshet.rag.retrieval import hybrid_search

    index_stats.clear_cache()
    now = datetime.now(UTC)
    # ..., title, similarity, centered_similarity
    vec_rows = [("chk_e1_0", "e1", "auth", now, now, "alert", "5xx spike",
                 "alert_fired", "Error spike", 0.81, 0.31)]

    class FakeConn:
        def execute(self, sql, params=None):
            rows = [] if "ts_rank" in sql else vec_rows

            class _Cur:
                def fetchall(self_inner):
                    return rows

                def fetchone(self_inner):
                    return ("[0.1,0.2]",)          # a stored centroid

            return _Cur()

    class Bgeish(StubEmbedder):
        name = "bge-ish"
        min_similarity = 0.70
        min_similarity_centered = 0.44

    r = hybrid_search(FakeConn(), Bgeish(), "q", k=5)
    assert r.abstained is True                     # 0.31 < 0.44
    assert r.hits[0].centered_similarity == 0.31
    assert r.hits[0].similarity == 0.81            # raw is still reported


def test_abstention_falls_back_to_raw_without_a_centroid():
    from freshet.pipeline import index_stats
    from freshet.pipeline.embedding import StubEmbedder
    from freshet.rag.retrieval import hybrid_search

    index_stats.clear_cache()
    now = datetime.now(UTC)
    vec_rows = [("chk_e1_0", "e1", "auth", now, now, "alert", "5xx spike",
                 "alert_fired", "Error spike", 0.81, None)]

    class FakeConn:
        def execute(self, sql, params=None):
            rows = [] if "ts_rank" in sql else vec_rows

            class _Cur:
                def fetchall(self_inner):
                    return rows

                def fetchone(self_inner):
                    return None                    # index_stats has no row

            return _Cur()

    class Bgeish(StubEmbedder):
        name = "bge-ish"
        min_similarity = 0.70
        min_similarity_centered = 0.44

    r = hybrid_search(FakeConn(), Bgeish(), "q", k=5)
    assert r.abstained is False                    # 0.81 >= the raw floor 0.70
    assert r.hits[0].centered_similarity is None
