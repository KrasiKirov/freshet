"""The centroid module, exercised against a fake connection.

The real SQL is covered by tests/integration/test_centered_abstention.py; these
tests pin the contract the query path depends on — that a missing centroid is a
None rather than an exception, and that the read is cached.
"""


class FakeConn:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))
        row = self.rows.pop(0) if self.rows else None

        class _Cur:
            def fetchone(self_inner):
                return row

        return _Cur()


def test_compute_centroid_returns_literal():
    from freshet.pipeline.index_stats import compute_centroid

    conn = FakeConn([("[0.1,0.2]", 7)])
    assert compute_centroid(conn, "bge") == "[0.1,0.2]"
    assert conn.sql[0][1] == ("bge",)


def test_compute_centroid_is_none_for_an_empty_model():
    """An empty index must not raise on the query path — abstention falls back
    to the raw floor."""
    from freshet.pipeline.index_stats import compute_centroid

    assert compute_centroid(FakeConn([(None, 0)]), "bge") is None
    assert compute_centroid(FakeConn([None]), "bge") is None


def test_get_centroid_caches_within_the_ttl():
    from freshet.pipeline import index_stats

    index_stats.clear_cache()
    conn = FakeConn([("[0.1,0.2]",), ("[9.9,9.9]",)])
    clock = iter([100.0, 100.0 + index_stats.CENTROID_TTL_S / 2,
                  100.0 + index_stats.CENTROID_TTL_S * 2])
    assert index_stats.get_centroid(conn, "bge", now=lambda: next(clock)) == "[0.1,0.2]"
    assert index_stats.get_centroid(conn, "bge", now=lambda: next(clock)) == "[0.1,0.2]"
    assert len(conn.sql) == 1                     # served from cache
    assert index_stats.get_centroid(conn, "bge", now=lambda: next(clock)) == "[9.9,9.9]"
    assert len(conn.sql) == 2                     # TTL expired, re-read


def test_get_centroid_is_none_for_a_blank_model_name():
    """StubEmbedder-style embedders with no provenance get the raw floor."""
    from freshet.pipeline import index_stats

    index_stats.clear_cache()
    conn = FakeConn([])
    assert index_stats.get_centroid(conn, "", now=lambda: 0.0) is None
    assert conn.sql == []                         # no query issued at all
