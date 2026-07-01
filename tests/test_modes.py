from datetime import datetime, timezone

from freshet.eval.modes import keyword_only_event_ids, vector_only_event_ids
from freshet.pipeline.embedding import StubEmbedder


def _rows(*ids):
    now = datetime.now(timezone.utc)
    # (chunk_id, event_id, service, ts, indexed_at, source, text, score)
    return [(f"chk_{e}_0", e, "s", now, now, "alert", "t", 0.9 - i * 0.1)
            for i, e in enumerate(ids)]


class FakeConn:
    def __init__(self, vec, kw):
        self.vec, self.kw = vec, kw

    def execute(self, sql, params=None):
        rows = self.vec if "embedding <=>" in sql else self.kw

        class _Cur:
            def fetchall(self_inner):
                return rows

        return _Cur()


def test_vector_only_returns_ranked_unique_event_ids():
    conn = FakeConn(_rows("e1", "e2", "e1", "e3"), [])
    got = vector_only_event_ids(conn, StubEmbedder(), "q", k=3)
    assert got == ["e1", "e2", "e3"]   # dedup keeps first occurrence, truncates to k


def test_keyword_only_uses_keyword_arm():
    conn = FakeConn([], _rows("e9", "e8"))
    got = keyword_only_event_ids(conn, "q", k=5)
    assert got == ["e9", "e8"]
