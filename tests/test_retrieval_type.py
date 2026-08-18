from datetime import UTC, datetime

from freshet.api.retrieval import RetrievedHit
from freshet.pipeline.embedding import EMBEDDING_DIM


def test_retrieved_hit_has_type():
    h = RetrievedHit(chunk_id="c", event_id="e", service="s",
                     ts=datetime(2026, 6, 6, tzinfo=UTC),
                     indexed_at=datetime(2026, 6, 6, tzinfo=UTC),
                     source="deploy", text="t", type="rollback",
                     similarity=0.1, score=0.2)
    assert h.type == "rollback"


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
    def execute(self, *_args, **_kw):
        rows = self._rows
        class _R:
            def fetchall(self_inner):
                return rows
        return _R()


class _FakeEmbedder:
    def encode(self, texts):
        return [[0.0] * EMBEDDING_DIM for _ in texts]
    def encode_query(self, texts):
        return self.encode(texts)



def _rows():
    ts = datetime(2026, 6, 6, 12, tzinfo=UTC)
    # chunk_id, event_id, service, ts, indexed_at, source, text, type, score
    return [
        ("c1", "e1", "s", ts, ts, "deploy", "deploy started", "deploy_started", 0.9),
        ("c2", "e2", "s", ts, ts, "deploy", "rolling back", "rollback", 0.4),
    ]


