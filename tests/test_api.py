from datetime import datetime, timezone

from fastapi.testclient import TestClient

from freshet.api.app import app, get_deps, topk_sql
from freshet.pipeline.embedding import StubEmbedder


def test_topk_sql_filters():
    now = datetime.now(timezone.utc)
    base = topk_sql(None, None)
    assert "WHERE" not in base
    assert "ORDER BY embedding <=>" in base
    assert "service = %(service)s" in topk_sql("scheduler-api", None)
    assert "ts >= %(since)s" in topk_sql(None, now)
    both = topk_sql("s", now)
    assert "service = %(service)s" in both and "ts >= %(since)s" in both


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        return FakeCursor(self.rows)


def test_query_endpoint_returns_scored_hits():
    now = datetime.now(timezone.utc)
    rows = [("chk_evt1_0", "evt1", "scheduler-api", now, now, "alert", "5xx spike", 0.93)]
    fake = FakeConn(rows)
    app.dependency_overrides[get_deps] = lambda: (fake, StubEmbedder())
    try:
        client = TestClient(app)
        resp = client.post("/query", json={"question": "what is wrong?", "k": 3})
    finally:
        app.dependency_overrides.clear()
    assert resp.status_code == 200
    hits = resp.json()["hits"]
    assert len(hits) == 1
    assert hits[0]["event_id"] == "evt1"
    assert hits[0]["score"] == 0.93
    # the SQL actually ran with the embedded question vector
    sql, params = fake.queries[0]
    assert params["k"] == 3
    assert params["qvec"].startswith("[")
