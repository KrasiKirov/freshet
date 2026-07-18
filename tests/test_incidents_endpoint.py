"""/incidents returns recent ingested incidents grouped by incident_id (keyless,
dependency-overridden conn — no DB)."""
from datetime import UTC, datetime

from fastapi.testclient import TestClient


def test_incidents_endpoint_shape():
    from freshet.api import app as appmod

    ts = datetime(2026, 6, 29, 10, 40, tzinfo=UTC)

    class _Cur:
        def fetchall(self):
            return [("cloudflare:inc_100", "cloudflare", ts, ts,
                     "Elevated 5xx errors in WAF: Rollback complete.", "resolved", "SEV2")]

    class _Conn:
        def execute(self, *a, **k):
            return _Cur()

    appmod.app.dependency_overrides[appmod.get_deps] = lambda: (_Conn(), None, None)
    try:
        r = TestClient(appmod.app).get("/incidents")
    finally:
        appmod.app.dependency_overrides.clear()

    assert r.status_code == 200
    body = r.json()
    assert body[0]["service"] == "cloudflare"
    assert body[0]["status"] == "resolved"
    assert body[0]["severity"] == "SEV2"
    assert body[0]["incident_id"] == "cloudflare:inc_100"
