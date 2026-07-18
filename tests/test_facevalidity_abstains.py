from datetime import UTC, datetime

from freshet.api.retrieval import RetrievedHit
from freshet.api.synthesis import build_timeline


def _hit(eid, ts_min, type_, text):
    ts = datetime(2026, 7, 1, 12, ts_min, 0, tzinfo=UTC)
    return RetrievedHit(chunk_id=eid + "#0", event_id=eid, service="acme", ts=ts,
                        indexed_at=ts, source="status", text=text, type=type_,
                        similarity=1.0, score=1.0)


def test_symptom_only_incident_abstains():
    """Status-feed incidents carry only status updates (no change events), so the
    cause selector must abstain rather than fabricate a root cause."""
    hits = [
        _hit("u1", 0, "investigating", "acme: investigating elevated error rates"),
        _hit("u2", 8, "identified", "acme: issue identified with a downstream provider"),
        _hit("u3", 30, "resolved", "acme: the incident has been resolved"),
    ]
    tl = build_timeline(hits)
    assert tl.cause is None
