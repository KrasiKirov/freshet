from datetime import datetime, timezone

from freshet.api.retrieval import RetrievedHit
from freshet.api.synthesis import build_timeline


def _hit(eid, ts_min, type_, text):
    ts = datetime(2026, 7, 1, 12, ts_min, 0, tzinfo=timezone.utc)
    return RetrievedHit(chunk_id=eid + "#0", event_id=eid, service="api", ts=ts,
                        indexed_at=ts, source="deploy", text=text, type=type_,
                        similarity=1.0, score=1.0)


def test_commit_before_spike_is_cause():
    hits = [
        _hit("c1", 0, "commit", "commit abc1234: bump pool size (by alice)"),
        _hit("s1", 5, "error_spike", "5xx on api crossed 5% (now 20%)"),
    ]
    tl = build_timeline(hits)
    assert tl.cause is not None and tl.cause.event_id == "c1"
    assert "abc1234" in tl.cause.text
