import re
from datetime import datetime, timedelta, timezone

from freshet.api.retrieval import RetrievedHit
from freshet.api.synthesis import build_timeline

CITE = re.compile(r"\[[\w.\-]+ @ [^\]]+\]")
T0 = datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc)


def _hit(eid, off, source, type_, text, incident="INC-1"):
    ts = T0 + timedelta(seconds=off)
    return RetrievedHit(chunk_id=eid, event_id=eid, service="scheduler-api",
                        ts=ts, indexed_at=ts, source=source, text=text, type=type_,
                        similarity=0.5, score=1.0 - off / 1000)


def _incident_hits():
    return [
        _hit("d1", 0, "deploy", "deploy_started", "Deploy v2 started"),
        _hit("sp", 90, "alert", "error_spike", "5xx crossed 5%"),
        _hit("rb", 240, "deploy", "rollback", "Rolling back to v1"),
        _hit("pm", 3600, "postmortem", "rca", "Postmortem: pool regression"),
    ]


def test_timeline_orders_and_identifies_cause_and_fix():
    tl = build_timeline(_incident_hits())
    assert [e.hit.event_id for e in tl.entries] == ["d1", "sp", "rb", "pm"]
    assert tl.cause.event_id == "d1"
    assert tl.fix.event_id == "rb"
    assert tl.service == "scheduler-api"


def test_render_is_cited():
    out = build_timeline(_incident_hits()).render()
    content = [ln for ln in out.splitlines() if ln.startswith("- ")]
    assert content and all(CITE.search(ln) for ln in content)
    assert "Cause" in out and "Resolution" in out


def test_empty_hits_abstain():
    out = build_timeline([]).render()
    assert "insufficient evidence" in out.lower()


def test_timeline_identifies_non_deploy_cause_and_fix():
    # a cert-expiry incident: cause is cert_expired, fix is cert_renewed
    hits = [
        _hit("c1", 0, "alert", "cert_expired", "TLS cert for api expired"),
        _hit("sp", 90, "alert", "error_spike", "5xx crossed 5%"),
        _hit("rn", 240, "deploy", "cert_renewed", "Renewed the TLS cert"),
        _hit("pm", 3600, "postmortem", "rca", "Postmortem: cert expiry"),
    ]
    tl = build_timeline(hits)
    assert tl.cause.event_id == "c1"
    assert tl.fix.event_id == "rn"
