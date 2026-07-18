import types
from datetime import UTC, datetime, timedelta

from freshet.api.retrieval import RetrievedHit
from freshet.api.synthesis import build_timeline, synthesize_narrative

T0 = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)


def _hit(eid, off, source, type_, text):
    ts = T0 + timedelta(seconds=off)
    return RetrievedHit(chunk_id=eid, event_id=eid, service="scheduler-api",
                        ts=ts, indexed_at=ts, source=source, text=text, type=type_,
                        similarity=0.5, score=1.0 - off / 1000)


def _timeline():
    return build_timeline([
        _hit("d1", 0, "deploy", "deploy_started", "Deploy v2 started"),
        _hit("sp", 90, "alert", "error_spike", "5xx crossed 5%"),
        _hit("rb", 240, "deploy", "rollback", "Rolling back to v1"),
    ])


class _FakeClient:
    """Mimics anthropic.Anthropic().messages.create(...) -> resp.content[*].text."""
    def __init__(self, text):
        self._text = text
        self.messages = self
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        block = types.SimpleNamespace(type="text", text=self._text)
        return types.SimpleNamespace(content=[block])


def test_narrative_uses_injected_client_and_evidence():
    fake = _FakeClient("v2 caused a 5xx spike; rolling back to v1 resolved it.")
    out = synthesize_narrative(_timeline(), client=fake)
    assert "rolling back" in out.lower()
    sent = str(fake.calls[0]["messages"])
    assert "d1" in sent and "rb" in sent


def test_empty_timeline_abstains_without_a_client():
    out = synthesize_narrative(build_timeline([]))
    assert "insufficient evidence" in out.lower()
