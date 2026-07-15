from datetime import datetime, timezone

from freshet.api.composer import TemplateComposer, make_composer
from freshet.api.retrieval import RetrievedHit


def _hit(event_id="e1", text="5xx error spike on scheduler-api") -> RetrievedHit:
    now = datetime(2026, 6, 13, 9, 30, 0, tzinfo=timezone.utc)
    return RetrievedHit(
        chunk_id=f"chk_{event_id}_0", event_id=event_id, service="scheduler-api",
        ts=now, indexed_at=now, source="alert", text=text, type="alert_fired",
        similarity=0.8, score=0.9,
    )


def test_template_composer_cites_events():
    out = TemplateComposer().compose("what is wrong?", [_hit()])
    assert "e1" in out
    assert "09:30" in out
    assert "5xx error spike" in out


def test_template_composer_handles_no_hits():
    out = TemplateComposer().compose("anything?", [])
    assert "don't have" in out.lower() or "no " in out.lower()


def test_make_composer_explicit_template():
    assert isinstance(make_composer("template"), TemplateComposer)


def test_make_composer_auto_without_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert isinstance(make_composer("auto"), TemplateComposer)
