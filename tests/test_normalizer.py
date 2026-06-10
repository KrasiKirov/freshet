from datetime import datetime, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.normalizer import normalize


def test_normalize_stamps_ingested_at_and_preserves_event():
    ev = Event(service="scheduler-api", source=EventSource.ALERT, type="error_spike", text="boom")
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    out = normalize(ev.model_dump_json(), now=now)
    assert out is not None
    assert out.ingested_at == now
    assert out.event_id == ev.event_id
    assert out.ts == ev.ts
    assert out.text == "boom"


def test_normalize_rejects_invalid_payloads():
    assert normalize("not json at all") is None
    assert normalize('{"service": "s"}') is None  # missing required fields
