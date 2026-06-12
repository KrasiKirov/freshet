from datetime import datetime, timezone

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import to_vector_record


def test_to_vector_record_has_deterministic_chunk_id():
    ev = Event(
        service="scheduler-api",
        source=EventSource.ALERT,
        type="error_spike",
        text="5xx spike",
        incident_id="INC-1",
    )
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    a = to_vector_record(ev, now=now)
    b = to_vector_record(ev, now=now)
    # reprocessing the same event must target the same row (idempotent upsert)
    assert a.chunk_id == b.chunk_id == f"chk_{ev.event_id}_0"


def test_to_vector_record_copies_fields_and_stamps_indexed_at():
    ev = Event(service="s", source=EventSource.CHAT, type="message", text="hello")
    now = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
    rec = to_vector_record(ev, now=now)
    assert rec.event_id == ev.event_id
    assert rec.service == "s"
    assert rec.ts == ev.ts
    assert rec.indexed_at == now
    assert rec.text == "hello"
    assert rec.source is EventSource.CHAT
    assert rec.incident_id is None
