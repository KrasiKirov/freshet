from datetime import datetime, timedelta, timezone

from freshet.common.schemas import Event, EventSource, Severity, VectorRecord


def test_event_defaults_and_ids():
    e = Event(service="scheduler-api", source=EventSource.ALERT, type="error_spike")
    assert e.event_id.startswith("evt_")
    assert e.severity is None
    assert e.structured == {} and e.refs == []
    # not yet ingested/indexed -> freshness undefined
    assert e.end_to_end_latency_s() is None
    assert e.pipeline_latency_s() is None


def test_event_json_round_trip():
    e = Event(
        service="task-queue",
        source=EventSource.DEPLOY,
        type="rollback",
        severity=Severity.SEV2,
        text="rolling back",
        structured={"to": "v1"},
        refs=["evt_abc"],
    )
    restored = Event.model_validate_json(e.model_dump_json())
    assert restored == e
    assert restored.source is EventSource.DEPLOY
    assert restored.severity is Severity.SEV2


def test_freshness_math():
    t0 = datetime(2026, 6, 6, 8, 0, 0, tzinfo=timezone.utc)
    e = Event(
        service="s",
        source=EventSource.METRIC,
        type="metric_sample",
        ts=t0,
        ingested_at=t0 + timedelta(seconds=1.0),
        indexed_at=t0 + timedelta(seconds=2.5),
    )
    assert e.end_to_end_latency_s() == 2.5
    assert e.pipeline_latency_s() == 1.5


def test_vector_record_requires_core_fields():
    vr = VectorRecord(
        event_id="evt_1",
        service="s",
        ts=datetime.now(timezone.utc),
        text="chunk",
        source=EventSource.POSTMORTEM,
    )
    assert vr.chunk_id.startswith("chk_")
