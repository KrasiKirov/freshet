from datetime import UTC, datetime, timedelta

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
    t0 = datetime(2026, 6, 6, 8, 0, 0, tzinfo=UTC)
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
        ts=datetime.now(UTC),
        text="chunk",
        source=EventSource.POSTMORTEM,
    )
    assert vr.chunk_id.startswith("chk_")


_MINIMAL = ('{"event_id":"github:INC1:u1","ts":"2026-08-18T11:42:00Z","service":"github",'
            '"source":"alert","type":"status_update","text":"done"}')


def test_an_event_from_an_unversioned_message_defaults_to_v1():
    """Messages produced before the field existed are still on the topic; a replay
    of retained history must not poison-pill the consumer on its first message."""
    assert Event.model_validate_json(_MINIMAL).v == 1


def test_a_future_wire_version_survives_parsing_so_it_can_be_reported():
    """Rejecting it outright would dead-letter a whole producer rollout. The fields
    this worker understands are still valid; the version is carried so the
    embedder can count it and say so."""
    ev = Event.model_validate_json('{"v":99,' + _MINIMAL[1:])
    assert ev.v == 99
