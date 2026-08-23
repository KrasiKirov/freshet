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


def test_the_contract_does_not_document_a_vocabulary_no_producer_writes():
    """CHANGE_TYPES, REMEDIATION_TYPES and the synthetic EventType members were v1's
    generator vocabulary. Nothing in this pipeline emits them — the Flink projection
    hardcodes source='alert' and type='status_update' — and nothing outside
    schemas.py referenced them. A contract that documents fields nobody writes
    misleads whoever reads it next."""
    import freshet.common.schemas as schemas

    assert not hasattr(schemas, "CHANGE_TYPES")
    assert not hasattr(schemas, "REMEDIATION_TYPES")
    assert {m.value for m in schemas.EventType} == {"status_update", "rca"}


def test_type_stays_an_open_vocabulary_string():
    """Trimming the enum must not start rejecting a type it no longer names: the
    field is deliberately `str`, and messages carrying older types are still on
    the topic."""
    ev = Event(service="s", source=EventSource.ALERT, type="deploy_started")
    assert ev.type == "deploy_started"
