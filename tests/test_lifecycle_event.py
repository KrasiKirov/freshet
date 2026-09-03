from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC, LifecycleEvent


def test_topic_name():
    assert LIFECYCLE_TOPIC == "incident.lifecycle"


def test_roundtrip():
    ev = LifecycleEvent(type="opened", incident_id="INC_1", service="scheduler-api",
                        ts="2026-07-01T12:00:00+00:00")
    back = LifecycleEvent.from_json(ev.to_json())
    assert back == ev


def test_from_json_reads_fields():
    raw = '{"type":"resolved","incident_id":"INC_2","service":"api","ts":"2026-07-01T00:00:00+00:00"}'
    ev = LifecycleEvent.from_json(raw)
    assert ev.type == "resolved" and ev.incident_id == "INC_2" and ev.service == "api"


def test_a_lifecycle_event_is_a_validated_model():
    """Everything else in the contract layer is pydantic; this was a hand-rolled
    dataclass plus json, so a missing field surfaced as a bare KeyError."""
    from pydantic import BaseModel

    from freshet.pipeline.lifecycle import LifecycleEvent

    assert issubclass(LifecycleEvent, BaseModel)


def test_a_lifecycle_event_missing_a_required_field_is_rejected_clearly():
    import pytest
    from pydantic import ValidationError

    from freshet.pipeline.lifecycle import LifecycleEvent

    with pytest.raises(ValidationError):
        LifecycleEvent.from_json('{"type": "opened", "ts": "2026-08-22T10:00:00Z"}')


def test_an_unknown_lifecycle_type_still_parses():
    """The consumer prints 'no action' for a type it does not handle. Rejecting
    unknown types would poison-pill a replay of retained history instead."""
    from freshet.pipeline.lifecycle import LifecycleEvent

    ev = LifecycleEvent.from_json(
        '{"type": "updated", "incident_id": "INC_1", "service": "api",'
        ' "ts": "2026-08-22T10:00:00Z"}')
    assert ev.type == "updated"
