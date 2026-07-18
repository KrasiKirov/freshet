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
