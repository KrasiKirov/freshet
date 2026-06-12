import json

from freshet.pipeline.deadletter import DEADLETTER_TOPIC, build_deadletter


def test_envelope_preserves_payload_and_context():
    out = json.loads(build_deadletter("validation failed", '{"bad": true}', "raw.events"))
    assert out["error"] == "validation failed"
    assert out["payload"] == '{"bad": true}'
    assert out["source_topic"] == "raw.events"
    assert "dead_lettered_at" in out
    assert DEADLETTER_TOPIC == "deadletter.events"
