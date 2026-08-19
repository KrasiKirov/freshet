"""Dead-lettered messages must be replayable, and unusable ones left alone."""
import json

from freshet.pipeline.replay_deadletter import replay_record


class _Producer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, payload, key=None):
        self.sent.append((topic, payload))


def test_a_record_is_republished_to_its_own_source_topic():
    p = _Producer()
    raw = json.dumps({"source_topic": "normalized.updates", "payload": '{"event_id":"e1"}',
                      "error": "boom"})
    assert replay_record(p, raw) == "normalized.updates"
    assert p.sent == [("normalized.updates", '{"event_id":"e1"}')]


def test_a_record_without_a_source_topic_is_quarantined():
    p = _Producer()
    assert replay_record(p, json.dumps({"payload": "{}"})) is None
    assert p.sent[0][0] == "deadletter.unusable"


def test_a_non_json_record_is_quarantined_instead_of_dropped():
    """One unreadable record must not stop the rest of the queue draining."""
    p = _Producer()
    assert replay_record(p, "{not json") is None
    assert p.sent[0][0] == "deadletter.unusable"


def test_an_empty_payload_is_distinguished_from_a_missing_one():
    p = _Producer()
    assert replay_record(p, json.dumps({"source_topic": "t", "payload": ""})) == "t"
    assert p.sent == [("t", "")]


def test_an_unusable_record_is_quarantined_not_committed_into_oblivion():
    """Returning None used to commit past a broken envelope, losing it."""
    p = _Producer()
    topic = replay_record(p, "{not json")
    assert topic is None
    assert p.sent and p.sent[0][0] == "deadletter.unusable"
