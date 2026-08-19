"""The composer must tell the model what "now" is.

Regression: asked "what incidents happened today?", the model replied that it
could not determine what "today" meant — correctly, because the prompt carried
only event timestamps and no anchor. Temporal questions are the primary use case
for a freshness-first system, so this is load-bearing.
"""
from datetime import UTC, datetime

from freshet.rag.composer import AnthropicComposer
from freshet.rag.retrieval import RetrievedHit


class _FakeMessages:
    def __init__(self):
        self.kwargs = None

    def create(self, **kw):
        self.kwargs = kw

        class _B:
            type = "text"
            text = "no relevant events"
        return type("R", (), {"content": [_B()]})()


class _FakeClient:
    def __init__(self):
        self.messages = _FakeMessages()


def _hit():
    # compose() short-circuits on empty hits, so a real one is required to reach
    # the API call this test inspects.
    now = datetime.now(UTC)
    return RetrievedHit(chunk_id="c1", event_id="svc:1:a", service="svc", ts=now,
                        indexed_at=now, source="alert", text="Elevated errors",
                        type="status_update", similarity=0.9, score=0.5)


def _compose():
    c = AnthropicComposer.__new__(AnthropicComposer)
    c._client, c._model = _FakeClient(), "test-model"
    c.compose("what happened today?", [_hit()])
    assert c._client.messages.kwargs is not None, "the API was never called"
    return c._client.messages.kwargs


def test_the_user_turn_carries_the_current_date():
    sent = _compose()["messages"][0]["content"]
    assert "Current time:" in sent
    assert datetime.now(UTC).strftime("%Y-%m-%d") in sent
    # the anchor must precede the question, not trail the event dump
    assert sent.index("Current time:") < sent.index("Question:")


def test_the_system_prompt_tells_the_model_to_use_it():
    system = _compose()["system"]
    assert "Current time" in system and "today" in system


def test_the_system_prompt_stays_static_for_caching():
    # The timestamp belongs on the user turn; a date in the system prompt would
    # bust the prompt cache on every call.
    assert datetime.now(UTC).strftime("%Y-%m-%d") not in _compose()["system"]

