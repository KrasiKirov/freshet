"""Composer contract. Generation is mandatory, so these tests inject a fake
client rather than setting a key — the LLM path is exercised, not skipped."""
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from freshet.rag.composer import NO_EVIDENCE, AnthropicComposer, make_composer


def _hit(eid="e1", text="5xx errors are elevated"):
    return SimpleNamespace(event_id=eid, source="alert", text=text,
                           ts=datetime(2026, 8, 18, 12, 0, tzinfo=UTC))


def _client(answer: str):
    class FakeClient:
        class messages:
            @staticmethod
            def create(**kw):
                FakeClient.last = kw
                return SimpleNamespace(
                    content=[SimpleNamespace(type="text", text=answer)])
    return FakeClient


def test_the_model_is_given_the_evidence_and_the_question():
    client = _client("Grounded answer [e1 @ 2026-08-18 12:00:00].")
    AnthropicComposer(client=client).compose("what is wrong?", [_hit()])
    sent = client.last["messages"][0]["content"]
    assert "what is wrong?" in sent
    assert "5xx errors are elevated" in sent, "the hit's text must reach the model"
    assert "e1" in sent, "the model must be given the id it is expected to cite"


def test_no_evidence_short_circuits_without_calling_the_model():
    class Exploding:
        class messages:
            @staticmethod
            def create(**kw):
                raise AssertionError("must not call the model with no evidence")
    assert AnthropicComposer(client=Exploding()).compose("q", []) == NO_EVIDENCE


def test_make_composer_fails_loudly_without_a_key(monkeypatch):
    """A silently extractive 'answer' is worse than a clear failure."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY is required"):
        make_composer()
