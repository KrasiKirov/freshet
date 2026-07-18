import types
from datetime import UTC, datetime

import pytest

from freshet.api.retrieval import RetrievedHit
from freshet.eval import judge


def _hit(eid, text):
    ts = datetime(2026, 6, 6, 12, tzinfo=UTC)
    return RetrievedHit(chunk_id=eid, event_id=eid, service="s", ts=ts, indexed_at=ts,
                        source="deploy", text=text, type="rollback", similarity=0.5, score=1.0)


class _FakeClient:
    def __init__(self, text):
        self._text = text
        self.messages = self
    def create(self, **kwargs):
        block = types.SimpleNamespace(type="text", text=self._text)
        return types.SimpleNamespace(content=[block])


def test_parse_score_reads_and_clamps():
    assert judge._parse_score("0.8") == 0.8
    assert judge._parse_score("Score: 1") == 1.0
    assert judge._parse_score("12") == 1.0
    with pytest.raises(ValueError):
        judge._parse_score("no number here")


def test_faithfulness_uses_client_and_evidence():
    fake = _FakeClient("0.5")
    score = judge.judge_faithfulness("some answer", [_hit("rb", "Rolling back")], client=fake)
    assert score == 0.5


def test_answer_relevance_uses_client():
    fake = _FakeClient("0.9")
    assert judge.judge_answer_relevance("an answer", "a question?", client=fake) == 0.9
