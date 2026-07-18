"""Unit tests for investigate() using a scripted fake Anthropic client."""
import json
from datetime import UTC, datetime
from types import SimpleNamespace


def _block(**kwargs):
    return SimpleNamespace(**kwargs)


def _text_block(text: str):
    return _block(type="text", text=text)


def _tool_use_block(name: str, inp: dict, id: str = "tu_001"):
    return _block(type="tool_use", name=name, input=inp, id=id)


def _response(blocks):
    r = SimpleNamespace()
    r.content = blocks
    r.stop_reason = "tool_use" if any(b.type == "tool_use" for b in blocks) else "end_turn"
    return r


class _FakeMessages:
    def __init__(self, responses):
        self._it = iter(responses)

    def create(self, **kwargs):
        return next(self._it)


class FakeClient:
    def __init__(self, responses):
        self.messages = _FakeMessages(responses)


def _hit(event_id: str, type_: str = "deploy", text: str = "event"):
    return SimpleNamespace(
        event_id=event_id,
        ts=datetime(2026, 6, 6, 8, 0, 0, tzinfo=UTC),
        type=type_,
        text=text,
    )


# ── Tests ──────────────────────────────────────────────────────────────────


def test_investigate_normal_flow():
    """search → events_around → submit_findings returns correct Investigation."""
    from unittest.mock import MagicMock, patch

    from freshet.api.agent import investigate

    cause_id, fix_id = "evt_cause_001", "evt_fix_001"
    responses = [
        _response([_tool_use_block("search", {"query": "error spike"}, "tu_1")]),
        _response([_tool_use_block("get_events_around", {"service": "billing-api-00", "timestamp": "2026-06-06T08:00:00+00:00"}, "tu_2")]),
        _response([_tool_use_block("submit_findings", {"cause_id": cause_id, "fix_id": fix_id, "narrative": "deploy v2 caused 5xx"}, "tu_3")]),
    ]

    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search") as mock_hs, \
         patch("freshet.api.agent.events_around") as mock_ea:
        mock_hs.return_value = MagicMock(hits=[_hit(cause_id, "deploy"), _hit(fix_id, "rollback")])
        mock_ea.return_value = []
        inv = investigate(mock_conn, mock_embedder, "billing-api-00",
                          client=FakeClient(responses))

    assert inv.cause_id == cause_id
    assert inv.fix_id == fix_id
    assert "deploy" in inv.narrative
    assert inv.steps == 3
    assert any(e["role"] == "tool_call" for e in inv.transcript)


def test_investigate_max_steps_without_submit():
    """Loop bounded by max_steps; returns partial Investigation."""
    from unittest.mock import MagicMock, patch

    from freshet.api.agent import investigate

    responses = [
        _response([_tool_use_block("search", {"query": f"q{i}"}, f"tu_{i}")])
        for i in range(10)
    ]
    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search") as mock_hs, \
         patch("freshet.api.agent.events_around"):
        mock_hs.return_value = MagicMock(hits=[])
        inv = investigate(mock_conn, mock_embedder, "billing-api-00",
                          max_steps=3, client=FakeClient(responses))

    assert inv.cause_id is None
    assert inv.fix_id is None
    assert inv.steps <= 3
    assert "incomplete" in inv.narrative.lower()


def test_investigate_drops_unseen_cited_ids():
    """submit_findings citing IDs not in any tool result → those IDs dropped."""
    from unittest.mock import MagicMock, patch

    from freshet.api.agent import investigate

    responses = [
        _response([_tool_use_block("submit_findings", {
            "cause_id": "evt_hallucinated",
            "fix_id": "evt_also_hallucinated",
            "narrative": "made up",
        }, "tu_1")]),
    ]
    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search"), patch("freshet.api.agent.events_around"):
        inv = investigate(mock_conn, mock_embedder, "billing-api-00",
                          client=FakeClient(responses))

    assert inv.cause_id is None
    assert inv.fix_id is None
    assert inv.narrative == "made up"


def test_seen_ids_extracts_from_list_results():
    from freshet.api.agent import _seen_ids_from

    results = [
        json.dumps([{"event_id": "a"}, {"event_id": "b"}]),
        json.dumps([{"event_id": "c"}]),
        json.dumps([]),
    ]
    assert _seen_ids_from(results) == {"a", "b", "c"}


def test_seen_ids_handles_invalid_json():
    from freshet.api.agent import _seen_ids_from

    assert _seen_ids_from(["not-json", "null", ""]) == set()


def test_investigate_transcript_records_submit():
    """Transcript includes a submit_findings entry with the final ids."""
    from unittest.mock import MagicMock, patch

    from freshet.api.agent import investigate

    cause_id = "evt_x"
    responses = [
        _response([_tool_use_block("search", {"query": "spike"}, "tu_1")]),
        _response([_tool_use_block("submit_findings", {"cause_id": cause_id, "narrative": "found it"}, "tu_2")]),
    ]
    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search") as mock_hs, \
         patch("freshet.api.agent.events_around"):
        mock_hs.return_value = MagicMock(hits=[_hit(cause_id)])
        inv = investigate(mock_conn, mock_embedder, "billing-api-00",
                          client=FakeClient(responses))

    assert inv.cause_id == cause_id
    submit_entries = [e for e in inv.transcript if e["role"] == "submit_findings"]
    assert len(submit_entries) == 1
    assert submit_entries[0]["cause_id"] == cause_id
