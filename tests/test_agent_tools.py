"""Unit tests for agent.py tool schemas, base_service(), and dispatch routing."""
import json
from datetime import UTC
from unittest.mock import MagicMock, patch


def test_tool_schemas_names():
    from freshet.api.agent import SUBMIT_SCHEMA, TOOL_SCHEMAS

    names = {s["name"] for s in TOOL_SCHEMAS}
    assert names == {"search", "get_events_around", "get_runbook"}
    assert SUBMIT_SCHEMA["name"] == "submit_findings"


def test_tool_schemas_have_required_fields():
    from freshet.api.agent import TOOL_SCHEMAS

    for schema in TOOL_SCHEMAS:
        assert "name" in schema
        assert "description" in schema
        assert "input_schema" in schema
        assert schema["input_schema"].get("properties"), f"{schema['name']} has no properties"


def test_base_service_strips_numeric_suffix():
    from freshet.api.agent import base_service

    assert base_service("scheduler-api-00") == "scheduler-api"
    assert base_service("billing-api-07") == "billing-api"
    assert base_service("scheduler-api") == "scheduler-api"


def test_dispatch_search_routes_to_hybrid_search():
    from freshet.api.agent import make_dispatch

    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search") as mock_hs:
        mock_hs.return_value = MagicMock(hits=[])
        dispatch = make_dispatch(mock_conn, mock_embedder)
        result = dispatch("search", {"query": "error spike", "k": 5})
    mock_hs.assert_called_once()
    assert json.loads(result) == []


def test_dispatch_search_applies_default_since_when_model_omits_it():
    from datetime import datetime

    from freshet.api.agent import make_dispatch

    bound = datetime(2026, 6, 6, 6, 0, 0, tzinfo=UTC)
    with patch("freshet.api.agent.hybrid_search") as mock_hs:
        mock_hs.return_value = MagicMock(hits=[])
        dispatch = make_dispatch(MagicMock(), MagicMock(), default_since=bound)
        dispatch("search", {"query": "error spike"})           # no since from the model
        assert mock_hs.call_args.kwargs["since"] == bound
        dispatch("search", {"query": "error spike",
                            "since": "2026-06-06T08:00:00+00:00"})  # explicit wins
        assert mock_hs.call_args.kwargs["since"].hour == 8


def test_dispatch_get_events_around_routes():
    from freshet.api.agent import make_dispatch

    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.events_around") as mock_ea:
        mock_ea.return_value = []
        dispatch = make_dispatch(mock_conn, mock_embedder)
        result = dispatch(
            "get_events_around",
            {"service": "billing-api-00", "timestamp": "2026-06-06T08:00:00+00:00"},
        )
    mock_ea.assert_called_once()
    assert json.loads(result) == []


def test_dispatch_get_runbook_strips_suffix_and_queries_db():
    from freshet.api.agent import make_dispatch

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = [
        ("billing-api runbook: on elevated 5xx check recent deploys.",)
    ]
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search"), patch("freshet.api.agent.events_around"):
        dispatch = make_dispatch(mock_conn, mock_embedder)
        result = dispatch("get_runbook", {"service": "billing-api-07"})
    data = json.loads(result)
    assert data["service"] == "billing-api"
    assert "billing-api runbook" in data["runbook"]
    # Verify base_service was applied (query used "billing-api" not "billing-api-07")
    call_args = mock_conn.execute.call_args
    assert "billing-api" in str(call_args)
    assert "billing-api-07" not in str(call_args)


def test_dispatch_unknown_tool_returns_error():
    from freshet.api.agent import make_dispatch

    mock_conn = MagicMock()
    mock_embedder = MagicMock()
    with patch("freshet.api.agent.hybrid_search"), patch("freshet.api.agent.events_around"):
        dispatch = make_dispatch(mock_conn, mock_embedder)
        result = dispatch("nonexistent_tool", {})
    data = json.loads(result)
    assert "error" in data
