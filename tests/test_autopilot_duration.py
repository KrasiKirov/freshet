"""Pure unit tests for the postmortem duration formatter (keyless, no DB)."""
from datetime import UTC, datetime, timedelta

from freshet.autopilot.investigate import _format_duration


def _span(**kw):
    start = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)
    return start, start + timedelta(**kw)


def test_seconds():
    assert _format_duration(*_span(seconds=42)) == "42s"


def test_minutes():
    assert _format_duration(*_span(minutes=42)) == "42m"


def test_hours_and_minutes():
    assert _format_duration(*_span(hours=3, minutes=5)) == "3h 5m"


def test_missing_timestamp_is_none():
    start = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)
    assert _format_duration(None, start) is None
    assert _format_duration(start, None) is None
