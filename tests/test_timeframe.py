"""Window inference must fire on explicit temporal phrases and nothing else."""
from datetime import UTC, datetime, timedelta

import pytest

from freshet.rag.timeframe import infer_window

NOW = datetime(2026, 8, 19, 14, 30, tzinfo=UTC)
MIDNIGHT = datetime(2026, 8, 19, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize("question, expected_since, expected_label", [
    ("what incidents happened today?", MIDNIGHT, "today"),
    ("anything broken this morning?", MIDNIGHT, "today"),
    ("what's degraded right now?", NOW - timedelta(hours=6), "last 6 hours"),
    ("which services are currently degraded?", NOW - timedelta(hours=6), "last 6 hours"),
    ("what is happening now", NOW - timedelta(hours=6), "last 6 hours"),
    ("incidents in the last 3 hours", NOW - timedelta(hours=3), "last 3 hours"),
    ("outages over the past 2 days", NOW - timedelta(days=2), "last 2 days"),
    ("anything in the last hour?", NOW - timedelta(hours=1), "last hour"),
    ("what happened this week?", NOW - timedelta(days=7), "last 7 days"),
    ("what broke yesterday?", MIDNIGHT - timedelta(days=1), "since yesterday"),
    ("recent Cloudflare problems", NOW - timedelta(hours=24), "last 24 hours"),
])
def test_recognised_phrases(question, expected_since, expected_label):
    since, label = infer_window(question, NOW)
    assert since == expected_since
    assert label == expected_label


@pytest.mark.parametrize("question", [
    "Cloudflare errors",
    "what caused the GitHub Actions outage?",
    "which services are degraded?",          # no temporal word at all
    "is anything known about the API?",      # 'known' must not match \bnow\b
    "show me nowhere-land incidents",        # substring, not a word
])
def test_questions_without_a_temporal_phrase_are_untouched(question):
    assert infer_window(question, NOW) == (None, None)


def test_a_numeric_span_beats_the_generic_rule():
    # "last 2 hours" must not be captured by the bare 'last week'/'now' rules
    since, label = infer_window("errors in the last 2 hours right now", NOW)
    assert label == "last 2 hours"
    assert since == NOW - timedelta(hours=2)


def test_the_window_is_always_in_the_past():
    for q in ("today", "right now", "this week", "last 5 days", "yesterday"):
        since, _ = infer_window(q, NOW)
        assert since is not None and since <= NOW
