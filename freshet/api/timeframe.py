"""Infer a retrieval time window from an explicit temporal phrase in a question.

Freshness-first retrieval has a gap: "what happened today?" resembles no single
incident, so pure semantic ranking returns whatever mentions the word "incident"
— which in a status-feed corpus is boilerplate from months ago. The question does
carry the constraint; it is just in the words rather than in a filter.

Deliberately conservative. This fires only on explicit temporal expressions, and
the caller SHOWS the window it applied, so an inferred filter is visible and
overridable rather than hidden behaviour. An unrecognised question yields no
window and the ordinary semantic path is untouched.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

# Ordered: the first match wins, so specific patterns ("last 3 hours") must
# precede generic ones ("now"). Each entry is (pattern, window-builder, label).
_RULES: list[tuple[re.Pattern[str], object, str]] = []


def _rule(pattern: str, build, label: str) -> None:
    _RULES.append((re.compile(pattern, re.I), build, label))


def _midnight(now: datetime) -> datetime:
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


# Numeric spans first — "the last 2 hours" must not be swallowed by "last week".
_rule(r"\b(?:last|past|previous)\s+(\d+)\s*h(?:ou)?rs?\b",
      lambda now, m: now - timedelta(hours=int(m.group(1))), "last {0} hours")
_rule(r"\b(?:last|past|previous)\s+(\d+)\s*days?\b",
      lambda now, m: now - timedelta(days=int(m.group(1))), "last {0} days")
_rule(r"\b(?:last|past|previous)\s+hour\b",
      lambda now, m: now - timedelta(hours=1), "last hour")
_rule(r"\b(?:this|the)\s+morning\b", lambda now, m: _midnight(now), "today")
_rule(r"\btoday\b|\bso far today\b", lambda now, m: _midnight(now), "today")
_rule(r"\byesterday\b", lambda now, m: _midnight(now) - timedelta(days=1),
      "since yesterday")
_rule(r"\b(?:this|past|last)\s+week\b|\blast\s+7\s*days\b",
      lambda now, m: now - timedelta(days=7), "last 7 days")
# "right now" / "currently" mean ongoing, not instantaneous: a status page updates
# every few hours, so a one-minute window would be empty for every real incident.
_rule(r"\bright now\b|\bcurrently\b|\bat the moment\b|\bongoing\b|\bnow\b",
      lambda now, m: now - timedelta(hours=6), "last 6 hours")
_rule(r"\brecent(?:ly)?\b|\blately\b|\blatest\b",
      lambda now, m: now - timedelta(hours=24), "last 24 hours")


def infer_window(question: str, now: datetime | None = None
                 ) -> tuple[datetime | None, str | None]:
    """Return (since, human label), or (None, None) when no phrase is recognised."""
    now = now or datetime.now(UTC)
    for pattern, build, label in _RULES:
        m = pattern.search(question)
        if m:
            since = build(now, m)                      # type: ignore[operator]
            return since, label.format(*m.groups())
    return None, None
