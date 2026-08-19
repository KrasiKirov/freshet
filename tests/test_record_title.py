"""A chunk is evidence; the incident's title is what labels it.

Regression: Flink emits "<incident_name>: <update text>" as one string, so only
the FIRST chunk of a long update carries the name. The UI derived its labels from
chunk text, which produced the citation "incident." and the suggested question
"what is happening with the are monitoring for continued stability.?".
"""
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import records_for_event, title_of

SQL = (Path(__file__).resolve().parents[1] / "freshet/stream/dedup_job.sql").read_text()


@pytest.mark.parametrize("text, expected", [
    ("CDNJS Elevated Errors: This incident is now resolved.", "CDNJS Elevated Errors"),
    ("[Minor] Issues with All Files Page: After monitoring", "[Minor] Issues with All Files Page"),
    ("no colon here at all", None),
    ("trailing colon but no body: ", None),
    ("", None),
])
def test_title_is_the_prefix_before_the_first_separator(text, expected):
    assert title_of(text) == expected


def test_a_title_containing_a_colon_is_truncated_by_derivation():
    # Precisely why the field travels separately from Flink now: derivation is a
    # heuristic and this case is unrecoverable from the fused string alone.
    assert title_of("Aug 10: 30am UTC maintenance: has completed") == "Aug 10"


def _event(text, title=None):
    return Event(event_id="svc:1:a", ts=datetime.now(UTC), service="svc",
                 source=EventSource.ALERT, type="status_update", text=text, title=title)


def test_every_chunk_of_an_event_carries_the_title():
    long_text = "Big Outage: " + ("degraded " * 400)
    recs = records_for_event(_event(long_text))
    assert len(recs) > 1, "need a multi-chunk event for this to mean anything"
    assert all(r.title == "Big Outage" for r in recs), (
        "later chunks must not be left title-less — that is the original bug")


def test_the_explicit_field_beats_derivation():
    rec = records_for_event(_event("Wrong: body", title="Real Incident Name"))[0]
    assert rec.title == "Real Incident Name"


def test_a_titleless_event_yields_none_not_a_fragment():
    recs = records_for_event(_event("just some text with no separator"))
    assert all(r.title is None for r in recs)


def test_the_flink_sink_declares_the_title_the_consumer_expects():
    block = re.search(r"CREATE TABLE normalized_updates \((.*?)\) WITH", SQL, re.S)
    assert block and re.search(r"^\s*title\s+STRING", block.group(1), re.M), (
        "normalized_updates must emit `title`; the embedder reads Event.title")
    assert "incident_name AS title" in SQL, "the projection must populate it"
    assert "title" in Event.model_fields
