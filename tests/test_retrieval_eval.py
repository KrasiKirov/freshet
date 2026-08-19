"""The eval must measure the production path, and must not be winnable blind."""
import json
from pathlib import Path

import pytest

from freshet.eval.retrieval_eval import (
    aggregate,
    cause_event_ids,
    dedupe_events,
    event_id_for,
    events_from_incident,
    load_labels,
    score_one,
)

FIXTURES = Path(__file__).resolve().parents[1] / "freshet/eval/fixtures/real"

_INCIDENT = {
    "id": "abc123",
    "name": "Elevated API errors",
    "incident_updates": [
        {"id": "u2", "body": "Root cause was a bad config push.",
         "created_at": "2026-08-02T10:05:00.000Z", "status": "identified"},
        {"id": "u1", "body": "We are investigating.",
         "created_at": "2026-08-02T10:00:00.000Z", "status": "investigating"},
        {"id": "u0", "body": "   ", "created_at": "2026-08-02T09:00:00.000Z",
         "status": "investigating"},
    ],
}


def test_events_use_the_production_id_and_text_shape():
    evs = events_from_incident("github", _INCIDENT)
    assert [e.event_id for e in evs] == ["github:abc123:u2", "github:abc123:u1"], \
        "blank-bodied updates must be dropped; ids must match the Flink projection"
    # Flink emits `incident_name || ': ' || text`, with the name carried separately
    assert evs[0].text == "Elevated API errors: Root cause was a bad config push."
    assert evs[0].title == "Elevated API errors"
    assert evs[0].service == "github"


def test_labels_map_onto_indexed_event_ids():
    entry = {"incident_id": "github:abc123", "cause_update_ids": ["u2"]}
    assert cause_event_ids(entry) == {"github:abc123:u2"}
    evs = events_from_incident("github", _INCIDENT)
    assert cause_event_ids(entry) <= {e.event_id for e in evs}, \
        "a label that matches no indexed event would score 0 for the wrong reason"


def test_scoring_uses_rank_of_the_first_cause_bearing_event():
    ranked = ["a", "b", "cause", "d"]
    s = score_one(ranked, {"cause"})
    assert s["hit_at_k"] and s["mrr"] == pytest.approx(1 / 3) and not s["top1_cite"]


def test_a_miss_scores_zero_not_none():
    s = score_one(["a", "b"], {"cause"})
    assert not s["hit_at_k"] and s["mrr"] == 0.0 and not s["top1_cite"]


def test_a_cause_beyond_k_is_not_counted_as_recall():
    ranked = [f"x{i}" for i in range(5)] + ["cause"]
    assert not score_one(ranked, {"cause"})["hit_at_k"], "rank 6 must not count at k=5"


def test_chunk_level_hits_collapse_to_first_appearance():
    class _H:
        def __init__(self, e):
            self.event_id = e
    assert dedupe_events([_H("a"), _H("a"), _H("b"), _H("a")]) == ["a", "b"]


def test_aggregate_of_nothing_is_zero_not_a_crash():
    assert aggregate([])["n"] == 0


def test_the_committed_labels_are_human_reviewed():
    labels = load_labels(FIXTURES)
    assert labels["curated"] == "reviewed"
    assert len(labels["labeled"]) >= 10
    for entry in labels["labeled"]:
        assert entry["cause_update_ids"], f"{entry['incident_id']} labels no cause"
        assert entry["query"].strip()


def test_every_label_points_at_an_update_present_in_the_corpus():
    """A label referencing a missing update would silently depress every score."""
    labels = load_labels(FIXTURES)
    known = set()
    for path in FIXTURES.glob("*.json"):
        if path.name == "labels.json":
            continue
        for inc in json.loads(path.read_text()).get("incidents") or []:
            for u in inc.get("incident_updates") or []:
                known.add(event_id_for(path.stem, inc["id"], u["id"]))
    for entry in labels["labeled"]:
        missing = cause_event_ids(entry) - known
        assert not missing, f"{entry['incident_id']} labels unknown updates: {missing}"
