"""Unit tests for the real-data validation eval's pure parts: the label →
event-id mapping must match what map_incident derives, and scoring must rank
at the event level (hits are chunks)."""
import json
from dataclasses import dataclass
from pathlib import Path

from freshet.eval.real_eval import (
    K,
    aggregate,
    load_corpus,
    score_label,
    update_event_id,
)
from freshet.ingest.status_poller import map_incident


def test_update_event_id_matches_map_incident():
    fx = json.loads(Path("freshet/ingest/fixtures/status/sample_incidents.json").read_text())
    incident = fx["incidents"][0]
    events = map_incident("cloudflare", incident)
    derived = [update_event_id(incident["id"], u["id"])
               for u in incident["incident_updates"]]
    assert [e.event_id for e in events] == derived


def test_load_corpus_uses_provider_filename_as_service(tmp_path):
    fx = Path("freshet/ingest/fixtures/status/sample_incidents.json").read_text()
    (tmp_path / "cloudflare.json").write_text(fx)
    # labels.json must be ignored, not parsed as a feed
    (tmp_path / "labels.json").write_text('{"curated": "draft", "labeled": []}')
    events = load_corpus(tmp_path)
    assert len(events) == 3
    assert all(e.service == "cloudflare" for e in events)
    assert all(e.incident_id == "cloudflare:inc_100" for e in events)


@dataclass
class _Hit:
    event_id: str


def test_score_label_ranks_events_not_chunks():
    # two chunks of the same event must count as one ranked event
    hits = [_Hit("ev_a"), _Hit("ev_a"), _Hit("ev_cause"), _Hit("ev_b")]
    rec = score_label(hits, {"ev_cause"})
    assert rec["hit_at_k"] is True
    assert rec["mrr"] == 0.5          # second distinct event
    assert rec["top1_cite"] is False


def test_score_label_miss_and_empty():
    assert score_label([_Hit("x")], {"ev_cause"})["mrr"] == 0.0
    rec = score_label([], {"ev_cause"})
    assert rec["hit_at_k"] is False and rec["top1_cite"] is False


def test_score_label_rank_beyond_k_is_a_miss():
    hits = [_Hit(f"ev_{i}") for i in range(K)] + [_Hit("ev_cause")]
    rec = score_label(hits, {"ev_cause"})
    assert rec["hit_at_k"] is False
    assert rec["mrr"] == 1.0 / (K + 1)


def test_aggregate():
    recs = [
        {"hit_at_k": True, "mrr": 1.0, "top1_cite": True},
        {"hit_at_k": False, "mrr": 0.0, "top1_cite": False},
    ]
    agg = aggregate(recs)
    assert agg == {"recall@5": 0.5, "mrr": 0.5, "top1_cite": 0.5, "n": 2}
    assert aggregate([])["n"] == 0


def test_sample_fixture_cause_update_maps():
    """The face-validity fixture's 'identified' update (the one stating the
    cause) maps to a stable event id via the label path."""
    fx = json.loads(Path("freshet/ingest/fixtures/status/sample_incidents.json").read_text())
    incident = fx["incidents"][0]
    cause_update = next(u for u in incident["incident_updates"]
                        if u["status"] == "identified")
    eid = update_event_id(incident["id"], cause_update["id"])
    events = {e.event_id: e for e in map_incident("cloudflare", incident)}
    assert eid in events
    assert "cause" in events[eid].text  # "A bad WAF rule deploy is the cause"
