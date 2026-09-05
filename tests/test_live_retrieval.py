"""No labels, no judge: ground truth is the incident id itself. These tests
guard the two things that would make that free lunch worthless: an eligibility
rule that lets a one-update incident score a trivial "hit", and a blind arm
that turns out to be winnable."""
from datetime import datetime

import pytest

from freshet.eval.live_retrieval import (
    aggregate,
    blind_recent,
    capped,
    dedupe_events,
    eligible_candidates,
    score_one,
)


def _dt(day: int) -> datetime:
    return datetime(2026, 1, day)


def test_an_incident_with_only_one_update_is_not_eligible():
    events_by_incident = {"inc1": [("e1", _dt(1))]}
    assert eligible_candidates(events_by_incident) == []


def test_an_incident_with_two_updates_is_eligible_and_picks_the_earliest_as_query():
    events_by_incident = {"inc1": [("e2", _dt(2)), ("e1", _dt(1))]}
    [c] = eligible_candidates(events_by_incident)
    assert c.incident_id == "inc1"
    assert c.query_event_id == "e1", "the query must be the FIRST update, by ts"
    assert c.other_event_ids == frozenset({"e2"})


def test_ties_on_ts_break_by_event_id_for_determinism():
    """Two updates landing at the identical timestamp must not make the choice
    of query depend on database row order."""
    events_by_incident = {"inc1": [("b", _dt(1)), ("a", _dt(1))]}
    [c] = eligible_candidates(events_by_incident)
    assert c.query_event_id == "a"


def test_eligible_candidates_are_sorted_by_incident_id():
    events_by_incident = {
        "zeta": [("z1", _dt(1)), ("z2", _dt(2))],
        "alpha": [("a1", _dt(1)), ("a2", _dt(2))],
    }
    cands = eligible_candidates(events_by_incident)
    assert [c.incident_id for c in cands] == ["alpha", "zeta"]


def test_capped_takes_a_deterministic_slice_not_a_sample():
    events_by_incident = {str(i): [(f"{i}a", _dt(1)), (f"{i}b", _dt(2))] for i in range(10)}
    cands = eligible_candidates(events_by_incident)
    assert [c.incident_id for c in capped(cands, 3)] == ["0", "1", "2"]
    assert capped(cands, None) == cands, "no cap means every eligible incident runs"


def test_the_querys_own_event_is_excluded_from_the_results_it_is_scored_against():
    """The query text IS an update's body verbatim, so that update is trivially
    its own top hit. Scoring it would make every arm look perfect for the wrong
    reason."""
    class _H:
        def __init__(self, e):
            self.event_id = e

    hits = [_H("query_event"), _H("other_update"), _H("unrelated")]
    ranked = dedupe_events(hits, exclude="query_event")
    assert ranked == ["other_update", "unrelated"]
    assert score_one(ranked, {"other_update"})["top1_cite"] is True


def test_recall_at_5_counts_a_hit_when_any_other_update_is_in_the_top_5():
    causes = {"u2", "u3"}  # any other update of the same incident counts
    ranked = ["noise1", "noise2", "u3", "noise3", "noise4"]
    assert score_one(ranked, causes)["hit_at_k"] is True


def test_recall_at_5_misses_when_the_only_other_update_is_past_rank_5():
    causes = {"u2"}
    ranked = ["n1", "n2", "n3", "n4", "n5", "u2"]
    assert score_one(ranked, causes)["hit_at_k"] is False


def test_blind_recent_ranks_by_recency_ignoring_the_query():
    class _Cur:
        def fetchall(self):
            return [("e1", _dt(1)), ("e2", _dt(3)), ("e3", _dt(2))]

    class FakeConn:
        def execute(self, sql, params=None):
            return _Cur()

    assert blind_recent(FakeConn(), k=2) == ["e2", "e3"]


def test_blind_recent_scores_near_zero_when_recency_is_uninformative():
    """The gameability guard: blind_recent's ranking never looks at the query,
    so on a corpus where recency doesn't correlate with incident membership,
    plugging its fixed ranking into every query's score must land near zero."""
    causes_per_query = [{"a2"}, {"b2"}, {"c2"}, {"d2"}]
    blind_ranking = ["z1", "z2", "z3", "z4", "z5"]  # unrelated to any incident above
    records = [score_one(blind_ranking, causes) for causes in causes_per_query]
    agg = aggregate(records)
    assert agg["recall@5"] == 0.0
    assert agg["mrr"] == 0.0
    assert agg["top1_cite"] == 0.0


def test_aggregate_of_nothing_is_zero_not_a_crash():
    assert aggregate([])["n"] == 0


@pytest.mark.parametrize("k", [1, 5, 10])
def test_capped_never_grows_the_candidate_list(k):
    events_by_incident = {str(i): [(f"{i}a", _dt(1)), (f"{i}b", _dt(2))] for i in range(3)}
    cands = eligible_candidates(events_by_incident)
    assert len(capped(cands, k)) == min(k, len(cands))
