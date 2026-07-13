from datetime import datetime, timezone

from freshet.api.retrieval import RetrievedHit
from freshet.api.synthesis import build_timeline


def _hit(eid, ts_min, type_, text, rank):
    ts = datetime(2026, 7, 1, 12, ts_min, 0, tzinfo=timezone.utc)
    # `score` is deliberately uniform: the selector must use LIST ORDER (rank),
    # which is what reranking changes, not the RRF score.
    return RetrievedHit(chunk_id=eid + "#0", event_id=eid, service="api", ts=ts,
                        indexed_at=ts, source="deploy", text=text, type=type_,
                        similarity=1.0, score=1.0), rank


def _ordered(*pairs):
    """Return hits ordered by their given rank (0 = most relevant)."""
    return [h for h, _ in sorted(pairs, key=lambda p: p[1])]


def test_score_aware_prefers_higher_ranked_bad_change_over_later_benign_decoy():
    # bad change @00 ranked #0 (most relevant), benign decoy @03 ranked #5 (less
    # relevant) is LATER (closer to spike). Naive last-before-spike would pick benign;
    # score-aware picks the bad change.
    hits = _ordered(
        _hit("bad",    0, "config_changed", "pool size 8 -> 64 on api", 0),
        _hit("benign", 3, "config_changed", "log level info -> debug on api", 5),
        _hit("spike",  5, "error_spike", "5xx on api crossed 5%", 3),
    )
    tl = build_timeline(hits)
    assert tl.cause is not None and tl.cause.event_id == "bad"


def test_single_candidate_is_identical_to_recency_regression_guard():
    # ⑤ regression: one change before the spike -> that change, regardless of rank.
    hits = _ordered(
        _hit("commit", 0, "commit", "commit abc1234: bump pool size (by alice)", 2),
        _hit("spike",  5, "error_spike", "5xx on api crossed 5%", 0),
    )
    tl = build_timeline(hits)
    assert tl.cause is not None and tl.cause.event_id == "commit"


def test_abstains_when_no_change_before_spike():
    hits = _ordered(
        _hit("spike",   5, "error_spike", "5xx on api crossed 5%", 0),
        _hit("chat",    6, "message", "alice: investigating", 1),
        _hit("healthy", 9, "healthy", "back to normal", 2),
    )
    tl = build_timeline(hits)
    assert tl.cause is None
