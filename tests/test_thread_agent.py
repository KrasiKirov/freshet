"""The Slack thread is the RAG surface: questions go through hybrid retrieval.

The brief is a key lookup — an incident's updates are addressable. A follow-up
question ("is anything else affected?") has no key, so it runs the same path the
query API uses and the retrieval eval measures.
"""

import pytest

from freshet.autopilot.thread_agent import (
    ABSTAIN_REPLY,
    answer_question,
    is_human_reply,
    poll_threads,
)
from freshet.autopilot.thread_agent import RateLimited as ThreadRateLimited


class _Hit:
    """Minimal hit: the merge dedupes on event_id, the composer cites it."""
    def __init__(self, event_id="corpus:1:a"):
        self.event_id = event_id


THREAD = "1700000000.000100"


def _msg(ts, text, **kw):
    return {"ts": ts, "text": text, **kw}


def test_our_own_messages_are_never_answered():
    """Every message this bot posts carries a bot_id; answering one would have
    the agent talking to itself."""
    assert not is_human_reply(_msg("1.1", "brief text", bot_id="B123"), THREAD)
    assert not is_human_reply(_msg(THREAD, "the parent brief"), THREAD)
    assert not is_human_reply(_msg("1.2", "joined", subtype="channel_join"), THREAD)
    assert not is_human_reply(_msg("1.3", "   "), THREAD)
    assert is_human_reply(_msg("1.4", "has this happened before?"), THREAD)


class _Conn:
    """Rows are (incident_id, slack_ts, seen_ts); the channel id column is
    appended here the way coalesce(slack_channel_id, %s) returns it."""
    def __init__(self, rows, channel_id="C123"):
        self.rows = [(r + (channel_id,)) if len(r) == 3 else r for r in rows]
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        rows = self.rows if "slack_ts IS NOT NULL" in sql else []

        class _R:
            def fetchall(self_inner):
                return rows
        return _R()


class _Client:
    def __init__(self, messages, fail=False):
        self.messages, self.fail = messages, fail
        self.posted = []

    def conversations_replies(self, channel, ts, oldest=None):
        if self.fail:
            raise RuntimeError("slack down")
        return {"messages": self.messages}

    def chat_postMessage(self, **kw):
        self.posted.append(kw)
        return {"ok": True, "ts": "9.9"}


class _Emb:
    min_similarity = 0.7


class _Composer:
    def compose(self, question, hits):
        return f"answer to {question}"


def _patch_search(monkeypatch, abstained=False, hits=(_Hit(),)):
    from freshet.autopilot import thread_agent

    class _Res:
        def __init__(self):
            self.hits, self.abstained = list(hits), abstained
    monkeypatch.setattr("freshet.rag.retrieval.hybrid_search", lambda *a, **k: _Res())
    return thread_agent


def test_a_question_is_answered_in_thread_and_marked_seen(monkeypatch):
    _patch_search(monkeypatch)
    conn = _Conn([("INC-1", THREAD, None)])
    client = _Client([_msg(THREAD, "parent"), _msg("1.5", "what else is down?")])
    assert poll_threads(conn, _Emb(), _Composer(), client, "#ops") == 1
    assert client.posted[0]["thread_ts"] == THREAD
    assert "what else is down?" in client.posted[0]["text"]
    assert any("thread_seen_ts" in sql and p == ("1.5", "INC-1")
               for sql, p in conn.executed), "the reply must not be answered twice"


def test_an_already_seen_reply_is_not_answered_again(monkeypatch):
    _patch_search(monkeypatch)
    conn = _Conn([("INC-1", THREAD, "1.5")])
    client = _Client([_msg("1.5", "old question")])
    assert poll_threads(conn, _Emb(), _Composer(), client, "#ops") == 0
    assert client.posted == []


def test_weak_evidence_abstains_rather_than_guessing(monkeypatch):
    _patch_search(monkeypatch, abstained=True)
    conn = _Conn([("INC-1", THREAD, None)])
    client = _Client([_msg("1.5", "why is the moon offline?")])
    poll_threads(conn, _Emb(), _Composer(), client, "#ops")
    assert client.posted[0]["text"] == ABSTAIN_REPLY


def test_one_unreadable_thread_does_not_stop_the_others(monkeypatch):
    _patch_search(monkeypatch)
    conn = _Conn([("INC-1", THREAD, None)])
    assert poll_threads(conn, _Emb(), _Composer(), _Client([], fail=True), "#ops") == 0


def test_answer_question_uses_retrieval_not_a_key_lookup(monkeypatch):
    """This is the whole point: the thread answer is the measured RAG path."""
    seen = {}

    class _Res:
        hits, abstained = [_Hit()], False
    def _fake(conn, emb, q, **kw):
        seen["q"], seen["k"] = q, kw.get("k")
        return _Res()
    monkeypatch.setattr("freshet.rag.retrieval.hybrid_search", _fake)
    out = answer_question(None, _Emb(), _Composer(), "has this happened before?")
    assert seen["q"] == "has this happened before?" and seen["k"] == 6
    assert out.startswith("answer to")


def test_a_very_long_question_is_capped(monkeypatch):
    seen = {}

    class _Res:
        hits, abstained = [_Hit()], False
    monkeypatch.setattr("freshet.rag.retrieval.hybrid_search",
                        lambda c, e, q, **k: (seen.__setitem__("q", q), _Res())[1])
    answer_question(None, _Emb(), _Composer(), "x" * 5000)
    assert len(seen["q"]) == 500


# --- temporal questions must reach recent evidence ---------------------------
# This moved here from the query endpoint. Without it, "what broke today?" asked
# in a Slack thread competes on semantics alone and returns boilerplate from
# months ago — the exact bug the window inference was built to fix.

def _capture_search(monkeypatch, abstained=False):
    seen = {}

    class _Res:
        def __init__(self):
            self.hits, self.abstained = [_Hit()], abstained

    def _fake(conn, emb, q, **kw):
        seen["q"], seen["since"], seen["k"] = q, kw.get("since"), kw.get("k")
        return _Res()
    monkeypatch.setattr("freshet.rag.retrieval.hybrid_search", _fake)
    return seen


def test_a_temporal_question_narrows_retrieval_to_that_window(monkeypatch):
    seen = _capture_search(monkeypatch)
    out = answer_question(None, _Emb(), _Composer(), "what broke today?")
    assert seen["since"] is not None, "the filter must reach retrieval"
    assert "time filter: today" in out, "an inferred filter must be visible"


def test_a_plain_question_applies_no_window(monkeypatch):
    seen = _capture_search(monkeypatch)
    out = answer_question(None, _Emb(), _Composer(), "why did Cloudflare fail?")
    assert seen["since"] is None
    assert "time filter" not in out


def test_an_abstained_temporal_question_still_shows_its_window(monkeypatch):
    _capture_search(monkeypatch, abstained=True)
    out = answer_question(None, _Emb(), _Composer(), "what broke today?")
    assert out.startswith(ABSTAIN_REPLY)
    assert "time filter: today" in out, "the reader must see WHY nothing came back"


def test_the_window_is_inferred_from_the_capped_question(monkeypatch):
    """The cap is applied before inference, so a temporal word past the cap
    cannot silently change the window."""
    seen = _capture_search(monkeypatch)
    answer_question(None, _Emb(), _Composer(), "x" * 600 + " today")
    assert len(seen["q"]) == 500 and seen["since"] is None


# --- call volume ------------------------------------------------------------
# Measured against real Slack: polling every thread on every idle tick made 493
# conversations.replies calls in three minutes and earned a 429.

class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_polling_is_throttled_between_ticks(monkeypatch):
    from freshet.autopilot import thread_agent

    calls = []
    monkeypatch.setattr(thread_agent, "poll_threads",
                        lambda *a, **k: (calls.append(1), 0)[1])
    clock = _Clock()
    poller = thread_agent.ThreadPoller(interval_s=30, now=clock)

    poller(None, None, None, None, "#ops")     # first tick polls
    for t in (1, 5, 29):                        # a busy idle loop
        clock.t = t
        poller(None, None, None, None, "#ops")
    assert len(calls) == 1, "an idle loop must not poll Slack every second"
    clock.t = 31
    poller(None, None, None, None, "#ops")
    assert len(calls) == 2


def test_a_rate_limit_backs_the_whole_loop_off(monkeypatch):
    from freshet.autopilot import thread_agent

    calls = []

    def _boom(*a, **k):
        calls.append(1)
        raise thread_agent.RateLimited("ratelimited")
    monkeypatch.setattr(thread_agent, "poll_threads", _boom)
    clock = _Clock()
    poller = thread_agent.ThreadPoller(interval_s=30, now=clock)

    assert poller(None, None, None, None, "#ops") == 0    # swallowed, not raised
    clock.t = 35                                          # normal interval elapsed
    poller(None, None, None, None, "#ops")
    assert len(calls) == 1, "backoff must outlast the ordinary interval"
    clock.t = 95
    poller(None, None, None, None, "#ops")
    assert len(calls) == 2


def test_a_rate_limited_thread_raises_rather_than_looping(monkeypatch):
    """Per-thread 'continue' on a 429 just earns more 429s."""
    _patch_search(monkeypatch)
    conn = _Conn([("INC-1", THREAD, None)])

    class _Limited:
        def conversations_replies(self, **kw):
            raise RuntimeError("{'ok': False, 'error': 'ratelimited'}")

        def chat_postMessage(self, **kw):
            return {"ok": True}

    with pytest.raises(ThreadRateLimited):
        poll_threads(conn, _Emb(), _Composer(), _Limited(), "#ops")


def test_other_errors_still_skip_just_that_thread(monkeypatch):
    _patch_search(monkeypatch)
    conn = _Conn([("INC-1", THREAD, None)])
    assert poll_threads(conn, _Emb(), _Composer(), _Client([], fail=True), "#ops") == 0


# --- deictic follow-ups ------------------------------------------------------
# Observed in real Slack: "give me more details on this event" scored 0.661
# against a 0.700 floor and abstained. "this event" is a pointer, not a
# description — there is nothing semantic to match, so the thread's own incident
# has to be evidence regardless of what retrieval finds.

class _ConnWithUpdates:
    """Returns thread rows AND incident updates, like the real schema."""
    def __init__(self, updates):
        self.updates, self.executed = updates, []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "slack_ts IS NOT NULL" in sql:
            rows = [("INC-1", THREAD, None, "C123")]
        elif "GROUP BY event_id" in sql:
            rows = self.updates
        else:
            rows = []

        class _R:
            def fetchall(self_inner):
                return rows
        return _R()


def _update_row(event_id="INC-1:u1"):
    from datetime import UTC, datetime
    return (event_id, datetime.now(UTC), "TLS certificates are not being rotated",
            "mongodb", "status_update", "alert")


def test_a_deictic_question_is_answered_from_the_threads_own_incident(monkeypatch):
    _patch_search(monkeypatch, abstained=True)      # corpus search finds nothing
    conn = _ConnWithUpdates([_update_row()])
    out = answer_question(conn, _Emb(), _Composer(), "give me more details on this event",
                          incident_id="INC-1")
    assert out != ABSTAIN_REPLY, "the thread's incident is evidence the question pointed at"
    assert "more details" in out


def test_a_deictic_question_with_no_incident_still_abstains(monkeypatch):
    _patch_search(monkeypatch, abstained=True)
    out = answer_question(_ConnWithUpdates([]), _Emb(), _Composer(),
                          "give me more details on this event", incident_id="INC-1")
    assert out == ABSTAIN_REPLY, "no evidence anywhere is still an honest refusal"


def test_abstained_corpus_hits_are_not_folded_in(monkeypatch):
    """Below the floor means not evidence; using them anyway discards the signal."""
    seen = {}

    class _C:
        def compose(self, q, hits):
            seen["hits"] = list(hits)
            return "answer"
    _patch_search(monkeypatch, abstained=True, hits=(_Hit("corpus:weak:1"),))
    answer_question(_ConnWithUpdates([_update_row()]), _Emb(), _C(), "q",
                    incident_id="INC-1")
    assert [h.event_id for h in seen["hits"]] == ["INC-1:u1"]
