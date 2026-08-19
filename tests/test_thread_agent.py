"""The Slack thread is the RAG surface: questions go through hybrid retrieval.

The brief is a key lookup — an incident's updates are addressable. A follow-up
question ("is anything else affected?") has no key, so it runs the same path the
query API uses and the retrieval eval measures.
"""

from freshet.autopilot.thread_agent import (
    ABSTAIN_REPLY,
    answer_question,
    is_human_reply,
    poll_threads,
)

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
    def __init__(self, rows):
        self.rows, self.executed = rows, []

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


def _patch_search(monkeypatch, abstained=False, hits=(1,)):
    from freshet.autopilot import thread_agent

    class _Res:
        def __init__(self):
            self.hits, self.abstained = list(hits), abstained
    monkeypatch.setattr("freshet.api.retrieval.hybrid_search", lambda *a, **k: _Res())
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
        hits, abstained = [1], False
    def _fake(conn, emb, q, **kw):
        seen["q"], seen["k"] = q, kw.get("k")
        return _Res()
    monkeypatch.setattr("freshet.api.retrieval.hybrid_search", _fake)
    out = answer_question(None, _Emb(), _Composer(), "has this happened before?")
    assert seen["q"] == "has this happened before?" and seen["k"] == 6
    assert out.startswith("answer to")


def test_a_very_long_question_is_capped(monkeypatch):
    seen = {}

    class _Res:
        hits, abstained = [1], False
    monkeypatch.setattr("freshet.api.retrieval.hybrid_search",
                        lambda c, e, q, **k: (seen.__setitem__("q", q), _Res())[1])
    answer_question(None, _Emb(), _Composer(), "x" * 5000)
    assert len(seen["q"]) == 500
