"""Answer follow-up questions in the Slack thread under a brief.

This is what joins the two halves of the project. The brief itself is grounded
generation over a KEY LOOKUP — an incident's updates are addressable, so there is
nothing to search for. A human replying "is anything else affected?" or "has this
hit us before?" asks something with no key, over ~1,200 incidents from 42
providers. That is retrieval, and it runs the same `hybrid_search` + composer path
the query API uses — so the retrieval eval measures this surface too, rather than
measuring a web UI nobody demos.

Replies are POLLED via conversations.replies rather than pushed over Socket Mode:
the bot token already in use is enough, no app-level token or public endpoint is
needed, and the whole project is already a polled pipeline.
"""

from __future__ import annotations

import logging

from freshet.api.composer import NO_EVIDENCE
from freshet.api.timeframe import infer_window
from freshet.autopilot.sinks.base import Sink

log = logging.getLogger(__name__)

MAX_QUESTION_CHARS = 500
ABSTAIN_REPLY = ("I don't have enough relevant indexed evidence to answer that "
                 "confidently.")

_OPEN_THREADS_SQL = (
    "SELECT incident_id, slack_ts, thread_seen_ts FROM incidents"
    " WHERE slack_ts IS NOT NULL AND brief_delivered_at IS NOT NULL"
    " ORDER BY brief_delivered_at DESC LIMIT %s")
_MARK_SEEN_SQL = "UPDATE incidents SET thread_seen_ts = %s WHERE incident_id = %s"


def is_human_reply(message: dict, thread_ts: str) -> bool:
    """A question from a person, not our own brief or postmortem echoed back.

    Slack returns the parent message in `conversations.replies`, and every message
    this bot posted carries a bot_id. Answering either would have the agent
    talking to itself.
    """
    if message.get("bot_id") or message.get("subtype"):
        return False
    if message.get("ts") == thread_ts:
        return False                        # the parent brief
    return bool((message.get("text") or "").strip())


def answer_question(conn, embedder, composer, question: str) -> str:
    """Hybrid retrieval, abstention, cited answer — the path the eval measures.

    A temporal phrase in the question ("what broke today?") narrows retrieval to
    that window. Without it the question competes on semantics alone, and in a
    corpus of four years of status updates the word "incident" matches boilerplate
    from months ago rather than anything from today — which is the wrong answer
    from a product whose entire claim is freshness.
    """
    from freshet.api.retrieval import hybrid_search

    question = question[:MAX_QUESTION_CHARS]
    since, window = infer_window(question)
    result = hybrid_search(conn, embedder, question, k=6, since=since)
    if result.abstained:
        return _with_window(ABSTAIN_REPLY, window)
    # The question is untrusted text from a Slack user; the composer already
    # refuses instructions found in evidence, and every citation it emits is
    # verified against the retrieved events before this is posted.
    answer = composer.compose(question, result.hits) or NO_EVIDENCE
    return _with_window(answer, window)


def _with_window(answer: str, window: str | None) -> str:
    """Say which window was applied. An inferred filter the reader cannot see is
    indistinguishable from the bot ignoring half their question."""
    return f"{answer}\n\n_time filter: {window} (inferred from your question)_" \
        if window else answer


def poll_threads(conn, embedder, composer, client, channel: str, *,
                 sink: Sink | None = None, limit: int = 20) -> int:
    """Answer new replies in threads we have briefed. Returns replies posted."""
    posted = 0
    for incident_id, thread_ts, seen_ts in conn.execute(
            _OPEN_THREADS_SQL, (limit,)).fetchall():
        try:
            resp = client.conversations_replies(channel=channel, ts=thread_ts,
                                                oldest=seen_ts or thread_ts)
            messages = list(resp["messages"] or [])
        except Exception as exc:                # one bad thread must not stop the rest
            log.warning("thread %s unreadable: %r", thread_ts, exc)
            continue

        newest = seen_ts
        for message in messages:
            if not is_human_reply(message, thread_ts):
                continue
            if seen_ts and message["ts"] <= seen_ts:
                continue                        # already answered
            answer = answer_question(conn, embedder, composer, message["text"])
            client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=answer)
            posted += 1
            newest = message["ts"] if not newest else max(newest, message["ts"])
        if newest and newest != seen_ts:
            conn.execute(_MARK_SEEN_SQL, (newest, incident_id))
    return posted
