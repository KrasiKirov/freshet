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
import time

from freshet.autopilot.sinks.base import Sink
from freshet.rag.budget import BudgetExhausted
from freshet.rag.composer import NO_EVIDENCE
from freshet.rag.timeframe import infer_window

log = logging.getLogger(__name__)

MAX_QUESTION_CHARS = 500
# The idle tick fires roughly once a second whenever the lifecycle topic is
# quiet. Polling every thread on every tick made 493 conversations.replies calls
# in three minutes and Slack rate-limited us; a human typing a follow-up will
# happily wait 30 seconds for an answer.
POLL_INTERVAL_S = 30.0
RATE_LIMIT_BACKOFF_S = 60.0
# Threads stop being conversational quickly; polling a week of them forever is
# what makes the call volume grow without bound.
THREAD_WINDOW_HOURS = 24
ABSTAIN_REPLY = ("I don't have enough relevant indexed evidence to answer that "
                 "confidently.")

_OPEN_THREADS_SQL = (
    "SELECT incident_id, slack_ts, thread_seen_ts,"
    " coalesce(slack_channel_id, %s) FROM incidents"
    " WHERE slack_ts IS NOT NULL AND brief_delivered_at IS NOT NULL"
    f"  AND brief_delivered_at > now() - interval '{THREAD_WINDOW_HOURS} hours'"
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
    from freshet.rag.retrieval import hybrid_search

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


class ThreadPoller:
    """Throttled, rate-limit-aware wrapper around poll_threads.

    Slack answers 429 with a Retry-After; ignoring it just earns more 429s, so a
    rate-limited poll backs the whole loop off rather than retrying per thread.
    """

    def __init__(self, interval_s: float = POLL_INTERVAL_S, now=time.monotonic) -> None:
        self._interval = interval_s
        self._now = now
        self._next_at = -1e9

    def due(self) -> bool:
        return self._now() >= self._next_at

    def __call__(self, conn, embedder, composer, client, channel: str) -> int:
        if not self.due():
            return 0
        self._next_at = self._now() + self._interval
        try:
            return poll_threads(conn, embedder, composer, client, channel)
        except RateLimited:
            log.warning("slack rate-limited the thread poll; backing off %.0fs",
                        RATE_LIMIT_BACKOFF_S)
            self._next_at = self._now() + RATE_LIMIT_BACKOFF_S
            return 0


class RateLimited(Exception):
    """Slack asked us to slow down."""


def _is_rate_limit(exc: Exception) -> bool:
    return "ratelimited" in str(exc) or "429" in str(exc)


def poll_threads(conn, embedder, composer, client, channel: str, *,
                 sink: Sink | None = None, limit: int = 20) -> int:
    """Answer new replies in threads we have briefed. Returns replies posted."""
    posted = 0
    for incident_id, thread_ts, seen_ts, thread_channel in conn.execute(
            _OPEN_THREADS_SQL, (channel, limit)).fetchall():
        try:
            # the stored ID, not the #name: conversations.replies rejects names
            resp = client.conversations_replies(channel=thread_channel, ts=thread_ts,
                                                oldest=seen_ts or thread_ts)
            messages = list(resp["messages"] or [])
        except Exception as exc:
            if _is_rate_limit(exc):
                raise RateLimited(str(exc)) from exc   # back the whole loop off
            # One bad thread must not stop the rest — but log it once, not per tick.
            log.warning("thread %s unreadable: %r", thread_ts, exc)
            continue

        newest = seen_ts
        for message in messages:
            if not is_human_reply(message, thread_ts):
                continue
            if seen_ts and message["ts"] <= seen_ts:
                continue                        # already answered
            try:
                answer = answer_question(conn, embedder, composer, message["text"])
            except BudgetExhausted as exc:
                # Do NOT advance thread_seen_ts: the question stays unanswered and
                # is picked up once the budget frees, rather than lost.
                log.warning("thread question deferred: %s", exc)
                return posted
            client.chat_postMessage(channel=thread_channel, thread_ts=thread_ts,
                                    text=answer)
            posted += 1
            newest = message["ts"] if not newest else max(newest, message["ts"])
        if newest and newest != seen_ts:
            conn.execute(_MARK_SEEN_SQL, (newest, incident_id))
    return posted
