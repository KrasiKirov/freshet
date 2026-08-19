"""Autopilot: watch incident.lifecycle and print a cited brief per new incident.

    python -m freshet.autopilot --brokers localhost:9092

Generation is not optional: ANTHROPIC_API_KEY is REQUIRED (make autopilot sources
.env.local). Briefs and postmortems are LLM-composed over the incident's own
indexed updates, and every citation the model emits is verified against that
evidence before the brief is rendered."""

from __future__ import annotations

import argparse
import os
import signal
import threading

from freshet.api.composer import make_composer
from freshet.autopilot.consumer import drain_due_briefs, handle_and_drain
from freshet.autopilot.sinks.factory import make_sink
from freshet.autopilot.thread_agent import poll_threads
from freshet.common.db import connect
from freshet.common.kafka_io import consume_loop
from freshet.pipeline.embedding import make_embedder
from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC


def _handle(conn, raw: str, window_s: float, sink, embedder) -> None:
    """consume_loop wants a None-returning handler; the count is only for tests."""
    handle_and_drain(conn, raw, window_s=window_s, sink=sink, embedder=embedder)


def _idle(conn, sink, embedder, threads) -> None:
    """Idle work: deliver anything due, then answer new Slack thread replies."""
    drain_due_briefs(conn, sink=sink, embedder=embedder)
    if threads is not None:
        threads()


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet autopilot (incident.lifecycle -> briefs)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default=os.environ.get("AUTOPILOT_GROUP", "autopilot"))
    p.add_argument("--window-s", type=float,
                   default=float(os.environ.get("AUTOPILOT_WINDOW_S", "45")))
    p.add_argument("--max-messages", type=int, default=None)
    p.add_argument("--sink", default=os.environ.get("FRESHET_SINK", "stdout"),
                   choices=["stdout", "slack", "slack-dry-run"])
    args = p.parse_args()

    conn = connect()
    # Retrieval is needed for two things now: recurrence in the brief, and
    # answering follow-up questions in the Slack thread.
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    composer = make_composer()
    sink = make_sink(args.sink)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit(
            "[autopilot] ANTHROPIC_API_KEY is required: briefs are LLM-composed. "
            "Set it in .env.local (make autopilot sources it).")
    # Thread replies are POLLED with the bot token already in use: no app-level
    # token, no public endpoint, and the project is already a polled pipeline.
    threads = None
    if args.sink == "slack" and os.environ.get("SLACK_BOT_TOKEN"):
        from slack_sdk import WebClient

        client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
        channel = os.environ.get("SLACK_CHANNEL", "#general")

        def threads() -> None:      # noqa: F811 — bound only when Slack is configured
            poll_threads(conn, embedder, composer, client, channel)

    print(f"[autopilot] listening on {LIFECYCLE_TOPIC} "
          f"(window={args.window_s}s, briefs=LLM-composed, citations verified)")

    try:
        consume_loop(
            args.brokers, args.group, [LIFECYCLE_TOPIC],
            # handle_and_drain, not handle_lifecycle: a due brief must not wait
            # for an idle poll that a busy partition never produces.
            lambda v: _handle(conn, v, args.window_s, sink, embedder),
            max_messages=args.max_messages, auto_commit=False, stop=stop,
            # Briefs are delivered here, not on the message path: the debounce is
            # a due-time in Postgres, so offsets commit while it elapses.
            idle_hook=lambda: _idle(conn, sink, embedder, threads),
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
