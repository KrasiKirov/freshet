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

from freshet.autopilot.consumer import handle_lifecycle
from freshet.autopilot.sinks.factory import make_sink
from freshet.common.db import connect
from freshet.common.kafka_io import consume_loop
from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC


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
    sink = make_sink(args.sink)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit(
            "[autopilot] ANTHROPIC_API_KEY is required: briefs are LLM-composed. "
            "Set it in .env.local (make autopilot sources it).")
    print(f"[autopilot] listening on {LIFECYCLE_TOPIC} "
          f"(window={args.window_s}s, briefs=LLM-composed, citations verified)")

    try:
        consume_loop(
            args.brokers, args.group, [LIFECYCLE_TOPIC],
            lambda v: handle_lifecycle(conn, v, window_s=args.window_s, sink=sink),
            max_messages=args.max_messages, auto_commit=False, stop=stop,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
