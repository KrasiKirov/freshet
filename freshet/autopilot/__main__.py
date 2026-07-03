"""Autopilot: watch incident.lifecycle and print a cited brief per new incident.

    python -m freshet.autopilot --brokers localhost:9092

Loads ANTHROPIC_API_KEY from the environment (make autopilot sources .env.local);
runs keyless via the extractive timeline when no key is present."""

from __future__ import annotations

import argparse
import os
import signal
import threading

from freshet.common.db import connect
from freshet.common.kafka_io import consume_loop
from freshet.pipeline.embedding import make_embedder
from freshet.pipeline.lifecycle import LIFECYCLE_TOPIC
from freshet.autopilot.consumer import handle_lifecycle
from freshet.autopilot.sinks.stdout import StdoutSink


def main() -> None:
    p = argparse.ArgumentParser(description="Freshet autopilot (incident.lifecycle -> briefs)")
    p.add_argument("--brokers", default="localhost:9092")
    p.add_argument("--group", default=os.environ.get("AUTOPILOT_GROUP", "autopilot"))
    p.add_argument("--window-s", type=float,
                   default=float(os.environ.get("AUTOPILOT_WINDOW_S", "45")))
    p.add_argument("--max-messages", type=int, default=None)
    args = p.parse_args()

    conn = connect()
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    sink = StdoutSink()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    mode = "agent" if os.environ.get("ANTHROPIC_API_KEY") else "keyless timeline"
    print(f"[autopilot] listening on {LIFECYCLE_TOPIC} (window={args.window_s}s, mode={mode})")

    try:
        consume_loop(
            args.brokers, args.group, [LIFECYCLE_TOPIC],
            lambda v: handle_lifecycle(conn, embedder, v, window_s=args.window_s, sink=sink),
            max_messages=args.max_messages, auto_commit=False, stop=stop,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
