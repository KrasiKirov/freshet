"""Repeatable stdout rehearsal over a captured incident in session-local tables.

Exercises indexing, lifecycle handling and rendering without current feeds or
Kafka. By default, quotes evidence with stub vectors; --llm uses the real
budgeted composer. Neither mode sends Slack messages or modifies live rows.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql

from freshet.autopilot.consumer import handle_and_drain
from freshet.autopilot.sinks.stdout import StdoutSink
from freshet.common.db import DEFAULT_DSN
from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import make_handler
from freshet.pipeline.embedding import StubEmbedder
from freshet.pipeline.lifecycle import LifecycleEvent
from freshet.rag.budget import BudgetedComposer
from freshet.rag.composer import verify_citations

FIXTURE = Path(__file__).resolve().parents[1] / "eval/fixtures/replay.jsonl"
INCIDENT = "01KZRE6ZA1RE1ZH4HQ111CX1S8"
TABLES = (
    "vector_records", "event_indexing", "incidents", "incident_services",
    "incident_events", "llm_budget", "pipeline_heartbeat", "pipeline_heartbeat_log",
    "index_stats",
)


class FixtureComposer:
    """Explicit extractive stand-in for a deterministic, keyless rehearsal."""

    def compose(self, question, hits):
        latest = max(hits, key=lambda h: (h.ts, h.event_id))
        return verify_citations(f'Provider update: "{latest.text}" [{latest.event_id}]', hits)


class _NoDeadletters:
    def produce(self, *args, **kwargs):
        raise RuntimeError("The committed demo fixture could not be indexed")


def isolate(conn) -> None:
    """Shadow every replay table, leaving public tables untouched.

    Use plain psycopg: reconnecting would lose the temporary tables and could
    fall through to public ones. Closing the session removes all replay state.
    """
    for table in TABLES:
        conn.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE public.{} INCLUDING ALL)").format(
            sql.Identifier(table), sql.Identifier(table)))
    conn.execute("SET search_path TO pg_temp, public")


def replay(conn, *, composer=None, sink=None) -> int:
    """Replay only after isolate(); source timestamps and text stay unchanged."""
    for table in TABLES:
        row = conn.execute(
            "SELECT c.relpersistence FROM pg_class c WHERE c.oid = to_regclass(%s)",
            (table,)).fetchone()
        if row is None or row[0] != "t":
            raise RuntimeError("Replay requires session-local temporary tables")
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
    rows = sorted((r for r in rows if r["provider"] == "openai"
                   and r["incident_id"] == INCIDENT), key=lambda r: r["created_at"])
    if not rows or not {"monitoring", "resolved"} <= {r["status"] for r in rows}:
        raise RuntimeError("Captured incident is missing its lifecycle")
    composer = composer or FixtureComposer()
    sink = sink or StdoutSink()
    index = make_handler(conn, StubEmbedder(), _NoDeadletters())
    emitted: set[str] = set()
    for row in rows:
        stamp = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
        iid = f"{row['provider']}:{row['incident_id']}"
        event = Event(
            event_id=f"{iid}:{row['update_id']}", incident_id=iid,
            service=row["provider"], source=EventSource.ALERT, type="status_update",
            ts=stamp, text=f"{row['incident_name']}: {row['text']}", title=row["incident_name"],
        )
        index(event.model_dump_json())
        transition = ("resolved" if row["status"] == "resolved" else
                      "in_progress" if row["status"] == "monitoring" else "opened")
        if transition in emitted:
            continue
        emitted.add(transition)
        # Only the trigger is current to pass the old-alert guard; the already
        # indexed incident retains its provider timestamp for opened_at.
        trigger_ts = datetime.now(UTC) if transition == "opened" else stamp
        lifecycle = LifecycleEvent(type=transition, incident_id=iid,
                                   service=row["provider"], ts=trigger_ts.isoformat(),
                                   title=row["incident_name"])
        handle_and_drain(conn, lifecycle.to_json(), window_s=0, sink=sink, composer=composer)
    return len(emitted)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llm", action="store_true", help="use Anthropic (requires key; billed)")
    args = parser.parse_args()
    print("CAPTURED INCIDENT REPLAY — OpenAI, 2026-08-11")
    print("Session-local database tables; stdout only; Kafka/Flink and live feeds are bypassed.")
    print("Stub vectors; " + ("live LLM summaries (billed)." if args.llm else
                             "deterministic provider quotations, no LLM calls."))
    with psycopg.connect(os.environ.get("FRESHET_DSN", DEFAULT_DSN), autocommit=True) as conn:
        isolate(conn)
        composer = None
        if args.llm:
            from freshet.rag.composer import make_composer
            composer = BudgetedComposer(make_composer(), conn)
        count = replay(conn, composer=composer)
        print(f"Replay complete: {count} lifecycle stages. Temporary state is discarded on exit.")


if __name__ == "__main__":
    main()
