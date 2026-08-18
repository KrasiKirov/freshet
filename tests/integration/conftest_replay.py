"""Seed a Postgres+pgvector corpus from the committed real-incident replay
fixture, so integration tests run against real status-feed language without a
network call.

Events are built in exactly the shape the Flink SQL job emits
(freshet/stream/dedup_job.sql), so a test that passes here is exercising the same
records the live pipeline produces.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from freshet.common.schemas import Event, EventSource
from freshet.pipeline.embedder import records_for_event, upsert_record

FIXTURE = Path("freshet/eval/fixtures/replay.jsonl")


def load_replay(limit: int | None = None) -> list[dict]:
    rows = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
    return rows[-limit:] if limit else rows


def event_from_update(row: dict) -> Event:
    """Same mapping the stream job performs, kept in one place so the tests and
    the pipeline cannot drift apart."""
    ev = Event(
        ts=datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")).astimezone(UTC),
        incident_id=row["incident_id"],
        service=row["provider"],
        source=EventSource.ALERT,
        type="status_update",
        text=f'{row["incident_name"]}: {row["text"]}',
    )
    ev.event_id = f'{row["provider"]}:{row["incident_id"]}:{row["update_id"]}'
    return ev


def seed_from_replay(conn, embedder, limit: int = 300) -> list[dict]:
    """Index the most recent `limit` real updates. Returns the rows indexed so a
    test can pick a real incident to ask about."""
    conn.execute("DELETE FROM vector_records")
    rows = load_replay(limit)
    for row in rows:
        for rec in records_for_event(event_from_update(row)):
            [vec] = embedder.encode([rec.text])
            upsert_record(conn, rec, vec)
    conn.commit()
    return rows
