"""Webhook receiver — the connector framework's HTTP edge. Generic over the
REGISTRY: POST /webhook/{source} verifies the delivery, parses it into canonical
Events via the source's connector, and produces them to raw.events. Untrusted
input: unverified or malformed deliveries are rejected, nothing is produced.

Run:  uvicorn freshet.connectors.webhook:app --port 8088
Config: FRESHET_BROKERS, FRESHET_RAW_TOPIC (default raw.events),
        FRESHET_GITHUB_WEBHOOK_SECRET (verify when set)."""

from __future__ import annotations

import json
import logging
import os

from fastapi import FastAPI, Request, Response

import freshet.connectors.github  # noqa: F401 — registers the GitHub connector
from freshet.common.kafka_io import produce_sync
from freshet.connectors.base import REGISTRY

log = logging.getLogger(__name__)
app = FastAPI(title="Freshet connectors")
_producer = None


def _get_producer():
    global _producer
    if _producer is None:
        from freshet.common.kafka_io import make_producer
        _producer = make_producer(os.environ.get("FRESHET_BROKERS", "localhost:9092"))
    return _producer


@app.post("/webhook/{source}")
async def webhook(source: str, request: Request):
    connector = REGISTRY.get(source)
    if connector is None:
        return Response(status_code=404)
    body = await request.body()
    if not connector.verify(request.headers, body):
        return Response(status_code=401)
    try:
        payload = json.loads(body)
    except ValueError:
        return Response(status_code=400)
    try:
        events = connector.parse(connector.event_type(request.headers), payload)
    except Exception:
        log.exception("connector.parse failed for source=%s", source)
        return Response(status_code=400)
    topic = os.environ.get("FRESHET_RAW_TOPIC", "raw.events")
    producer = _get_producer()
    for ev in events:
        produce_sync(producer, topic, ev.model_dump_json(), key=ev.service)
    return {"produced": len(events)}
