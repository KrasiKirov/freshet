"""Incident lifecycle events: emitted when an incident actually
transitions (opens or resolves); the autopilot consumer reads them. Kept tiny and
self-contained — the consumer re-reads full detail from Postgres when it acts."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

LIFECYCLE_TOPIC = "incident.lifecycle"

# type isn't a Literal: an unfamiliar value must print "no action", not crash a replay
KNOWN_TYPES = frozenset({"opened", "resolved"})


class LifecycleEvent(BaseModel):
    type: str          # "opened" | "resolved"; see KNOWN_TYPES
    incident_id: str
    service: str
    ts: str            # ISO-8601, passed through to Postgres unparsed
    # carried so Autopilot can create a titled incidents row before the embedder does
    title: str = ""

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> LifecycleEvent:
        d: dict[str, Any] = json.loads(raw)
        # `status` is the legacy field name; accept both so old records don't poison-pill
        if "type" not in d and "status" in d:
            d["type"] = d.pop("status")
        if "type" not in d:
            raise KeyError("lifecycle event has neither 'type' nor 'status'")
        d["title"] = d.get("title") or ""
        return cls.model_validate(d)
