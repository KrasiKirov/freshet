"""Incident lifecycle events: emitted when an incident actually
transitions (opens or resolves); the autopilot consumer reads them. Kept tiny and
self-contained — the consumer re-reads full detail from Postgres when it acts."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

LIFECYCLE_TOPIC = "incident.lifecycle"

# What the consumer acts on. NOT a Literal on the field: an unhandled type must
# print "no action" and move on, the way it does today. Validating it away would
# poison-pill a replay of retained history on the very first unfamiliar message.
KNOWN_TYPES = frozenset({"opened", "resolved"})


class LifecycleEvent(BaseModel):
    type: str          # "opened" | "resolved"; see KNOWN_TYPES
    incident_id: str
    service: str
    ts: str            # ISO-8601, passed through to Postgres unparsed
    # Flink already emits this; carried so Autopilot can create a titled
    # incidents row when a lifecycle event arrives before the embedder.
    title: str = ""

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> LifecycleEvent:
        d: dict[str, Any] = json.loads(raw)
        # `status` is the legacy field name: the Flink sink emitted it before the
        # column was renamed to `type`, and those records are still on the topic.
        # Accepting both keeps a replay of retained history from poison-pilling
        # the consumer on its first message.
        if "type" not in d and "status" in d:
            d["type"] = d.pop("status")
        if "type" not in d:
            # Named explicitly rather than left to pydantic: the wire contract
            # pins this message, and "neither 'type' nor 'status'" tells a
            # reader which legacy shape they are missing.
            raise KeyError("lifecycle event has neither 'type' nor 'status'")
        d["title"] = d.get("title") or ""
        return cls.model_validate(d)
