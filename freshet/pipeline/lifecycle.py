"""Incident lifecycle events: the normalizer emits one when an incident actually
transitions (opens or resolves); the autopilot consumer reads them. Kept tiny and
self-contained — the consumer re-reads full detail from Postgres when it acts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

LIFECYCLE_TOPIC = "incident.lifecycle"


@dataclass
class LifecycleEvent:
    type: str          # "opened" | "resolved"
    incident_id: str
    service: str
    ts: str            # ISO-8601

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> "LifecycleEvent":
        d = json.loads(raw)
        return cls(type=d["type"], incident_id=d["incident_id"],
                   service=d["service"], ts=d["ts"])
