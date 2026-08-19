"""Incident lifecycle events: emitted when an incident actually
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
    # Flink already emits this; carried so Autopilot can create a titled
    # incidents row when a lifecycle event arrives before the embedder.
    title: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> LifecycleEvent:
        d = json.loads(raw)
        # `status` is the legacy field name: the Flink sink emitted it before the
        # column was renamed to `type`, and those records are still on the topic.
        # Accepting both keeps a replay of retained history from poison-pilling
        # the consumer on its first message.
        kind = d.get("type", d.get("status"))
        if kind is None:
            raise KeyError("lifecycle event has neither 'type' nor 'status'")
        return cls(type=kind, incident_id=d["incident_id"],
                   service=d["service"], ts=d["ts"], title=d.get("title") or "")
