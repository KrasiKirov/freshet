"""The ingest seam.

`IncidentSource` is the only thing the poller depends on, so the entire ingest
path can be exercised with a fixture implementation and zero network. Adding a
new provider means writing one adapter and changing nothing else.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class IncidentUpdate:
    """One update posted to one incident by one provider."""

    provider: str
    incident_id: str
    update_id: str
    created_at: datetime
    status: str
    text: str
    incident_name: str

    @property
    def dedup_key(self) -> str:
        """Stable identity. Polling re-delivers the same update endlessly, so this
        is what the Flink job keys on to emit each update exactly once."""
        return f"{self.provider}:{self.incident_id}:{self.update_id}"


@runtime_checkable
class IncidentSource(Protocol):
    name: str

    def fetch(self) -> list[IncidentUpdate]:
        """Return the updates currently visible from this source."""
        ...
