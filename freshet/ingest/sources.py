"""The wire model for one status-feed update.

The poller produces these, the Flink job dedups them by `dedup_key`, and the
embedder indexes them. A provider adapter (see `statuspage.py`) is just a pure
function producing this type, so ingest is testable with no network.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


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

