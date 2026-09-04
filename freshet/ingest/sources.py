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
    # What the update SAYS, held apart from what it IS. The Flink dedup tuple
    # includes this so an edit re-emits; `event_id` doesn't, so it overwrites the same rows.
    body_digest: str = ""

    @property
    def dedup_key(self) -> str:
        """Stable identity. Polling re-delivers the same update endlessly, so this
        is what the Flink job keys on to emit each update exactly once."""
        return f"{self.provider}:{self.incident_id}:{self.update_id}"

    @property
    def partition_key(self) -> str:
        """Kafka partition key. Ordering holds within a partition only, and the
        lifecycle projection's keep-first semantics need one incident's updates in
        arrival order — so the key is the incident, not the update. Keyed by
        dedup_key instead, a three-partition topic scattered one incident across
        all three and 'the first open' became a race. Matches the namespaced
        incident_id the Flink projection emits."""
        return f"{self.provider}:{self.incident_id}"

