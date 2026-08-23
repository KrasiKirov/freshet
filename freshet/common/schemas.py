"""Canonical data contract for the Freshet incident-intelligence pipeline.

Everything downstream (ingest, embedder, query layer, eval) depends on these
schemas, so they are deliberately small and explicit. The three timestamps on
``Event`` (``ts``, ``ingested_at``, ``indexed_at``) are the basis for every
freshness metric in the eval harness — do not remove them.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _new_id(prefix: str = "evt") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class EventSource(str, Enum):
    ALERT = "alert"
    DEPLOY = "deploy"
    METRIC = "metric"
    CHAT = "chat"
    POSTMORTEM = "postmortem"
    RUNBOOK = "runbook"


class Severity(str, Enum):
    SEV1 = "SEV1"
    SEV2 = "SEV2"
    SEV3 = "SEV3"
    SEV4 = "SEV4"


# Open vocabulary — `Event.type` is a plain `str`, so this enum names what THIS
# pipeline emits rather than constraining what it accepts. The two dozen other
# members (error_spike, deploy_started, rollback, cert_expired, ...) were v1's
# synthetic-generator vocabulary: no producer in this codebase ever wrote one, and
# nothing outside this module referenced the enum at all. CHANGE_TYPES and
# REMEDIATION_TYPES went with them — they classified those same synthetic types as
# causes and fixes, for a correlator that no longer exists.
class EventType(str, Enum):
    STATUS_UPDATE = "status_update"   # every live status-feed update
    RCA = "rca"                       # root-cause analysis / postmortem


class Event(BaseModel):
    """A single normalized operational event flowing through the pipeline."""

    event_id: str = Field(default_factory=lambda: _new_id("evt"))

    # Wire-format version, carried from raw.incidents through the Flink projection.
    # Defaults to 1 for the unversioned messages still on the topic, so a replay of
    # retained history parses rather than dead-lettering on its first record.
    v: int = 1

    # --- the three timestamps freshness is computed from ---
    ts: datetime = Field(default_factory=_utcnow, description="When the event occurred")
    ingested_at: datetime | None = Field(
        default=None, description="When the pipeline received it"
    )
    indexed_at: datetime | None = Field(
        default=None, description="When it became retrievable"
    )

    service: str
    source: EventSource
    type: str  # usually an EventType value; kept str for an open vocabulary
    severity: Severity | None = None
    incident_id: str | None = None
    # The incident's name, carried from the source feed. Optional: messages
    # produced before the field existed are still on the topic, and the embedder
    # falls back to deriving it from `text` for those.
    title: str | None = None

    text: str = ""
    structured: dict[str, Any] = Field(default_factory=dict)
    refs: list[str] = Field(default_factory=list)

    # --- freshness helpers ---
    def end_to_end_latency_s(self) -> float | None:
        """Seconds from the event happening to becoming queryable."""
        if self.indexed_at is None:
            return None
        return (self.indexed_at - self.ts).total_seconds()

    def pipeline_latency_s(self) -> float | None:
        """Seconds the pipeline itself added (ingest -> indexed)."""
        if self.indexed_at is None or self.ingested_at is None:
            return None
        return (self.indexed_at - self.ingested_at).total_seconds()


class VectorRecord(BaseModel):
    """A retrievable chunk + its metadata (embedding stored in pgvector column)."""

    chunk_id: str = Field(default_factory=lambda: _new_id("chk"))
    event_id: str
    incident_id: str | None = None
    service: str
    ts: datetime
    indexed_at: datetime = Field(default_factory=_utcnow)
    text: str
    # The incident's own title, held separately from the chunk. Flink emits
    # "<incident_name>: <update text>" as one string, so only the FIRST chunk of a
    # long update carries the title — every later chunk is a mid-sentence fragment.
    # Labelling a citation (or building a question) from such a chunk produced
    # "what is happening with the are monitoring for continued stability.?".
    title: str | None = None
    source: EventSource
    severity: Severity | None = None
    type: str = ""
