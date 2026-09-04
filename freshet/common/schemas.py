"""Canonical data contract for the Freshet incident-intelligence pipeline.

Everything downstream (ingest, embedder, query layer, eval) depends on these
schemas, so they are deliberately small and explicit. The three timestamps on
``Event`` (``ts``, ``ingested_at``, ``indexed_at``) are the basis for every
freshness metric in the eval harness — do not remove them.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _new_id(prefix: str = "evt") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


log = logging.getLogger(__name__)
_WARNED: set[str] = set()


def _to_utc(value: datetime) -> datetime:
    """Attach UTC to a naive timestamp.

    Postgres stores these in `timestamptz`, which interprets a naive value in
    the SESSION time zone — so a message arriving without an offset silently
    shifts every freshness metric by that offset, in the one project whose whole
    claim is measuring freshness. Coercing rather than REJECTING is deliberate:
    rejection would dead-letter legacy messages still retained on the topic and
    break replay, which is a worse outcome than assuming the UTC the pipeline
    already writes everywhere else. Warned once per process so drift is visible.
    """
    if value.tzinfo is not None:
        return value
    if "naive" not in _WARNED:
        _WARNED.add("naive")
        log.warning("timestamp arrived without a timezone; assuming UTC")
    return value.replace(tzinfo=UTC)


def _as_utc(value: datetime | None) -> datetime | None:
    """Optional variant: Event's pipeline timestamps are absent until they are set."""
    return None if value is None else _to_utc(value)


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


# Open vocabulary — Event.type is a plain str, so this enum names what THIS
# pipeline emits, not what it accepts. The two dozen other v1 members
# (error_spike, deploy_started, rollback, ...) were the synthetic-generator's
# vocabulary — no producer here ever wrote one. CHANGE_TYPES/REMEDIATION_TYPES
# went with them (they classified those types for a correlator that's gone).
class EventType(str, Enum):
    STATUS_UPDATE = "status_update"   # every live status-feed update
    RCA = "rca"                       # root-cause analysis / postmortem


class Event(BaseModel):
    """A single normalized operational event flowing through the pipeline."""

    event_id: str = Field(default_factory=lambda: _new_id("evt"))

    # Wire-format version, carried from raw.incidents through the Flink projection.
    # Defaults to 1 for unversioned messages still on the topic, so replay parses instead of dead-lettering.
    v: int = 1

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
    # before the field existed fall back to the embedder deriving it from `text`.
    title: str | None = None

    text: str = ""
    structured: dict[str, Any] = Field(default_factory=dict)
    refs: list[str] = Field(default_factory=list)

    @field_validator("ts", "ingested_at", "indexed_at")
    @classmethod
    def _utc_event(cls, v: datetime | None) -> datetime | None:
        return _as_utc(v)

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

    # No default: the id is always "chk_<event_id>_<chunk_index>" — a factory
    # producing any other shape let the ordinal-parsing queries drift from the writer.
    chunk_id: str
    chunk_index: int = 0
    event_id: str
    incident_id: str | None = None
    service: str
    ts: datetime
    indexed_at: datetime = Field(default_factory=_utcnow)
    text: str
    # Held separately from the chunk: Flink emits "<name>: <text>" as one
    # string, so only the FIRST chunk carries the title — later chunks are
    # mid-sentence fragments (once produced "what is happening with the are
    # monitoring for continued stability.?").
    title: str | None = None
    source: EventSource
    severity: Severity | None = None
    type: str = ""

    @field_validator("ts", "indexed_at")
    @classmethod
    def _utc_record(cls, v: datetime) -> datetime:
        return _to_utc(v)
