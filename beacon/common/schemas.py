"""Canonical data contract for the Beacon incident-intelligence pipeline.

Everything downstream (normalizer, embedder, query layer, eval) depends on these
schemas, so they are deliberately small and explicit. The three timestamps on
``Event`` (``ts``, ``ingested_at``, ``indexed_at``) are the basis for every
freshness metric in the eval harness — do not remove them.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_id(prefix: str = "evt") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class EventSource(str, Enum):
    ALERT = "alert"
    DEPLOY = "deploy"
    METRIC = "metric"
    CHAT = "chat"
    POSTMORTEM = "postmortem"


class Severity(str, Enum):
    SEV1 = "SEV1"
    SEV2 = "SEV2"
    SEV3 = "SEV3"
    SEV4 = "SEV4"


# Open vocabulary, but these are the canonical types the generator emits.
class EventType(str, Enum):
    ERROR_SPIKE = "error_spike"
    LATENCY_SPIKE = "latency_spike"
    DEPLOY_STARTED = "deploy_started"
    DEPLOY_FINISHED = "deploy_finished"
    ROLLBACK = "rollback"
    SCALE = "scale"
    METRIC_SAMPLE = "metric_sample"
    MESSAGE = "message"
    RCA = "rca"  # root-cause analysis / postmortem
    HEALTHY = "healthy"


class Event(BaseModel):
    """A single normalized operational event flowing through the pipeline."""

    event_id: str = Field(default_factory=lambda: _new_id("evt"))

    # --- the three timestamps freshness is computed from ---
    ts: datetime = Field(default_factory=_utcnow, description="When the event occurred")
    ingested_at: Optional[datetime] = Field(
        default=None, description="When the pipeline received it"
    )
    indexed_at: Optional[datetime] = Field(
        default=None, description="When it became retrievable"
    )

    service: str
    source: EventSource
    type: str  # usually an EventType value; kept str for an open vocabulary
    severity: Optional[Severity] = None
    incident_id: Optional[str] = None

    text: str = ""
    structured: dict[str, Any] = Field(default_factory=dict)
    refs: list[str] = Field(default_factory=list)

    # --- freshness helpers ---
    def end_to_end_latency_s(self) -> Optional[float]:
        """Seconds from the event happening to becoming queryable."""
        if self.indexed_at is None:
            return None
        return (self.indexed_at - self.ts).total_seconds()

    def pipeline_latency_s(self) -> Optional[float]:
        """Seconds the pipeline itself added (ingest -> indexed)."""
        if self.indexed_at is None or self.ingested_at is None:
            return None
        return (self.indexed_at - self.ingested_at).total_seconds()


class Incident(BaseModel):
    """Assembled incident state (lives in Postgres at runtime)."""

    incident_id: str = Field(default_factory=lambda: _new_id("INC"))
    title: str = ""
    services: list[str] = Field(default_factory=list)
    opened_at: datetime = Field(default_factory=_utcnow)
    resolved_at: Optional[datetime] = None
    event_ids: list[str] = Field(default_factory=list)
    resolution_summary: Optional[str] = None

    @property
    def is_open(self) -> bool:
        return self.resolved_at is None


class VectorRecord(BaseModel):
    """A retrievable chunk + its metadata (embedding stored in pgvector column)."""

    chunk_id: str = Field(default_factory=lambda: _new_id("chk"))
    event_id: str
    incident_id: Optional[str] = None
    service: str
    ts: datetime
    indexed_at: datetime = Field(default_factory=_utcnow)
    text: str
    source: EventSource
