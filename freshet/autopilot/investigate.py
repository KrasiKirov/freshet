"""Gather one incident's own evidence for the brief: its indexed updates, a
cause quoted from them when the provider states one, and an LLM summary."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from freshet.autopilot.brief import (
    Findings,
    cause_from_updates,
    findings_from_updates,
)
from freshet.autopilot.impact import estimate_impact

log = logging.getLogger(__name__)


_RUNBOOK_SQL = ("SELECT text FROM vector_records WHERE service = %s AND type = 'runbook'"
                " ORDER BY ts LIMIT 1")
_INCIDENT_META_SQL = "SELECT opened_at, resolved_at FROM incidents WHERE incident_id = %s"
_INCIDENT_SERVICES_SQL = "SELECT service FROM incident_services WHERE incident_id = %s"
# The brief's update timeline is a DIRECT lookup, not a similarity search:
# an incident's updates are a known, complete set, and retrieval filters only by
# service — so a search would happily cite the provider's OTHER incidents.
# Long updates are chunked, so DISTINCT ON (event_id) returned ONE chunk and the
# brief never saw the rest — a cause stated in chunk 1 was invisible. Reassemble
# the update by concatenating its chunks in order. The index is cast to int
# because chunk_id sorts lexically otherwise, putting `_10` before `_2`.
_INCIDENT_UPDATES_SQL = (
    "SELECT event_id,"
    "       max(ts) AS ts,"
    "       string_agg(text, ' ' ORDER BY (regexp_match(chunk_id, '_(\\d+)$'))[1]::int)"
    "         AS text,"
    "       min(service) AS service, min(type) AS type, min(source) AS source"
    " FROM vector_records"
    " WHERE incident_id = %s"
    " GROUP BY event_id"
    " ORDER BY max(ts) DESC")


@dataclass(frozen=True)
class _Update:
    """Minimal shape `cite_hit`, `findings_from_updates` and the composer need."""

    event_id: str
    ts: datetime
    text: str
    service: str
    type: str
    source: str = "alert"


def fetch_incident_updates(conn, incident_id: str) -> list[_Update]:
    """Every indexed update belonging to one incident. Deduplicated by event_id
    because a long update chunks into several rows."""
    rows = conn.execute(_INCIDENT_UPDATES_SQL, (incident_id,)).fetchall()
    return [_Update(event_id=r[0], ts=r[1], text=r[2], service=r[3],
                    type=r[4], source=r[5]) for r in rows]


def fetch_runbook(conn, service: str) -> str | None:
    row = conn.execute(_RUNBOOK_SQL, (service,)).fetchone()
    return row[0] if row else None


def _impact_for(conn, incident_id: str, service: str, hits) -> str:
    row = conn.execute(_INCIDENT_META_SQL, (incident_id,)).fetchone()
    opened_at, resolved_at = row if row else (None, None)
    services = [r[0] for r in conn.execute(_INCIDENT_SERVICES_SQL, (incident_id,)).fetchall()]
    if not services:
        services = [service]
    return estimate_impact(services, opened_at, resolved_at, [h.text for h in hits])


def _summarise(updates, service: str, composer, question: str) -> str | None:
    """One narrative path for briefs and postmortems alike. Both used to have
    their own: the postmortem's bypassed citation verification entirely, so it
    could ship a fabricated citation that a brief never could."""
    if not updates:
        return None
    from freshet.api.composer import make_composer
    composer = composer or make_composer()
    try:
        return composer.compose(question, updates)
    except Exception as exc:          # never let generation break an alert
        log.warning("summary generation failed (%r); rendering without it", exc)
        return None


def gather_findings(conn, service: str, incident_id: str, status: str,
                    *, composer=None, embedder=None) -> Findings:
    runbook = fetch_runbook(conn, service)
    # EVERY input is scoped to this incident. A service-wide similarity search
    # used to feed the timeline and the impact heuristic, so a provider with
    # several open incidents could have another incident's error percentages
    # folded into this one's impact line. An incident's events are a known set —
    # look them up rather than search for them.
    own = fetch_incident_updates(conn, incident_id)
    f = Findings(service=service, status=status, cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=runbook, narrative=None)
    # Cause/fix is kept for corpora that contain change events; the update
    # timeline is ADDED, not substituted, because status feeds have none. It is
    # sourced by direct lookup so the brief cannot cite a different incident.
    f.updates = findings_from_updates(service, status, own, runbook).updates
    # Change events give the strongest cause, but status feeds have none. Fall
    # back to the provider's own words IF an update actually states a cause.
    if not f.cause_text:
        stated = cause_from_updates(own)
        if stated:
            f.cause_text, f.cause_cite = stated
    # Generation: the "G" in RAG, and the default path. The composer grounds a
    # short summary in this incident's own updates and every citation it emits is
    # verified against them. It summarises only — the Cause line stays a verbatim
    # provider quote, so the model never gets to diagnose.
    f.narrative = _summarise(own, service, composer,
                             f"What is happening with {service}? "
                             "Summarise in two sentences.")
    f.impact = _impact_for(conn, incident_id, service, own)
    # Recurrence is the only input that is NOT addressable by key: which past
    # incident resembles this one has no primary key, so it goes through the
    # retrieval path the eval measures. Optional — no embedder, no claim.
    if embedder is not None and own:
        f.recurrence = _recurrence_for(conn, embedder, service, incident_id, own)
    return f


def _recurrence_for(conn, embedder, service: str, incident_id: str, own) -> str | None:
    from freshet.autopilot.recurrence import find_recurrences, recurrence_line

    earliest = min(own, key=lambda u: u.ts)
    # Query with the provider's own symptom text, exactly as the eval does.
    matches = find_recurrences(conn, embedder, service=service,
                               incident_id=incident_id, query_text=earliest.text,
                               before=earliest.ts)
    return recurrence_line(matches)


_INCIDENT_ROW_SQL = ("SELECT opened_at, resolved_at, resolution_summary"
                     " FROM incidents WHERE incident_id = %s")


def _format_duration(opened_at, resolved_at) -> str | None:
    if not opened_at or not resolved_at:
        return None
    secs = int((resolved_at - opened_at).total_seconds())
    if secs < 60:
        return f"{secs}s"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    return f"{mins // 60}h {mins % 60}m"


def gather_postmortem(conn, service: str, incident_id: str,
                      *, composer=None, embedder=None) -> Findings:
    row = conn.execute(_INCIDENT_ROW_SQL, (incident_id,)).fetchone()
    opened_at, resolved_at, resolution_summary = row if row else (None, None, None)
    duration = _format_duration(opened_at, resolved_at)

    own = fetch_incident_updates(conn, incident_id)
    narrative = _summarise(own, service, composer,
                           f"Summarise the resolved {service} incident in two sentences.")
    runbook = fetch_runbook(conn, service)
    summary = resolution_summary or "resolved"
    meta = f"Duration {duration} · {summary}" if duration else summary
    f = Findings(service=service, status="resolved", cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=runbook, narrative=narrative, meta=meta)
    stated = cause_from_updates(own)
    if stated:
        f.cause_text, f.cause_cite = stated
    f.updates = findings_from_updates(service, "resolved", own, runbook).updates
    f.impact = _impact_for(conn, incident_id, service, own)
    if embedder is not None and own:
        f.recurrence = _recurrence_for(conn, embedder, service, incident_id, own)
    return f
