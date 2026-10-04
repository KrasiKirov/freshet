"""Gather one incident's own evidence for the brief: its indexed updates, a
cause quoted from them when the provider states one, and an LLM summary."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from freshet.autopilot.brief import Findings, cause_from_updates, update_lines
from freshet.autopilot.impact import estimate_impact
from freshet.rag.budget import BudgetExhausted
from freshet.rag.composer import Cited

log = logging.getLogger(__name__)


_RUNBOOK_SQL = ("SELECT text FROM vector_records WHERE service = %s AND type = 'runbook'"
                " ORDER BY ts LIMIT 1")
_INCIDENT_META_SQL = ("SELECT opened_at, resolved_at, brief_as_of"
                      " FROM incidents WHERE incident_id = %s")
_INCIDENT_SERVICES_SQL = "SELECT service FROM incident_services WHERE incident_id = %s"
# concatenates chunk rows per event_id so a cause split across chunks isn't hidden
_INCIDENT_UPDATES_SQL = (
    "SELECT event_id,"
    "       max(ts) AS ts,"
    "       string_agg(text, ' ' ORDER BY chunk_index) AS text,"
    "       min(service) AS service, min(type) AS type, min(source) AS source"
    " FROM vector_records"
    " WHERE incident_id = %s")


@dataclass(frozen=True)
class _Update:
    """One incident update, fetched by key. Satisfies `composer.Cited`, which is
    what the composer actually requires — `compose` used to be annotated
    `list[RetrievedHit]` while receiving these, a mismatch mypy could not see
    because the shape only duck-typed it."""

    event_id: str
    ts: datetime
    text: str
    service: str
    type: str
    source: str = "alert"


def fetch_incident_updates(conn, incident_id: str, as_of: datetime | None = None) -> list[_Update]:
    """Every indexed update belonging to one incident. Deduplicated by event_id
    because a long update chunks into several rows. ``as_of`` is used for a
    delayed opening brief: if resolution arrived before the debounce drained,
    the opening message must not summarize the later resolved history."""
    sql = _INCIDENT_UPDATES_SQL
    params: list[object] = [incident_id]
    if as_of is not None:
        sql += " AND ts <= %s"
        params.append(as_of)
    sql += " GROUP BY event_id ORDER BY max(ts) DESC"
    rows = conn.execute(sql, params).fetchall()
    return [_Update(event_id=r[0], ts=r[1], text=r[2], service=r[3],
                    type=r[4], source=r[5]) for r in rows]


def fetch_runbook(conn, service: str) -> str | None:
    row = conn.execute(_RUNBOOK_SQL, (service,)).fetchone()
    if not row or not row[0]:
        return None
    value = row[0].strip()
    # Some replay fixtures carry a literal placeholder as a runbook row. It
    # is not useful context and should not leak into Slack as if it were one.
    if value.lower() in {"none", "none found", "no runbook found", "n/a"}:
        return None
    return value


def _impact_for(conn, incident_id: str, service: str, hits) -> str:
    row = conn.execute(_INCIDENT_META_SQL, (incident_id,)).fetchone()
    opened_at, resolved_at = row[:2] if row else (None, None)
    services = [r[0] for r in conn.execute(_INCIDENT_SERVICES_SQL, (incident_id,)).fetchall()]
    if not services:
        services = [service]
    return estimate_impact(services, opened_at, resolved_at, [h.text for h in hits])


MAX_NARRATIVE_UPDATES = 20


def _summarise(updates: Sequence[Cited], composer, question: str) -> str | None:
    """One narrative path for briefs and postmortems alike. Both used to have
    their own: the postmortem's bypassed citation verification entirely, so it
    could ship a fabricated citation that a brief never could.

    A missing composer is a programming error, not a cue to build one. The old
    `composer or make_composer()` fallback silently produced an UNBUDGETED
    composer whenever a caller forgot to forward its own — which every caller on
    the brief path did, so briefs and postmortems were never counted or capped.
    """
    if composer is None:
        raise ValueError(
            "gather_findings/gather_postmortem need an explicit composer: "
            "constructing one here bypasses the LLM budget. Pass the "
            "BudgetedComposer the entrypoint built.")
    if not updates:
        return None
    updates = list(updates)[:MAX_NARRATIVE_UPDATES]   # newest first
    try:
        return composer.compose(question, updates)
    except BudgetExhausted:
        # pause, not failure: caller keeps brief_due_at and retries next window
        raise
    except Exception as exc:          # never let generation break an alert
        log.warning("summary generation failed (%r); rendering without it", exc)
        return None


def _gather(conn, service: str, incident_id: str, status: str, question: str,
            *, composer, embedder=None, meta: str | None = None,
            as_of: datetime | None = None) -> Findings:
    """Everything a brief or a postmortem needs, from this incident alone.

    EVERY input is scoped to this incident. A service-wide similarity search
    used to feed the timeline and the impact heuristic, so a provider with
    several open incidents could have another incident's error percentages
    folded into this one's impact line. An incident's events are a known set —
    look them up rather than search for them.
    """
    own = fetch_incident_updates(conn, incident_id, as_of=as_of)
    f = Findings(service=service, status=status, cause_text=None, cause_cite=None,
                 fix_text=None, fix_cite=None, runbook=fetch_runbook(conn, service),
                 narrative=None, meta=meta)
    f.updates = update_lines(own)
    stated = cause_from_updates(own)
    if stated:
        f.cause_text, f.cause_cite = stated
    # narrative summarises; cause_text stays a verbatim provider quote
    f.narrative = _summarise(own, composer, question)
    f.impact = _impact_for(conn, incident_id, service, own)
    # recurrence is the only field resolved by retrieval rather than key lookup
    if embedder is not None and own:
        f.recurrence = _recurrence_for(conn, embedder, service, incident_id, own)
    return f


def gather_findings(conn, service: str, incident_id: str, status: str,
                    *, composer=None, embedder=None) -> Findings:
    # A replay can deliver an `opened` event after its matching `resolved`
    # event. Keep the root brief truthful to the opening state; the postmortem
    # path below intentionally reads the complete incident history.
    row = conn.execute(_INCIDENT_META_SQL, (incident_id,)).fetchone()
    # ``brief_as_of`` is set only by the on-demand demo trigger. In normal live
    # operation an unresolved brief sees the current incident history; a replayed
    # resolved incident is still cut off at its opening timestamp.
    demo_cutoff = row[2] if row and len(row) > 2 else None
    as_of = (demo_cutoff if status == "open" and demo_cutoff is not None
             else row[0] if row and row[1] is not None else None)
    return _gather(conn, service, incident_id, status,
                   "Summarise the incident described by these provider updates in two "
                   "sentences. Name the affected product, region, and current state "
                   "exactly as supported by the updates. Treat the provider label as "
                   "metadata; do not reject the evidence because the product name "
                   "differs from that label. Do not add meta-commentary about missing "
                   "fields such as region unless it materially changes the incident "
                   "summary.",
                   composer=composer, embedder=embedder, as_of=as_of)


def _recurrence_for(conn, embedder, service: str, incident_id: str, own) -> str | None:
    from freshet.autopilot.recurrence import find_recurrences, recurrence_line

    earliest = min(own, key=lambda u: u.ts)
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
    if secs < 0:
        return None
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
    summary = resolution_summary or "resolved"
    return _gather(conn, service, incident_id, "resolved",
                   "The incident lifecycle state is resolved. Summarise the resolved "
                   "incident described by these provider updates in two sentences. "
                   "Name the affected product and state exactly as supported by the "
                   "updates, and do not invent resolution details. If the updates do "
                   "not describe what completed, say that the incident was marked "
                   "resolved without additional provider detail. Do not repeat the "
                   "same citation for the same claim.",
                   composer=composer, embedder=embedder,
                   meta=f"Duration {duration} · {summary}" if duration else summary)
