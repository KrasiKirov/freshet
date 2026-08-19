"""Has this happened before? — the one question the brief cannot answer by key.

Every other input to a brief is addressable: the incident's own updates come back
with `WHERE incident_id = %s`, which is correct precisely because the key is
known. Recurrence is different. "Is this the same failure as last month's?" has
no key — the answer is whichever PAST incident is semantically closest, among
~1,200 across 42 providers. That is a retrieval problem, so it uses the retrieval
system the eval actually measures (hybrid dense + full-text, RRF fusion).

Conservative by design: same service only, strictly earlier than this incident,
above the embedder's own abstention floor, at most a handful. A brief that
invents a connection between unrelated outages is worse than one that says
nothing, so the bar to claim recurrence is the same bar the query API uses to
decide it has evidence at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

# Routine maintenance notices are near-identical to each other by construction
# ("SIN (Singapore) on ...: We will be performing scheduled maintenance"), so
# every one of them "recurs" and the line becomes noise. Recurrence is a claim
# about FAILURES repeating; measured on the live corpus this filter is what
# separates the Durable Objects case (genuinely the third occurrence) from six
# datacenter maintenance windows that merely look alike.
_MAINTENANCE_MARKERS = (
    "scheduled maintenance", "will be performing", "planned maintenance",
    "maintenance window", "maintenance is scheduled",
)


def is_maintenance(text: str) -> bool:
    lowered = text.lower()
    return any(m in lowered for m in _MAINTENANCE_MARKERS)


MAX_RECURRENCES = 3
CANDIDATE_K = 25           # chunk-level hits to consider before grouping by incident


@dataclass(frozen=True)
class Recurrence:
    incident_id: str
    title: str
    ts: datetime
    event_id: str
    similarity: float


_INCIDENT_OF_SQL = (
    "SELECT event_id, incident_id, coalesce(title, '') FROM vector_records"
    " WHERE event_id = ANY(%s)")


def _incident_of(conn, event_ids: list[str]) -> dict[str, tuple[str, str]]:
    """event_id -> (incident_id, title). RetrievedHit carries neither."""
    if not event_ids:
        return {}
    return {r[0]: (r[1], r[2])
            for r in conn.execute(_INCIDENT_OF_SQL, (event_ids,)).fetchall()
            if r[1]}


def find_recurrences(conn, embedder, *, service: str, incident_id: str,
                     query_text: str, before: datetime | None,
                     limit: int = MAX_RECURRENCES) -> list[Recurrence]:
    """Prior incidents for this service that read like this one."""
    from freshet.api.retrieval import hybrid_search

    if not query_text.strip() or is_maintenance(query_text):
        return []
    # service-filtered: a Cloudflare outage recurring is a fact about Cloudflare.
    # The filter also puts this on the browse contract, where the caller's filter
    # is the relevance signal rather than the cosine floor.
    result = hybrid_search(conn, embedder, query_text, k=CANDIDATE_K, service=service)
    floor = getattr(embedder, "min_similarity", 0.7)

    hits = [h for h in result.hits if h.similarity >= floor]
    mapping = _incident_of(conn, [h.event_id for h in hits])

    best: dict[str, Recurrence] = {}
    for h in hits:
        found = mapping.get(h.event_id)
        if not found:
            continue
        other_id, title = found
        if other_id == incident_id:
            continue                          # the incident is not its own precedent
        if before is not None and h.ts >= before:
            continue                          # only PRIOR incidents are recurrence
        if is_maintenance(h.text):
            continue                          # a maintenance window is not a failure
        prior = best.get(other_id)
        if prior is None or h.similarity > prior.similarity:
            best[other_id] = Recurrence(incident_id=other_id, title=title, ts=h.ts,
                                        event_id=h.event_id, similarity=h.similarity)
    return sorted(best.values(), key=lambda r: r.similarity, reverse=True)[:limit]


def recurrence_line(matches: list[Recurrence]) -> str | None:
    """One line for the brief, or None when nothing similar came before."""
    if not matches:
        return None
    newest = max(matches, key=lambda r: r.ts)
    times = f"{len(matches)} similar prior incident" + ("s" if len(matches) > 1 else "")
    return (f"Recurrence: {times}, most recent {newest.ts:%Y-%m-%d} — "
            f"{newest.title[:70]} [{newest.event_id} @ {newest.ts:%Y-%m-%d %H:%M:%S}]")
