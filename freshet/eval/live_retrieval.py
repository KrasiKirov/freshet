"""Label-free retrieval eval: no labels, no LLM judge, no human review.

Ground truth is the incident id itself, not a curated answer key: the query is
an incident's FIRST update (its earliest `ts`), used verbatim as the query
text, and the correct answer is defined as ANY OTHER update of that SAME
incident. Retrieval already tells us which updates share an incident (the
`incident_id` column vector_records carries), so nothing here is labeled or
judged — the eval regenerates against whatever the live index has accumulated,
including incidents that did not exist when this file was written.

Scope, precisely: this measures WITHIN-INCIDENT LINKING — given the opening
symptom of an incident, can retrieval surface the rest of that incident's
thread out of the whole index? It does NOT measure causal identification (the
retired `retrieval_eval.py`'s claim, backed by hand-labeled cause updates).
A high score here says retrieval can find "more of this same incident"; it
says nothing about whether what it finds actually explains the incident.
Report this eval's numbers as within-incident linking, never as cause-finding.

The query's own document is excluded from the results it is scored against
(`exclude_event_id`, mirrored at the SQL layer and again in `dedupe_events`)
for the same reason the old eval excluded it: the query IS an update's text,
so that update is trivially its own top hit, and scoring it would make top-1
citation structurally 1.000 on every arm regardless of retrieval quality.

Only incidents with >=2 distinct updates are eligible — a one-update incident
has no "other update" to find, so it cannot be scored either way.

Reuses freshet/rag/retrieval.py's real search functions (hybrid_search,
vector_sql, keyword_sql) so this exercises the exact path production queries
through, not a reimplementation. `blind_recent` is the gameability control: a
query-blind rule that just returns the most recent chunks. It must score near
zero — if it does not, the task is solvable by recency alone and the eval is
worthless, which the report says loudly via `gameability_guard.verdict`.

Determinism: candidate incidents are sorted by incident id, and the earliest
update of each is chosen by (ts, event_id) so ties never depend on database
row order. A runtime cap (LIVE_RETRIEVAL_MAX_QUERIES) takes a deterministic
PREFIX of that sorted list, never a random sample, and is recorded in the
artifact so a reader knows whether every eligible incident ran.

Read-only against the LIVE index: this eval must never write to it.

Run: make live-eval
"""

from __future__ import annotations

import json
import os
import pathlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

RESULTS = pathlib.Path("results/live_retrieval.json")
K = 5

# Off by default: every eligible incident runs. Override to bound runtime; the
# artifact records whichever value was used.
MAX_QUERIES_ENV = "LIVE_RETRIEVAL_MAX_QUERIES"


@dataclass(frozen=True)
class Candidate:
    incident_id: str
    query_event_id: str
    other_event_ids: frozenset[str]


def eligible_candidates(
    events_by_incident: dict[str, list[tuple[str, datetime]]],
) -> list[Candidate]:
    """Incidents with >=2 distinct updates, sorted by incident id.

    The query is the incident's earliest update by ts; ties break on event_id
    so the choice never depends on database row order.
    """
    out: list[Candidate] = []
    for incident_id in sorted(events_by_incident):
        updates = events_by_incident[incident_id]
        distinct_ids = {event_id for event_id, _ in updates}
        if len(distinct_ids) < 2:
            continue
        query_event_id, _ = min(updates, key=lambda pair: (pair[1], pair[0]))
        out.append(Candidate(
            incident_id=incident_id,
            query_event_id=query_event_id,
            other_event_ids=frozenset(distinct_ids - {query_event_id}),
        ))
    return out


def capped(candidates: list[Candidate], cap: int | None) -> list[Candidate]:
    """A deterministic PREFIX of `candidates` (already sorted). `cap=None` is
    the identity — every eligible incident runs."""
    return candidates if cap is None else candidates[:cap]


def dedupe_events(hits, exclude: str | None = None) -> list[str]:
    """Hits are chunk-level; rank by first appearance of each event.

    `exclude` drops the query's OWN document — see module docstring.
    """
    seen: list[str] = []
    for h in hits:
        if h.event_id != exclude and h.event_id not in seen:
            seen.append(h.event_id)
    return seen


def score_one(ranked_event_ids: list[str], cause_ids: set[str], k: int = K) -> dict[str, Any]:
    """Score one query's ranked events against the OTHER updates of its incident."""
    rank = next((i + 1 for i, e in enumerate(ranked_event_ids) if e in cause_ids), None)
    return {
        "hit_at_k": rank is not None and rank <= k,
        "mrr": 1.0 / rank if rank else 0.0,
        "top1_cite": bool(ranked_event_ids) and ranked_event_ids[0] in cause_ids,
    }


def aggregate(records: list[dict]) -> dict[str, Any]:
    n = len(records)
    if not n:
        return {"recall@5": 0.0, "mrr": 0.0, "top1_cite": 0.0, "n": 0}
    return {
        "recall@5": round(sum(r["hit_at_k"] for r in records) / n, 3),
        "mrr": round(sum(r["mrr"] for r in records) / n, 3),
        "top1_cite": round(sum(r["top1_cite"] for r in records) / n, 3),
        "n": n,
    }


def blind_recent(conn, k: int) -> list[str]:
    """The GUARD: rank by recency, ignoring the question entirely.

    A benchmark a query-blind rule can win is not measuring retrieval.
    """
    rows = conn.execute(
        "SELECT DISTINCT ON (event_id) event_id, ts FROM vector_records"
        " ORDER BY event_id, ts DESC").fetchall()
    return [e for e, _ in sorted(rows, key=lambda r: r[1], reverse=True)[:k]]


def _single_arm(conn, embedder, question: str, sql_fn, k: int,
                exclude: str | None = None) -> list[str]:
    """Run ONE retrieval arm directly, for the vector-only / keyword-only rows."""
    from freshet.rag.retrieval import vec_literal

    [qvec] = embedder.encode_query([question])
    params: dict[str, Any] = {"qvec": vec_literal(qvec), "q": question, "k": k}
    if exclude is not None:
        params["exclude_event_id"] = exclude
    rows = conn.execute(sql_fn(None, None, exclude), params).fetchall()
    seen: list[str] = []
    for r in rows:
        if r[1] not in seen:
            seen.append(r[1])
    return seen


# ---------------------------------------------------------------- corpus ----

def events_by_incident(conn) -> dict[str, list[tuple[str, datetime]]]:
    """One (event_id, ts) pair per distinct update, grouped by incident_id.
    Events with no incident_id are not linkable to anything and are excluded."""
    rows = conn.execute(
        "SELECT DISTINCT incident_id, event_id, ts FROM vector_records"
        " WHERE incident_id IS NOT NULL").fetchall()
    grouped: dict[str, list[tuple[str, datetime]]] = {}
    for incident_id, event_id, ts in rows:
        grouped.setdefault(incident_id, []).append((event_id, ts))
    return grouped


def event_text(conn, event_id: str) -> str:
    """Reconstruct an update's verbatim text from its ordered chunks."""
    rows = conn.execute(
        "SELECT text FROM vector_records WHERE event_id = %s ORDER BY chunk_index",
        (event_id,)).fetchall()
    return " ".join(r[0] for r in rows)


_INDEX_PROVENANCE_SQL = (
    "SELECT count(*), count(DISTINCT event_id), count(DISTINCT service)"
    " FROM vector_records")


def index_provenance(conn) -> dict:
    """Size and shape of the index a live eval ran against — corpus grows, so a
    committed results file without this is unfalsifiable."""
    n_chunks, n_events, n_providers = conn.execute(_INDEX_PROVENANCE_SQL).fetchone()
    return {
        "n_chunks": n_chunks, "n_events": n_events, "n_providers": n_providers,
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------- runner ----

def main() -> None:
    from freshet.common.db import connect
    from freshet.pipeline.embedding import make_embedder
    from freshet.rag.retrieval import hybrid_search, keyword_sql, vector_sql

    conn = connect()
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))

    grouped = events_by_incident(conn)
    all_candidates = eligible_candidates(grouped)
    cap_raw = os.environ.get(MAX_QUERIES_ENV)
    cap = int(cap_raw) if cap_raw else None
    candidates = capped(all_candidates, cap)

    print(f"[live-eval] live index: {len(all_candidates)} eligible incidents "
          f"(>=2 updates), running {len(candidates)}")

    arms: dict[str, list[dict]] = {"hybrid": [], "vector_only": [],
                                   "keyword_only": [], "blind_recent": []}
    blind_ranking = blind_recent(conn, K)  # query-blind: identical every call
    for c in candidates:
        q = event_text(conn, c.query_event_id)
        causes = set(c.other_event_ids)

        r = hybrid_search(conn, embedder, q, k=K + 1, exclude_event_id=c.query_event_id)
        arms["hybrid"].append(score_one(dedupe_events(r.hits, c.query_event_id), causes))
        arms["vector_only"].append(score_one(
            _single_arm(conn, embedder, q, vector_sql, K + 1, c.query_event_id), causes))
        arms["keyword_only"].append(score_one(
            _single_arm(conn, embedder, q, keyword_sql, K + 1, c.query_event_id), causes))
        arms["blind_recent"].append(score_one(blind_ranking, causes))

    scored = {name: aggregate(recs) for name, recs in arms.items()}
    gap = round(scored["hybrid"]["recall@5"] - scored["blind_recent"]["recall@5"], 3)
    out = {
        "source": "live index",
        "measures": "within-incident linking (NOT causal identification)",
        "provenance": {
            "n_queries": len(candidates),
            "n_eligible_incidents": len(all_candidates),
            "max_queries_cap": cap,
            **index_provenance(conn),
        },
        "arms": scored,
        "gameability_guard": {
            "blind_recall@5": scored["blind_recent"]["recall@5"],
            "hybrid_minus_blind": gap,
            "verdict": "meaningful" if gap >= 0.25 else "SUSPECT — a query-blind rule "
                                                        "scores close to the system",
        },
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
