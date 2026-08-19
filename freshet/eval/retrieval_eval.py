"""Retrieval quality on real, hand-labeled Statuspage incidents.

v2 shipped with no retrieval measurement at all, which made every retrieval
change unfalsifiable. This restores one, on a corpus the project did NOT write:
225 real incidents (841 updates) from five public status pages, of which 12 are
hand-labeled and human-reviewed at the update that actually STATES a cause. The
modal real update names no cause ("we have identified the issue and a fix is
being implemented") and is deliberately unlabeled.

Two lessons from v1 are wired in as structure, not prose:

1. The eval maps fixtures into the SHAPE PRODUCTION EMITS — event_id
   "provider:incident:update", text "<name>: <body>", title "<name>", matching
   the Flink projection exactly. An eval-only shape measures an eval-only path.
2. A query-blind ranker is scored every run. v1's root-cause benchmark was
   game-able: a rule that understood nothing scored 1.000 because the generator
   planted every answer at a fixed offset. If the blind arm here approaches the
   real one, the corpus is trivial and the headline numbers mean nothing.

Runs against a DEDICATED database (freshet_eval): the live index is never
touched. Run: make retrieval-eval
"""

from __future__ import annotations

import json
import os
import pathlib
from datetime import datetime
from typing import Any

from freshet.common.schemas import Event, EventSource

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures/real"
RESULTS = pathlib.Path("results/retrieval_eval.json")
EVAL_DB = "freshet_eval"
K = 5

# Hard negatives: on-call vocabulary for systems these five feeds never cover,
# plus plainly unrelated questions. Both must abstain, or the floor is theatre.
OFF_CORPUS = [
    "why is the payments-gateway kubernetes cluster out of memory?",
    "who rotated the TLS certificates on the internal edge proxy?",
    "what caused the search-indexer outage last night?",
    "is the analytics postgres replica lagging behind primary?",
    "how long should I roast a chicken per pound?",
    "what is the capital of Australia?",
]


def event_id_for(provider: str, incident_id: str, update_id: str) -> str:
    """The id the Flink projection builds: provider:incident_id:update_id."""
    return f"{provider}:{incident_id}:{update_id}"


def events_from_incident(provider: str, incident: dict) -> list[Event]:
    """One Event per status update, in the shape normalized.updates carries."""
    name = incident.get("name") or ""
    out = []
    for u in incident.get("incident_updates") or []:
        body = (u.get("body") or "").strip()
        if not body:
            continue
        out.append(Event(
            event_id=event_id_for(provider, incident["id"], u["id"]),
            incident_id=incident["id"],
            service=provider,
            source=EventSource.ALERT,
            type="status_update",
            ts=datetime.fromisoformat(u["created_at"].replace("Z", "+00:00")),
            # Flink emits `incident_name || ': ' || text`, with the name also
            # carried separately — reproduced exactly so chunking and titling
            # behave here as they do in production.
            text=f"{name}: {body}" if name else body,
            title=name or None,
        ))
    return out


def load_corpus(fixtures: pathlib.Path = FIXTURES) -> list[Event]:
    events: list[Event] = []
    for path in sorted(fixtures.glob("*.json")):
        if path.name == "labels.json":
            continue
        data = json.loads(path.read_text())
        for incident in data.get("incidents") or []:
            events.extend(events_from_incident(path.stem, incident))
    return events


def load_labels(fixtures: pathlib.Path = FIXTURES) -> dict:
    return json.loads((fixtures / "labels.json").read_text())


def cause_event_ids(entry: dict) -> set[str]:
    """Labels reference raw Statuspage update ids; map them to indexed events."""
    provider, incident = entry["incident_id"].split(":", 1)
    return {event_id_for(provider, incident, uid) for uid in entry["cause_update_ids"]}


def score_one(ranked_event_ids: list[str], cause_ids: set[str]) -> dict[str, Any]:
    """Score one query's ranked events against the cause-bearing ones."""
    rank = next((i + 1 for i, e in enumerate(ranked_event_ids) if e in cause_ids), None)
    return {
        "hit_at_k": rank is not None and rank <= K,
        "mrr": 1.0 / rank if rank else 0.0,
        "top1_cite": bool(ranked_event_ids) and ranked_event_ids[0] in cause_ids,
    }


def dedupe_events(hits, exclude: str | None = None) -> list[str]:
    """Hits are chunk-level; rank by first appearance of each event.

    `exclude` drops the query's OWN document. When the query is real text lifted
    from an update, that update is trivially its own top hit — scoring it would
    have made top-1 citation structurally 0.000 on every arm, which is an
    artifact of the setup rather than anything about retrieval.
    """
    seen: list[str] = []
    for h in hits:
        if h.event_id != exclude and h.event_id not in seen:
            seen.append(h.event_id)
    return seen


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


# ---------------------------------------------------------------- runner ----

def ensure_eval_db() -> str:
    """A dedicated database. Indexing the eval corpus into the live index would
    both pollute it and let the eval's own writes flatter the freshness metric."""
    import psycopg

    from freshet.common.db import DEFAULT_DSN

    # Derived from the project's configured DSN by swapping only the database
    # name — hardcoding host/port/credentials here silently pointed at a
    # different Postgres (the stack listens on 5433, not the default 5432).
    base = os.environ.get("FRESHET_DSN", DEFAULT_DSN).rsplit("/", 1)[0]
    admin, dsn = f"{base}/postgres", f"{base}/{EVAL_DB}"
    with psycopg.connect(admin, autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (EVAL_DB,)).fetchone():
            c.execute(f'CREATE DATABASE "{EVAL_DB}"')
    schema = pathlib.Path("db/init.sql").read_text()
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(schema)
    return dsn


def index_corpus(conn, embedder, events: list[Event], batch: int = 64) -> int:
    from freshet.pipeline.embedder import records_for_event, upsert_record

    records = [r for ev in events for r in records_for_event(ev)]
    for i in range(0, len(records), batch):
        chunk = records[i:i + batch]
        vectors = embedder.encode([r.text for r in chunk])
        for rec, vec in zip(chunk, vectors, strict=True):
            upsert_record(conn, rec, vec, getattr(embedder, "name", None))
    return len(records)


def _single_arm(conn, embedder, question: str, sql_fn, k: int,
                exclude: str | None = None) -> list[str]:
    """Run ONE retrieval arm directly, for the vector-only / keyword-only rows."""
    from freshet.rag.retrieval import vec_literal

    [qvec] = embedder.encode_query([question])
    rows = conn.execute(sql_fn(None, None),
                        {"qvec": vec_literal(qvec), "q": question, "k": k}).fetchall()
    seen: list[str] = []
    for r in rows:
        if r[1] != exclude and r[1] not in seen:
            seen.append(r[1])
    return seen


def blind_recent(conn, k: int) -> list[str]:
    """The GUARD: rank by recency, ignoring the question entirely.

    A benchmark a query-blind rule can win is not measuring retrieval. This is
    the direct descendant of v1's blind-index guard, which caught a positional
    rule scoring 1.000 on the synthetic root-cause benchmark.
    """
    rows = conn.execute(
        "SELECT DISTINCT ON (event_id) event_id, ts FROM vector_records"
        " ORDER BY event_id, ts DESC").fetchall()
    return [e for e, _ in sorted(rows, key=lambda r: r[1], reverse=True)[:k]]


LIVE_LABELS = FIXTURES.parent / "labels_live.json"


def main() -> None:
    import psycopg

    from freshet.pipeline.embedding import make_embedder
    from freshet.rag.retrieval import hybrid_search, keyword_sql, vector_sql

    # `live` scores the running index (42 providers, current) against labels
    # curated from it; the default fixture corpus is frozen but reproducible
    # anywhere, which is what makes it the one CI can run.
    source = os.environ.get("RETRIEVAL_EVAL_SOURCE", "fixture")
    if source == "live":
        return _main_live(hybrid_search, keyword_sql, vector_sql, make_embedder)

    labels = load_labels()
    if labels.get("curated") != "reviewed":
        print("WARNING: labels are DRAFT — numbers below are not review-backed")

    events = load_corpus()
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    dsn = ensure_eval_db()
    print(f"[retrieval-eval] corpus: {len(events)} updates, "
          f"{len({e.incident_id for e in events})} incidents -> {EVAL_DB}")

    with psycopg.connect(dsn, autocommit=True) as conn:
        n_chunks = index_corpus(conn, embedder, events)
        print(f"[retrieval-eval] indexed {n_chunks} chunks")

        arms: dict[str, list[dict]] = {"hybrid": [], "vector_only": [],
                                       "keyword_only": [], "blind_recent": []}
        abstained_on_corpus = 0
        for entry in labels["labeled"]:
            causes = cause_event_ids(entry)
            q = entry["query"]
            r = hybrid_search(conn, embedder, q, k=K)
            abstained_on_corpus += bool(r.abstained)
            arms["hybrid"].append(score_one(dedupe_events(r.hits), causes))
            arms["vector_only"].append(
                score_one(_single_arm(conn, embedder, q, vector_sql, K), causes))
            arms["keyword_only"].append(
                score_one(_single_arm(conn, embedder, q, keyword_sql, K), causes))
            arms["blind_recent"].append(score_one(blind_recent(conn, K), causes))

        abstained_off = sum(
            bool(hybrid_search(conn, embedder, q, k=K).abstained) for q in OFF_CORPUS)

    scored = {name: aggregate(recs) for name, recs in arms.items()}
    gap = round(scored["hybrid"]["recall@5"] - scored["blind_recent"]["recall@5"], 3)
    out = {
        "corpus": {"updates": len(events),
                   "incidents": len({e.incident_id for e in events}),
                   "labeled": len(labels["labeled"]), "curated": labels.get("curated")},
        "arms": scored,
        "gameability_guard": {
            "blind_recall@5": scored["blind_recent"]["recall@5"],
            "hybrid_minus_blind": gap,
            "verdict": "meaningful" if gap >= 0.25 else "SUSPECT — a query-blind rule "
                                                        "scores close to the system",
        },
        "abstention": {
            "on_corpus_abstained": abstained_on_corpus,
            "on_corpus_total": len(labels["labeled"]),
            "off_corpus_abstained": abstained_off,
            "off_corpus_total": len(OFF_CORPUS),
        },
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))



def _main_live(hybrid_search, keyword_sql, vector_sql, make_embedder) -> None:
    """Score the LIVE index. Read-only: this must never write to it."""
    from freshet.common.db import connect

    labels = json.loads(LIVE_LABELS.read_text())
    state = labels.get("curated")
    if state == "assistant-reviewed":
        r = labels.get("_review", {})
        print(f"NOTE: {LIVE_LABELS.name} was reviewed by the assistant, not a human: "
              f"{len(r.get('rejected_not_a_cause', []))} rejected as non-causes, "
              f"{len(r.get('collapsed_identical_cause', []))} collapsed as duplicates. "
              f"A human sign-off is still outstanding.")
    elif state != "reviewed":
        print(f"WARNING: {LIVE_LABELS.name} is DRAFT (LLM-judged, unreviewed) — "
              f"treat these numbers as indicative only")
    conn = connect()
    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    print(f"[retrieval-eval] live index, {len(labels['labeled'])} labeled incidents")

    arms: dict[str, list[dict]] = {"hybrid": [], "vector_only": [],
                                   "keyword_only": [], "blind_recent": []}
    abstained = 0
    for entry in labels["labeled"]:
        causes, q = set(entry["cause_event_ids"]), entry["query"]
        self_doc = entry.get("query_event_id")     # the update the query came from
        r = hybrid_search(conn, embedder, q, k=K + 1)
        abstained += bool(r.abstained)
        arms["hybrid"].append(score_one(dedupe_events(r.hits, self_doc), causes))
        arms["vector_only"].append(
            score_one(_single_arm(conn, embedder, q, vector_sql, K + 1, self_doc), causes))
        arms["keyword_only"].append(
            score_one(_single_arm(conn, embedder, q, keyword_sql, K + 1, self_doc), causes))
        arms["blind_recent"].append(score_one(blind_recent(conn, K), causes))

    off = sum(bool(hybrid_search(conn, embedder, q, k=K).abstained) for q in OFF_CORPUS)
    scored = {name: aggregate(recs) for name, recs in arms.items()}
    gap = round(scored["hybrid"]["recall@5"] - scored["blind_recent"]["recall@5"], 3)
    out = {
        "source": "live index",
        "corpus": {"labeled": len(labels["labeled"]),
                   "providers": len({e["service"] for e in labels["labeled"]}),
                   "curated": labels.get("curated")},
        "arms": scored,
        "gameability_guard": {"blind_recall@5": scored["blind_recent"]["recall@5"],
                              "hybrid_minus_blind": gap,
                              "verdict": "meaningful" if gap >= 0.25 else "SUSPECT"},
        "abstention": {"on_corpus_abstained": abstained,
                       "on_corpus_total": len(labels["labeled"]),
                       "off_corpus_abstained": off, "off_corpus_total": len(OFF_CORPUS)},
    }
    path = RESULTS.with_name("retrieval_eval_live.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
