"""Real-data validation eval: hand-labeled public Statuspage incidents.

The synthetic benchmark is co-designed with the generator, so it cannot show
how the system behaves on incident language it did not write. This eval runs
against committed snapshots of the five status feeds the live poller watches
(freshet/eval/fixtures/real/*.json), mapped through the SAME code path live
polling uses (status_poller.map_incident), with hand-curated labels
(labels.json) marking the update(s) where the provider actually stated the
cause.

Scored at whole-corpus scale (no service hint, all providers indexed together):
  recall@5  — a cause-bearing update is retrieved in the top 5
  mrr       — reciprocal rank of the first cause-bearing update
  top1_cite — the top hit (what the keyless composer cites) is cause-bearing
plus the calibrated abstention floor checked against real language: on-corpus
queries must not abstain; off-corpus queries must.

Real status updates are typed investigating/identified/resolved — never
CHANGE_TYPES — so build_timeline's cause selection structurally abstains here
(rootcause-facevalidity covers that); retrieval + citation is what this
measures.

Run (stack up; bge, ~1k texts to embed):
    python -m freshet.eval.real_eval        # or: make real-eval
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib

RESULTS = "results/real_eval.json"
FIXTURES = pathlib.Path("freshet/eval/fixtures/real")

# Same as the other evals — recency-neutral: real incidents span months, so the
# demo-tuned 21-minute decay would zero every score.
_EVAL_TAU_S = 1e12
K = 5

# Off-corpus queries for the real-language abstention check. Ops-flavored ones
# are hard negatives: right vocabulary, but nothing these five feeds cover.
OFF_CORPUS = [
    "why is the payments-gateway kubernetes cluster out of memory?",
    "who rotated the TLS certificates on the internal edge proxy?",
    "what caused the search-indexer outage last night?",
    "is the analytics postgres replica lagging behind primary?",
    "how long should I roast a chicken per pound?",
    "what is the capital of Australia?",
    "recommend a good science fiction novel",
    "what time is sunset in Reykjavik in June?",
]


def update_event_id(incident_id: str, update_id: str) -> str:
    """The event_id map_incident derives for one statuspage update — labels
    reference raw statuspage ids; this maps them to indexed events."""
    digest = hashlib.sha256(f"{incident_id}:{update_id}".encode()).hexdigest()[:16]
    return f"sp_{digest}"


def load_corpus(fixtures_dir: pathlib.Path = FIXTURES) -> list:
    """All events from every provider snapshot, via the live-polling code path."""
    from freshet.ingest.status_poller import map_incident

    events = []
    for path in sorted(fixtures_dir.glob("*.json")):
        if path.name == "labels.json":
            continue
        data = json.loads(path.read_text())
        for incident in data.get("incidents") or []:
            events.extend(map_incident(path.stem, incident))
    return events


def load_labels(fixtures_dir: pathlib.Path = FIXTURES) -> dict:
    return json.loads((fixtures_dir / "labels.json").read_text())


def score_label(hits: list, cause_ids: set[str]) -> dict:
    """Score one query's ranked hits against its cause-bearing event ids.
    Hits are chunk-level; rank by first appearance of each event_id."""
    ranked_events: list[str] = []
    for h in hits:
        if h.event_id not in ranked_events:
            ranked_events.append(h.event_id)
    rank = next((i + 1 for i, eid in enumerate(ranked_events) if eid in cause_ids), None)
    return {
        "hit_at_k": rank is not None and rank <= K,
        "mrr": 1.0 / rank if rank else 0.0,
        "top1_cite": bool(ranked_events) and ranked_events[0] in cause_ids,
    }


def aggregate(records: list[dict]) -> dict:
    n = len(records)
    if n == 0:
        return {"recall@5": 0.0, "mrr": 0.0, "top1_cite": 0.0, "n": 0}
    return {
        "recall@5": round(sum(r["hit_at_k"] for r in records) / n, 3),
        "mrr": round(sum(r["mrr"] for r in records) / n, 3),
        "top1_cite": round(sum(r["top1_cite"] for r in records) / n, 3),
        "n": n,
    }


def main() -> None:
    from freshet.api.retrieval import hybrid_search
    from freshet.common.db import connect
    from freshet.eval.run_eval import index_corpus
    from freshet.pipeline.embedding import make_embedder

    labels = load_labels()
    if labels.get("curated") != "reviewed":
        print(f"NOTE: labels are '{labels.get('curated')}' — draft judgment "
              "calls, not blessed ground truth yet.")

    embedder = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
    conn = connect()
    corpus = load_corpus()
    print(f"Real corpus: {len(corpus)} update events across "
          f"{len({e.incident_id for e in corpus})} incidents; indexing…")
    index_corpus(conn, embedder, corpus)

    records = []
    abstained_on_corpus = 0
    for lab in labels["labeled"]:
        cause_ids = {update_event_id(lab["incident_id"].split(":", 1)[1], uid)
                     for uid in lab["cause_update_ids"]}
        res = hybrid_search(conn, embedder, lab["query"], k=K, service=None,
                            tau_s=_EVAL_TAU_S)
        if res.abstained:
            abstained_on_corpus += 1
        rec = score_label(res.hits, cause_ids)
        records.append(rec)
        mark = "+" if rec["hit_at_k"] else "-"
        print(f"  [{mark}] {lab['incident_id']}: top1={rec['top1_cite']} "
              f"mrr={rec['mrr']:.2f}  {lab['query'][:60]}")

    off_abstain = 0
    for q in OFF_CORPUS:
        if hybrid_search(conn, embedder, q, k=K, service=None,
                         tau_s=_EVAL_TAU_S).abstained:
            off_abstain += 1

    result = {
        "retrieval": aggregate(records),
        "abstention": {
            "on_corpus_abstained": abstained_on_corpus,
            "on_corpus_total": len(records),
            "off_corpus_abstained": off_abstain,
            "off_corpus_total": len(OFF_CORPUS),
        },
        "corpus_events": len(corpus),
        "labels_curated": labels.get("curated"),
        "note": ("hand-labeled real Statuspage incidents; cause = the update "
                 "where the provider stated the cause; whole-corpus, "
                 "recency-neutral, bge"),
    }
    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
    print(json.dumps(result, indent=2, sort_keys=True))
    conn.close()


if __name__ == "__main__":
    main()
