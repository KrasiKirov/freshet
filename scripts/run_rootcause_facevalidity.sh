#!/usr/bin/env bash
# Real-data face validity (qualitative, no ground truth): ingest the committed real
# status-feed incidents, then for each run the score-aware timeline and show that the
# cause selector ABSTAINS (status feeds are symptom-only — no change events). Reports
# the abstention rate. Keyless. Assumes `make up` + `make db-init`.
# LIVE=1 polls the live status feeds instead of the committed fixture.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python3}"

"$PY" - <<'PY'
import json, os
from pathlib import Path
from freshet.common.db import connect
from freshet.pipeline.embedding import make_embedder
from freshet.api.retrieval import hybrid_search
from freshet.api.synthesis import build_timeline
from freshet.eval.rootcause import _index_corpus, _EVAL_TAU_S
from freshet.ingest.status_poller import map_incident

if os.environ.get("LIVE") == "1":
    from freshet.ingest.status_poller import SOURCES, fetch
    incidents = []
    for name, url in SOURCES:
        data = fetch(url) or {}
        for inc in data.get("incidents", []):
            incidents.append((name, inc))
else:
    fx = json.loads(Path("freshet/ingest/fixtures/status/sample_incidents.json").read_text())
    incidents = [(fx.get("source", "status"), inc) for inc in fx["incidents"]]

events = []
for source, inc in incidents:
    events.extend(map_incident(source, inc))

emb = make_embedder(os.environ.get("FRESHET_EMBEDDER", "bge"))
conn = connect()
_index_corpus(conn, emb, events)

services = sorted({e.service for e in events})
abstained = 0
for svc in services:
    res = hybrid_search(conn, emb, f"what caused the {svc} incident?", k=12,
                        service=svc, min_similarity=0.0, tau_s=_EVAL_TAU_S)
    tl = build_timeline(res.hits)
    print(f"\n## {svc}")
    print(tl.render())
    if tl.cause is None:
        abstained += 1
    else:
        print(f"  [!] non-abstaining cause surfaced for inspection: "
              f"{tl.cause.type} {tl.cause.text!r}")
conn.close()
n = len(services)
rate = abstained / n if n else 0.0
print(f"\nAbstention rate on real symptom-only incidents: {abstained}/{n} = {rate:.2f}")
PY
