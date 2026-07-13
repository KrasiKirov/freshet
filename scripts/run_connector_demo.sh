#!/usr/bin/env bash
# Commit-signal demo: replay a GitHub push (a bad commit) + a matching error spike
# on the same service through the pipeline, then print the brief — whose cause cites
# the commit SHA. Keyless (no webhook secret needed). Assumes `make up`.
set -euo pipefail
cd "$(dirname "$0")/.."

BROKERS="${BROKERS:-localhost:9092}"
EMBEDDER="${EMBEDDER:-bge}"
PY="${PY:-.venv/bin/python3}"
SVC="${SVC:-scheduler-api}"

echo "==> Reset corpus + topics"
docker exec -i freshet-postgres psql -U freshet -d freshet -c "DELETE FROM vector_records; DELETE FROM incidents;" >/dev/null
docker exec freshet-redpanda rpk topic delete raw.events normalized.events deadletter.events >/dev/null 2>&1 || true
docker exec freshet-redpanda rpk topic create raw.events normalized.events deadletter.events -p 3 >/dev/null 2>&1 || true

echo "==> Start normalizer + embedder"
"$PY" -m freshet.pipeline.normalizer --brokers "$BROKERS" --group cx-norm --metrics-port 0 &
NORM=$!
"$PY" -m freshet.pipeline.embedder --brokers "$BROKERS" --group cx-emb --embedder "$EMBEDDER" --metrics-port 0 &
EMB=$!
trap 'kill $NORM $EMB 2>/dev/null || true' EXIT
sleep 3

echo "==> Replay a bad commit (GitHub push fixture) + a matching spike on $SVC"
SVC="$SVC" "$PY" - <<'PY'
import json, os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from freshet.common.kafka_io import make_producer, produce_sync
from freshet.common.schemas import Event, EventSource, Severity
from freshet.connectors.github import GitHubConnector

svc = os.environ["SVC"]
push = json.loads(Path("freshet/connectors/fixtures/github/push.json").read_text())
push["repository"]["name"] = svc
t0 = datetime.now(timezone.utc) - timedelta(minutes=6)
push["head_commit"]["timestamp"] = t0.isoformat()
commit = GitHubConnector().parse("push", push)[0]
spike = Event(ts=t0 + timedelta(minutes=5), service=svc, source=EventSource.ALERT,
              type="error_spike", severity=Severity.SEV2,
              text=f"5xx error rate on {svc} crossed 5% (now 20%)")
p = make_producer("localhost:9092")
for ev in (commit, spike):
    produce_sync(p, "raw.events", ev.model_dump_json(), key=ev.service)
print(f"    produced commit {commit.text!r} + spike on {svc}")
PY

echo "==> Waiting for indexing..."
i=0
until [ "$(docker exec -i freshet-postgres psql -U freshet -d freshet -tAc "SELECT count(*) FROM vector_records WHERE type='commit'")" -ge 1 ]; do
  i=$((i+1)); [ "$i" -ge 60 ] && { echo "ERROR: commit not indexed"; exit 1; }; sleep 1
done

echo "==> Brief for $SVC (cause should cite the commit SHA):"
SVC="$SVC" "$PY" - <<'PY'
import os
from freshet.common.db import connect
from freshet.pipeline.embedding import make_embedder
from freshet.api.retrieval import hybrid_search
from freshet.api.synthesis import build_timeline
svc = os.environ["SVC"]
conn = connect(); emb = make_embedder("bge")
res = hybrid_search(conn, emb, f"what caused the {svc} incident?", k=12, service=svc)
print(build_timeline(res.hits).render())
PY
