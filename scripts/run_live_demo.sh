#!/usr/bin/env bash
# Live demo: ingest REAL incidents from public status feeds through the full
# pipeline, then open the UI. Assumes `make up`. Uses the bge retriever by default
# (set EMBEDDER=stub for a keyless run).
set -euo pipefail
cd "$(dirname "$0")/.."

BROKERS="${BROKERS:-localhost:9092}"
EMBEDDER="${EMBEDDER:-bge}"
PY="${PY:-.venv/bin/python3}"
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

# Load local secrets so /query synthesizes cited answers via the LLM composer
# instead of the keyless extractive fallback. Never printed.
if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  echo "==> Answer synthesis: LLM (claude-sonnet-4-6)"
else
  echo "==> Answer synthesis: extractive template (set ANTHROPIC_API_KEY in .env.local for LLM)"
fi

echo "==> Resetting corpus + topics"
"${PSQL[@]}" -c "DELETE FROM vector_records; DELETE FROM incidents;" >/dev/null
docker exec freshet-redpanda rpk topic delete raw.events normalized.events deadletter.events >/dev/null 2>&1 || true
docker exec freshet-redpanda rpk topic create raw.events normalized.events deadletter.events -p 3 >/dev/null 2>&1 || true

echo "==> Starting workers + API (embedder=$EMBEDDER)"
"$PY" -m freshet.pipeline.normalizer --brokers "$BROKERS" --group live-norm --metrics-port 8001 &
NORM=$!
"$PY" -m freshet.pipeline.embedder --brokers "$BROKERS" --group live-emb --embedder "$EMBEDDER" --metrics-port 8002 &
EMB=$!
FRESHET_EMBEDDER="$EMBEDDER" "$PY" -m uvicorn freshet.api.app:app --port 8000 --log-level warning &
API=$!
trap 'kill $NORM $EMB $API 2>/dev/null || true' EXIT

echo "==> Polling real status feeds once"
sleep 3
"$PY" -m freshet.ingest.status_poller --brokers "$BROKERS" --once

echo "==> Waiting for events to index..."
i=0
until [ "$("${PSQL[@]}" -tAc "SELECT count(*) FROM vector_records WHERE source='alert'")" -ge 1 ]; do
  i=$((i + 1)); [ "$i" -ge 90 ] && { echo "ERROR: nothing indexed"; exit 1; }; sleep 1
done
echo "    indexed $("${PSQL[@]}" -tAc "SELECT count(*) FROM vector_records WHERE source='alert'") alert events across $("${PSQL[@]}" -tAc "SELECT count(DISTINCT service) FROM vector_records WHERE source='alert'") services"

echo "==> UI ready at http://localhost:8000  (Ctrl-C to stop)"
command -v open >/dev/null && open http://localhost:8000 || true
wait $API
