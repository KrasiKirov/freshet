#!/usr/bin/env bash
# Vertical-slice demo: generator -> raw.events -> normalizer -> normalized.events
# -> embedder -> pgvector, then a freshness report and one example query.
#
# Assumes the stack is up (make up). For a clean freshness demo run it on a
# fresh stack (make down && make up): workers use stable consumer groups, so
# they also drain any events left on the topics by earlier runs, and old
# non-live events carry fake-historical ts values that pollute the report.
set -euo pipefail
cd "$(dirname "$0")/.."          # repo root, so db/init.sql resolves regardless of caller

COUNT="${COUNT:-60}"            # noise events; total emitted = COUNT + 9 scripted
SPACING="${SPACING:-0.1}"       # seconds between events in live mode
EMBEDDER="${EMBEDDER:-minilm}"  # EMBEDDER=stub skips the model download
BROKERS="${BROKERS:-localhost:9092}"
SEED="${SEED:-$(date +%s)}"     # unique per run so re-runs add rows instead of overwriting
if [ "$COUNT" -le 20 ]; then
  echo "ERROR: COUNT must be > 20 — the scripted incident injects after event 20"
  exit 1
fi
TOTAL=$((COUNT + 9))
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

"${PSQL[@]}" -v ON_ERROR_STOP=1 < db/init.sql > /dev/null

BEFORE=$("${PSQL[@]}" -tAc "SELECT count(*) FROM vector_records")
TARGET=$((BEFORE + TOTAL))

python3 -m freshet.pipeline.normalizer --brokers "$BROKERS" &
NORM_PID=$!
python3 -m freshet.pipeline.embedder --brokers "$BROKERS" --embedder "$EMBEDDER" &
EMB_PID=$!
trap 'kill $NORM_PID $EMB_PID 2>/dev/null || true' EXIT

python3 -m freshet.generator --sink kafka --brokers "$BROKERS" --count "$COUNT" --seed "$SEED" --live --live-spacing "$SPACING"

echo "waiting for $TOTAL events to become queryable..."
i=0
until [ "$("${PSQL[@]}" -tAc 'SELECT count(*) FROM vector_records')" -ge "$TARGET" ]; do
  i=$((i+1))
  if [ "$i" -ge 120 ]; then
    echo "ERROR: pipeline did not index $TOTAL events within 120s"
    exit 1
  fi
  sleep 1
done

python3 -m freshet.eval.freshness

echo
echo "example query: 'what is happening with scheduler-api?'"
EMBEDDER="$EMBEDDER" python3 - <<'EOF'
import os

from freshet.api.app import QueryRequest, search
from freshet.common.db import connect
from freshet.pipeline.embedding import make_embedder

conn = connect()
hits = search(
    conn,
    make_embedder(os.environ["EMBEDDER"]),
    QueryRequest(question="what is happening with scheduler-api?", k=3),
)
for h in hits:
    print(f"  {h.score:.3f}  [{h.source}] {h.ts:%H:%M:%S} -> indexed {h.indexed_at:%H:%M:%S}  {h.text[:70]}")
conn.close()
EOF
