#!/usr/bin/env bash
# Root-cause demo: stream the richer corpus (live timestamps) through the pipeline,
# then print a grounded, cited root-cause timeline for one incident. Assumes
# `make up`. Keyless (stub embedder). Set FRESHET_RERANK=cross-encoder to rerank.
set -euo pipefail
cd "$(dirname "$0")/.."

EMBEDDER="${EMBEDDER:-stub}"
BROKERS="${BROKERS:-localhost:9092}"
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

"${PSQL[@]}" -v ON_ERROR_STOP=1 < db/init.sql > /dev/null
"${PSQL[@]}" -c "DELETE FROM vector_records; DELETE FROM incidents;" > /dev/null
docker exec freshet-redpanda rpk topic delete raw.events normalized.events deadletter.events >/dev/null 2>&1 || true
docker exec freshet-redpanda rpk topic create raw.events normalized.events deadletter.events -p 3 >/dev/null 2>&1 || true

echo "==> Streaming the richer corpus..."
python3 -m freshet.pipeline.normalizer --brokers "$BROKERS" --group rc-norm --metrics-port 0 &
NORM=$!
python3 -m freshet.pipeline.embedder --brokers "$BROKERS" --group rc-emb --embedder "$EMBEDDER" --metrics-port 0 &
EMB=$!
trap 'kill $NORM $EMB 2>/dev/null || true' EXIT

BROKERS="$BROKERS" python3 - <<'EOF'
import datetime as _dt
import os
from freshet.common.kafka_io import make_producer, produce_sync
from freshet.generator.generator import build_corpus_events
prod = make_producer(os.environ.get("BROKERS", "localhost:9092"))
events = build_corpus_events(seed=1, n_incidents=5)
for ev in events:
    ev.ts = _dt.datetime.now(_dt.timezone.utc)   # live timestamps for the 24h window
    produce_sync(prod, "raw.events", key=ev.service, value=ev.model_dump_json())
prod.flush()
print(f"produced {len(events)} events")
EOF

echo "waiting for events to index..."
i=0
until [ "$("${PSQL[@]}" -tAc 'SELECT count(*) FROM vector_records')" -ge 50 ]; do
  i=$((i + 1))
  if [ "$i" -ge 120 ]; then
    echo "ERROR: pipeline did not index in time"
    exit 1
  fi
  sleep 1
done

echo
echo "==> Root-cause timeline for the scheduler-api incident:"
EMBEDDER="$EMBEDDER" python3 - <<'EOF'
import os
from freshet.api.retrieval import hybrid_search
from freshet.api.rerank import make_reranker
from freshet.api.synthesis import build_timeline
from freshet.common.db import connect
from freshet.pipeline.embedding import make_embedder
conn = connect()
res = hybrid_search(conn, make_embedder(os.environ["EMBEDDER"]),
                    "what caused the scheduler-api incident and how was it resolved?",
                    k=12, service="scheduler-api", min_similarity=0.0,
                    reranker=make_reranker())
print(build_timeline(res.hits).render())
conn.close()
EOF
