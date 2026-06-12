#!/usr/bin/env bash
# Consumer-group scaling demo: drain a burst of events with WORKERS embedder
# instances and report throughput. Run twice (WORKERS=1, then WORKERS=3) on a
# fresh stack (make down && make up) — topics need their 3 partitions.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS="${WORKERS:-3}"
COUNT="${COUNT:-1000}"          # noise events; total = COUNT + 9 scripted
SPACING="${SPACING:-0}"         # 0 = instantaneous burst, so embedding (not
                                # generation) is the measured bottleneck
EMBEDDER="${EMBEDDER:-minilm}"
BROKERS="${BROKERS:-localhost:9092}"
SEED="${SEED:-$(date +%s)}"
TOTAL=$((COUNT + 9))
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

"${PSQL[@]}" -v ON_ERROR_STOP=1 < db/init.sql > /dev/null
BEFORE=$("${PSQL[@]}" -tAc "SELECT count(*) FROM vector_records")
TARGET=$((BEFORE + TOTAL))

python3 -m freshet.pipeline.normalizer --brokers "$BROKERS" &
PIDS=($!)
for i in $(seq 1 "$WORKERS"); do
  python3 -m freshet.pipeline.embedder --brokers "$BROKERS" --embedder "$EMBEDDER" --metrics-port $((8001 + i)) &
  PIDS+=($!)
done
trap 'kill "${PIDS[@]}" 2>/dev/null || true' EXIT

sleep 5   # let the group settle and the model load before the clock starts
START=$(date +%s)
python3 -m freshet.generator --sink kafka --brokers "$BROKERS" --count "$COUNT" --seed "$SEED" --live --live-spacing "$SPACING"

i=0
until [ "$("${PSQL[@]}" -tAc 'SELECT count(*) FROM vector_records')" -ge "$TARGET" ]; do
  i=$((i+1))
  if [ "$i" -ge 300 ]; then echo "ERROR: did not drain $TOTAL events within 300s"; exit 1; fi
  sleep 1
done
END=$(date +%s)
ELAPSED=$((END - START))
if [ "$ELAPSED" -eq 0 ]; then ELAPSED=1; fi
echo "drained $TOTAL events with $WORKERS embedder(s) in ${ELAPSED}s ($((TOTAL / ELAPSED)) ev/s)"
