#!/usr/bin/env bash
# One-command Slack demo. Drives one incident open -> resolve through the full
# pipeline so the autopilot posts a **cited brief** and, on resolution, a **threaded
# postmortem**. Safe by default: SINK=slack-dry-run renders the exact Block Kit
# payload to stdout and posts NOTHING (no token needed). REAL=1 posts for real
# (requires SLACK_BOT_TOKEN + SLACK_CHANNEL in .env.local). Assumes `make up`.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi

PY="${PY:-.venv/bin/python3}"
BROKERS="${BROKERS:-localhost:9092}"
EMBEDDER="${EMBEDDER:-bge}"
COUNT="${COUNT:-25}"                      # > 20: the scripted incident injects after event 20
export AUTOPILOT_WINDOW_S="${AUTOPILOT_WINDOW_S:-8}"   # short debounce so the brief posts fast
PSQL=(docker exec -i freshet-postgres psql -U freshet -d freshet)

if [ "${REAL:-0}" = "1" ]; then
  SINK=slack
  : "${SLACK_BOT_TOKEN:?REAL=1 needs SLACK_BOT_TOKEN in .env.local}"
  : "${SLACK_CHANNEL:?REAL=1 needs SLACK_CHANNEL in .env.local}"
  echo "==> REAL post mode: will post to $SLACK_CHANNEL"
else
  SINK=slack-dry-run
  echo "==> DRY-RUN: renders the Slack payload below, posts nothing (REAL=1 to post for real)"
fi

echo "==> Reset corpus + incident state + topics"
"${PSQL[@]}" -c "DELETE FROM vector_records; DELETE FROM incidents;" >/dev/null
docker exec freshet-redpanda rpk topic delete raw.events normalized.events incident.lifecycle deadletter.events >/dev/null 2>&1 || true
docker exec freshet-redpanda rpk topic create raw.events normalized.events incident.lifecycle deadletter.events -p 3 >/dev/null 2>&1 || true

echo "==> Start normalizer + embedder + autopilot (--sink $SINK)"
"$PY" -m freshet.pipeline.normalizer --brokers "$BROKERS" --group sd-norm --metrics-port 0 &
NORM=$!
"$PY" -m freshet.pipeline.embedder --brokers "$BROKERS" --group sd-emb --embedder "$EMBEDDER" --metrics-port 0 &
EMB=$!
"$PY" -m freshet.autopilot --brokers "$BROKERS" --group sd-auto --sink "$SINK" &
AUTO=$!
trap 'kill $NORM $EMB $AUTO 2>/dev/null || true' EXIT
sleep 4   # let the autopilot subscribe to incident.lifecycle before events flow

echo "==> Inject a scripted incident (deploy -> error spike -> chat -> rollback -> healthy)"
"$PY" -m freshet.generator --sink kafka --brokers "$BROKERS" \
  --count "$COUNT" --seed "$(date +%s)" --live --live-spacing 0.1

echo "==> Waiting for the autopilot to brief the incident, then postmortem it on resolve..."
i=0
until [ "$("${PSQL[@]}" -tAc "SELECT count(*) FROM incidents WHERE briefed_at IS NOT NULL AND postmortem_at IS NOT NULL")" -ge 1 ]; do
  i=$((i+1)); [ "$i" -ge 120 ] && { echo "ERROR: no brief+postmortem within 120s"; exit 1; }; sleep 1
done

echo
if [ "$SINK" = "slack" ]; then
  echo "==> Done — a cited brief and a threaded postmortem were POSTED to $SLACK_CHANNEL."
else
  echo "==> Done — the [slack-dry-run] blocks above are the exact brief + threaded"
  echo "    postmortem that would post. Run 'REAL=1 make slack-demo' to post for real."
fi
