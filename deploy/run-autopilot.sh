#!/bin/sh
# Launched by launchd, which provides almost no environment (no PATH beyond
# basics, no profile, no ~/Documents access) — everything here is absolute.
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

# Secrets stay in .env.local, out of the plist and out of git.
if [ -f "$REPO/.env.local" ]; then
  set -a
  . "$REPO/.env.local"
  set +a
fi

# bge would otherwise take every core; the caps bound spend.
export FRESHET_TORCH_THREADS="${FRESHET_TORCH_THREADS:-2}"
export FRESHET_LLM_HOURLY_CAP="${FRESHET_LLM_HOURLY_CAP:-60}"
export FRESHET_LLM_DAILY_CAP="${FRESHET_LLM_DAILY_CAP:-500}"
export FRESHET_SINK=slack

exec "$REPO/.venv/bin/python" -m freshet.autopilot \
  --brokers localhost:9092 --sink slack
