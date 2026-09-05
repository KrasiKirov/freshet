#!/bin/sh
# Everything a measurement run needs, under one supervisor.
#
# caffeinate -i keeps the run alive without blocking a deliberate lid-close sleep.
#
# Absolute paths: this is also the launchd entry point, which has no profile,
# no PATH beyond the basics, and no getcwd it can read.
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"
mkdir -p logs

if [ -f "$REPO/.env.local" ]; then
  set -a
  . "$REPO/.env.local"
  set +a
fi

# bge would otherwise take every core; the caps bound spend. Do not raise them.
export FRESHET_TORCH_THREADS="${FRESHET_TORCH_THREADS:-2}"
export FRESHET_LLM_HOURLY_CAP="${FRESHET_LLM_HOURLY_CAP:-60}"
export FRESHET_LLM_DAILY_CAP="${FRESHET_LLM_DAILY_CAP:-500}"
export FRESHET_SINK=slack

exec /usr/bin/caffeinate -i "$REPO/.venv/bin/python" -m freshet.ops.supervisor
