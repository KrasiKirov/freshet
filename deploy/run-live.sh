#!/bin/sh
# Everything a measurement run needs, under one supervisor.
#
# `caffeinate -i` is not a nicety. Freshness scores only the current continuous
# run, and a display/idle sleep longer than 300s (heartbeat GAP_TOLERANCE_S) ends
# that run and discards every live arrival scored so far. -i prevents idle sleep
# without preventing the lid closing from sleeping the machine deliberately.
#
# Absolute paths throughout: this is also the launchd entry point, and launchd
# provides no profile, no PATH beyond the basics, and no getcwd it can read.
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
