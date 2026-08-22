#!/bin/sh
# Declare the broker topology. `rpk topic create` is a no-op against an existing
# topic, so partition count and retention are asserted with `alter-config` rather
# than assumed: the old `create ... -p 3 || true` in `make up` silently left every
# topic at whatever Redpanda auto-created it as (1 partition), while the SQL job
# reasoned about 3.
set -eu
RPK="docker exec freshet-redpanda rpk"

# 3 partitions: enough to let a second embedder or autopilot instance take work,
# small enough that a single-node Redpanda is not paying for coordination.
for t in raw.incidents normalized.updates incident.lifecycle deadletter.events deadletter.raw; do
  $RPK topic create "$t" -p 3 >/dev/null 2>&1 || true
done
# Quarantine for dead-letter envelopes that cannot be routed (see
# pipeline/replay_deadletter.py). Never consumed by a worker, so it needs no
# parallelism. It was created only by accident, when something first produced to it.
$RPK topic create deadletter.unusable -p 1 >/dev/null 2>&1 || true

# The Flink source reads scan.startup.mode = earliest-offset. At the 7-day default
# a job resubmitted after a week silently replays a truncated history and the gap
# is invisible. 30 days covers any realistic outage.
$RPK topic alter-config raw.incidents --set retention.ms=2592000000 >/dev/null
$RPK topic alter-config normalized.updates --set retention.ms=2592000000 >/dev/null
# Dead letters are evidence; they are read by hand and by replay_deadletter.
$RPK topic alter-config deadletter.events --set retention.ms=2592000000 >/dev/null
$RPK topic alter-config deadletter.raw --set retention.ms=2592000000 >/dev/null
$RPK topic alter-config deadletter.unusable --set retention.ms=-1 >/dev/null

# v1 topics. The v1 pipeline (raw.events -> normalized.events) no longer exists;
# nothing in this codebase produces to or consumes from them.
for t in raw.events normalized.events; do
  $RPK topic delete "$t" >/dev/null 2>&1 || true
done

$RPK topic list
