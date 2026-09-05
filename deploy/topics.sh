#!/bin/sh
# Declare the broker topology. `rpk topic create` is a no-op against an
# existing topic, so partition count/retention are asserted via alter-config.
set -eu
RPK="docker exec freshet-redpanda rpk"

# 3 partitions: enough to let a second embedder or autopilot instance take work,
# small enough that a single-node Redpanda is not paying for coordination.
for t in raw.incidents normalized.updates incident.lifecycle deadletter.events deadletter.raw; do
  $RPK topic create "$t" -p 3 >/dev/null 2>&1 || true
done
# Quarantine for dead-letter envelopes that can't be routed (replay_deadletter.py).
# Never consumed by a worker, so needs no parallelism — created only by accident.
$RPK topic create deadletter.unusable -p 1 >/dev/null 2>&1 || true

# Flink reads scan.startup.mode = earliest-offset; 30 days covers any realistic outage
$RPK topic alter-config raw.incidents --set retention.ms=2592000000 >/dev/null
$RPK topic alter-config normalized.updates --set retention.ms=2592000000 >/dev/null
# Dead letters are evidence; they are read by hand and by replay_deadletter.
$RPK topic alter-config deadletter.events --set retention.ms=2592000000 >/dev/null
$RPK topic alter-config deadletter.raw --set retention.ms=2592000000 >/dev/null
$RPK topic alter-config deadletter.unusable --set retention.ms=-1 >/dev/null

# v1 topics; nothing in this codebase produces to or consumes from them
for t in raw.events normalized.events; do
  $RPK topic delete "$t" >/dev/null 2>&1 || true
done

$RPK topic list
