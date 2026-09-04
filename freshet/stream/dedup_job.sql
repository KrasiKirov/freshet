-- Freshet stream job: dedup, incident lifecycle, and correlated-degradation bursts.
--
-- Flink SQL, not PyFlink: apache-flink requires apache-beam, which ships no
-- macOS ARM64 wheel. The Flink distribution itself is pure JVM and needs no Python.
--
-- Why Flink: (1) the poller re-emits every update every 60s sweep; dedup here
-- is CHECKPOINTED keyed state, so each update reaches the embedder exactly
-- once, surviving a restart. (2) deduping UPSTREAM of the embedder avoids
-- re-embedding unchanged text — ~1,440 redundant embeddings/incident/day at
-- 42 feeds polled every 60s otherwise.
--
-- An event-time burst window ("N providers degrading at once") was built then
-- DELETED: against 3.1 years of real data it fired zero times at 5min/>=3
-- providers — too few providers for simultaneous degradation. Revisit at
-- ~2000 providers. Embedding stays out of this job so it scales on its own axis.

CREATE TABLE raw_incidents (
  -- Wire-format version. NULL for the unversioned records still on the topic;
  -- coalesced to 1 downstream.
  v             INT,
  provider      STRING,
  incident_id   STRING,
  update_id     STRING,
  -- Digest of the update's TEXT, distinct from update_id which is its identity.
  -- Part of the dedup tuple only. NULL on records produced before this field.
  body_digest   STRING,
  created_at    TIMESTAMP_LTZ(3),
  status        STRING,
  text          STRING,
  incident_name STRING,
  proc_time     AS PROCTIME()
  -- No WATERMARK: every projection dedups on proc_time, not event time, so
  -- nothing here consumes it. It previously read `created_at - INTERVAL '7'
  -- DAY` while three comments called it 30s and 90s — none true, none load-bearing.
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw.incidents',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'freshet-stream',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE normalized_updates (
  v            INT,
  event_id     STRING,
  ts           TIMESTAMP_LTZ(3),
  ingested_at  TIMESTAMP_LTZ(3),
  service      STRING,
  source       STRING,
  type         STRING,
  incident_id  STRING,
  text         STRING,
  -- The incident name as its own field. `text` keeps "<name>: <update>" for
  -- the embedder, but only the FIRST chunk carries that prefix — a citation
  -- from a later chunk was a mid-sentence fragment. Travels separately since names contain colons.
  title        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'normalized.updates',
  'properties.bootstrap.servers' = 'localhost:9092',
  -- Keyed by incident, like incident.lifecycle: ordering holds within a
  -- partition only. Unkeyed, a second embedder instance could process one incident's updates out of order.
  'key.format' = 'json',
  'key.fields' = 'incident_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE raw_deadletter (
  provider      STRING,
  incident_id   STRING,
  update_id     STRING,
  incident_name STRING,
  status        STRING,
  text          STRING,
  seen_at       TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'deadletter.raw',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE incident_lifecycle (
  incident_id STRING,
  service     STRING,
  -- The column name IS the JSON field name on the wire: the consumer
  -- (pipeline/lifecycle.py) reads `type`. Backticked since `type` is reserved in Flink SQL.
  `type`      STRING,
  ts          TIMESTAMP_LTZ(3),
  title       STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'incident.lifecycle',
  'properties.bootstrap.servers' = 'localhost:9092',
  -- Partition by incident so 'opened'/'resolved' stay ordered. Kafka orders
  -- within a partition only: unkeyed, a 3-partition topic (deploy/topics.sh)
  -- can deliver 'resolved' first, which the consumer skips (no brief yet) — losing the postmortem.
  'key.format' = 'json',
  'key.fields' = 'incident_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- Keep-first dedup and the Kafka source offsets live in keyed state; without
-- checkpoints neither survives a restart, so a restarted job re-reads from
-- earliest and re-emits everything already emitted. The header once claimed
-- 'checkpointed dedup' while nothing turned it on.
--
-- `table.exec.source.idle-timeout` was removed: with the watermark gone, it advanced nothing and was a no-op.

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Local single-node demo: a file-backed directory is enough to survive a restart.
SET 'state.checkpoints.dir' = 'file:///tmp/freshet-flink-checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' =
    'RETAIN_ON_CANCELLATION';

-- One shared source for both lifecycle transitions: the two branches used to
-- be byte-identical apart from status, including a pasted 12-line comment —
-- and the recency guard was pasted into only ONE. A view can't live inside a STATEMENT SET, so declared here.
CREATE TEMPORARY VIEW recent_transitions AS
SELECT provider, incident_id, incident_name, created_at, proc_time,
       CASE WHEN LOWER(status) IN ('resolved', 'completed', 'complete')
            THEN 'resolved' ELSE 'opened' END AS transition
FROM raw_incidents
WHERE created_at IS NOT NULL
  -- 'monitoring' counts as open: some providers never post investigating.
  -- 'complete' (no 'd') is what hashicorp posts, measured live — without it those incidents never get a postmortem.
  AND LOWER(status) IN ('investigating', 'identified', 'monitoring',
                        'resolved', 'completed', 'complete')
  -- Only RECENT transitions, both directions: the poller re-emits from
  -- earliest and a resubmitted job starts with empty dedup state, so without
  -- this every incident in 3 years transitions again — measured 1,429 opens
  -- of 3,000 records, only 10 under a day old. The guard was once on the
  -- opened branch only, so a cold replay still flagged thousands of rows
  -- postmortem_needed. A resolving update's own created_at is "now", so this is safe for long incidents.
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

EXECUTE STATEMENT SET
BEGIN

-- 1. Deduplication. The poller re-delivers everything each sweep; keep the
--    FIRST arrival of each (provider, incident, update), drop repeats.
-- Rows that PARSE but have no usable created_at were silently dropped before;
-- route them to a dead-letter topic instead (json.ignore-parse-errors still
-- drops non-JSON rows before they ever become rows, so those can't be routed here).
INSERT INTO raw_deadletter
SELECT provider, incident_id, update_id, incident_name, status, text, proc_time
FROM raw_incidents
WHERE created_at IS NULL;

INSERT INTO normalized_updates
-- Emits the project's canonical Event shape (schemas.py). `ingested_at` is
-- our processing time, so the gap to `ts` is the poll wait we don't control.
SELECT coalesce(v, 1) AS v,
       provider || ':' || incident_id || ':' || update_id AS event_id,
       created_at   AS ts,
       proc_time    AS ingested_at,
       provider     AS service,
       'alert'      AS source,
       'status_update' AS type,
       -- Namespaced by provider: Statuspage ids are per-tenant, shared as a
       -- PRIMARY KEY across 42 tenants — unqualified, a collision merges two providers' incidents.
       provider || ':' || incident_id AS incident_id,
       incident_name || ': ' || text AS text,
       incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             -- body_digest is in the TUPLE, not event_id: an identical
             -- re-emission is the same tuple and suppressed; an EDITED body is
             -- a new tuple, re-emitted under the SAME event_id, so the
             -- embedder's idempotent upsert corrects the row in place.
             --
             -- Do NOT switch to keep-last (ORDER BY proc_time DESC): the
             -- poller re-emits every update every sweep, so keep-last would
             -- re-embed the entire corpus every 60s — the opposite of why dedup is upstream.
             --
             -- coalesce is legibility, not correctness: window partitioning
             -- already groups NULLs; '' just says so and matches what the poller now sends.
             PARTITION BY provider, incident_id, update_id, coalesce(body_digest, '')
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL   -- a single unparseable record must not kill the job
)
WHERE seq = 1;

-- 2. Incident lifecycle, for the Autopilot: v1 inferred this by correlating
--    event types; the feeds state it outright.
--
--    FIRST-open and FIRST-resolve only: deduping per UPDATE (as this used to)
--    fired 'opened' on every investigating/identified update, re-claiming a
--    long incident repeatedly. Partitioning by (provider, incident_id,
--    transition) gives one open and one resolve per incident.
INSERT INTO incident_lifecycle
SELECT provider || ':' || incident_id AS incident_id, provider AS service,
       transition AS `type`, created_at AS ts, incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             -- ORDER BY a SINGLE time attribute: this is what Flink recognises
             -- as deduplication (keep-first), append-only and so sinkable to
             -- Kafka. A second sort key makes it a general Rank, whose
             -- changelog has updates — the sink rejects the job outright
             -- ("doesn't support consuming update and delete changes").
             -- proc_time (not created_at) keeps emission immediate, independent of a re-emitted event's lateness.
             PARTITION BY provider, incident_id, transition
             ORDER BY proc_time ASC) AS seq
  FROM recent_transitions
)
WHERE seq = 1;

END;
