-- Freshet stream job: dedup, incident lifecycle, and correlated-degradation bursts.
--
-- Written in Flink SQL, not PyFlink, because PyFlink cannot be installed on this
-- machine: apache-flink requires apache-beam, which publishes no macOS ARM64
-- wheel for any version. The Flink distribution itself is pure JVM and runs
-- natively, so the job is expressed declaratively and needs no Python at all.
--
-- Why Flink earns its place here:
--   1. The poller is stateless and re-emits every update on every 60s sweep.
--      The deduplication below is CHECKPOINTED keyed state, so each update
--      reaches the embedder exactly once and that guarantee survives a restart.
--   2. Deduping UPSTREAM of the embedder means unchanged text is never
--      re-embedded. At 42 feeds polled every 60s that is ~1,440 redundant
--      embeddings per incident per day.
--
-- An event-time burst window ("N providers degrading at once") was designed and
-- then DELETED: measured against 3.1 years of real data it fired zero times at
-- 5min/>=3 providers, because 42 providers are too few for simultaneous
-- degradation. It existed to justify the tool rather than to serve the
-- objective. At ~2000 providers it would be worth revisiting.
-- Embedding stays out of this job so it can scale on its own axis.

CREATE TABLE raw_incidents (
  -- Wire-format version. NULL for the unversioned records still on the topic;
  -- coalesced to 1 downstream.
  v             INT,
  provider      STRING,
  incident_id   STRING,
  update_id     STRING,
  created_at    TIMESTAMP_LTZ(3),
  status        STRING,
  text          STRING,
  incident_name STRING,
  proc_time     AS PROCTIME()
  -- No WATERMARK. Every projection below dedups on proc_time, so no operator here
  -- consumes event time; a watermark would only govern lateness for operators that
  -- do not exist. It previously read `created_at - INTERVAL '7' DAY` while three
  -- separate comments described it as 30s and as 90s — none of them true, and
  -- none of them load-bearing.
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
  -- The incident name as its own field. `text` keeps the "<name>: <update>" form
  -- the embedder indexes, but only its FIRST chunk carries that prefix, so a
  -- citation or a suggested question built from a later chunk was labelled with a
  -- mid-sentence fragment. Splitting it back out of `text` would be a guess —
  -- incident names contain colons — so it travels separately.
  title        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'normalized.updates',
  'properties.bootstrap.servers' = 'localhost:9092',
  -- Keyed by incident for the same reason incident.lifecycle is: ordering holds
  -- within a partition only. Unkeyed, this topic could never be compacted and a
  -- second embedder instance would process one incident's updates out of order.
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
  -- The column name IS the JSON field name on the wire. The consumer
  -- (pipeline/lifecycle.py LifecycleEvent) reads `type`, and this projection emits
  -- opened/resolved rather than the provider's raw status, so `type` is both the
  -- contract and the accurate name. Backticked: `type` is reserved in Flink SQL.
  `type`      STRING,
  ts          TIMESTAMP_LTZ(3),
  title       STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'incident.lifecycle',
  'properties.bootstrap.servers' = 'localhost:9092',
  -- Partition by incident so 'opened' and 'resolved' for the same incident are
  -- ordered. Kafka orders within a partition only: unkeyed, a 3-partition topic
  -- (declared in `deploy/topics.sh`) can deliver 'resolved' first, which the
  -- consumer skips because no brief has been delivered — postmortem then lost.
  'key.format' = 'json',
  'key.fields' = 'incident_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- Keep-first dedup and the Kafka source offsets live in keyed state; without
-- checkpoints neither survives a restart, so a restarted job re-reads
-- raw.incidents from earliest and re-emits everything it already emitted. The
-- SQL header claimed 'checkpointed dedup' while nothing turned it on.
--
-- `table.exec.source.idle-timeout` used to be set here to stop a quiet partition
-- pinning the watermark. With the watermark gone (nothing in this job consumes
-- event time) it advances nothing and is a no-op, so it is gone too.

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Local single-node demo: a file-backed directory is enough to survive a restart.
SET 'state.checkpoints.dir' = 'file:///tmp/freshet-flink-checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' =
    'RETAIN_ON_CANCELLATION';

-- One shared source for both lifecycle transitions. The two branches below used to
-- be byte-identical apart from their status list, including a twelve-line comment
-- copy-pasted into each -- and the recency guard was pasted into only ONE of them.
-- A view cannot live inside a STATEMENT SET, so it is declared here.
CREATE TEMPORARY VIEW recent_transitions AS
SELECT provider, incident_id, incident_name, created_at, proc_time,
       CASE WHEN LOWER(status) IN ('resolved', 'completed', 'complete')
            THEN 'resolved' ELSE 'opened' END AS transition
FROM raw_incidents
WHERE created_at IS NOT NULL
  -- 'monitoring' counts as open: some providers never post investigating.
  -- 'complete' (no 'd') is what hashicorp posts -- measured on the live feed;
  -- without it those incidents resolve silently and never get a postmortem.
  AND LOWER(status) IN ('investigating', 'identified', 'monitoring',
                        'resolved', 'completed', 'complete')
  -- Only RECENT transitions, for BOTH directions. The source is a re-emitting
  -- poller reading from earliest, and a resubmitted job starts with empty dedup
  -- state, so without this every incident in 3 years of history transitions again:
  -- a sample of 3,000 lifecycle records held 1,429 opens of which 10 were under a
  -- day old. The Autopilot would page a human about outages from 2022.
  -- The guard used to be on the opened branch only, so a cold replay still emitted
  -- a 'resolved' for every historical incident and the consumer's
  -- _DEFER_POSTMORTEM_SQL flagged thousands of rows postmortem_needed. A resolving
  -- update's own created_at is "now", so this is safe for long-running incidents.
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

EXECUTE STATEMENT SET
BEGIN

-- 1. Deduplication. The poller re-delivers everything each sweep; keep the FIRST
--    arrival of each (provider, incident, update) and drop every repeat.
-- Rows that PARSE but have no usable created_at were dropped by every branch
-- below with no trace. Routing them to a dead-letter topic makes the loss
-- visible and replayable, the same contract the embedder already honours.
-- (json.ignore-parse-errors still silently drops rows that are not valid JSON
-- at all; those never become rows and so cannot be routed here.)
INSERT INTO raw_deadletter
SELECT provider, incident_id, update_id, incident_name, status, text, proc_time
FROM raw_incidents
WHERE created_at IS NULL;

INSERT INTO normalized_updates
-- Emits the project's canonical Event shape (freshet/common/schemas.py), which is
-- what the embedder, retrieval and Autopilot all speak. `ingested_at` is our
-- processing time, so the gap to `ts` is the poll wait we do not control.
SELECT coalesce(v, 1) AS v,
       provider || ':' || incident_id || ':' || update_id AS event_id,
       created_at   AS ts,
       proc_time    AS ingested_at,
       provider     AS service,
       'alert'      AS source,
       'status_update' AS type,
       -- Namespaced by provider. Statuspage ids are per-tenant and this value is a
       -- PRIMARY KEY shared by 42 tenants; unqualified, a collision merges two
       -- providers' incidents into one row and the brief cites the wrong one.
       provider || ':' || incident_id AS incident_id,
       incident_name || ': ' || text AS text,
       incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             PARTITION BY provider, incident_id, update_id
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL   -- a single unparseable record must not kill the job
)
WHERE seq = 1;

-- 2. Incident lifecycle, for the Autopilot. v1 had to INFER this by correlating
--    event types; the feeds state it outright.
--
--    FIRST-open and FIRST-resolve only. Deduping per UPDATE (as this used to)
--    emitted 'opened' for every investigating/identified update, so a long
--    incident fired the lifecycle repeatedly — the Autopilot re-claimed it on
--    each one and only the delivery guard stopped a duplicate brief. Partitioning
--    by (provider, incident_id) instead of (.., update_id) means one open and one
--    resolve per incident, which is what the surface actually means.
--    Partitioning by (provider, incident_id, transition) means one open and one
--    resolve per incident, which is what the surface actually means.
INSERT INTO incident_lifecycle
SELECT provider || ':' || incident_id AS incident_id, provider AS service,
       transition AS `type`, created_at AS ts, incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             -- ORDER BY a SINGLE time attribute: this is what Flink recognises
             -- as deduplication (keep-first), which is append-only and so can
             -- feed a Kafka sink. Adding a second sort key makes it a general
             -- Rank, whose changelog contains updates, and the sink rejects the
             -- job outright with "doesn't support consuming update and delete
             -- changes". proc_time (not created_at) keeps emission immediate and
             -- independent of how late a re-emitted update's event time is.
             PARTITION BY provider, incident_id, transition
             ORDER BY proc_time ASC) AS seq
  FROM recent_transitions
)
WHERE seq = 1;

END;
