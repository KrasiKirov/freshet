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
  provider      STRING,
  incident_id   STRING,
  update_id     STRING,
  created_at    TIMESTAMP_LTZ(3),
  status        STRING,
  text          STRING,
  incident_name STRING,
  proc_time     AS PROCTIME(),
  -- 30s tolerance absorbs pollers whose sweeps are staggered against each other
  -- A cache-miss sweep re-emits MONTHS of Atom history long after the watermark
  -- has advanced to 'now'. At 90s those first-seen rows were dropped as late
  -- data and never indexed. Dedup orders by proc_time, so a wide watermark costs
  -- nothing here — it only governs how late an event may arrive.
  WATERMARK FOR created_at AS created_at - INTERVAL '7' DAY
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
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
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
  -- (which `make up` creates) can deliver 'resolved' first, which the consumer
  -- skips because no brief has been delivered — and the postmortem is then lost.
  'key.format' = 'json',
  'key.fields' = 'incident_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- Keep-first dedup and the Kafka source offsets live in keyed state; without
-- checkpoints neither survives a restart, so a restarted job re-reads
-- raw.incidents from earliest and re-emits everything it already emitted. The
-- SQL header claimed 'checkpointed dedup' while nothing turned it on.
-- A quiet partition otherwise pins the watermark at its last event and stalls
-- every event-time operator behind it. This is a pipeline option, not a Kafka
-- connector option: Flink rejects the job outright if it appears in a table's
-- WITH clause ("Unsupported options found for 'kafka'").
SET 'table.exec.source.idle-timeout' = '60s';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Local single-node demo: a file-backed directory is enough to survive a restart.
SET 'state.checkpoints.dir' = 'file:///tmp/freshet-flink-checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' =
    'RETAIN_ON_CANCELLATION';

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
SELECT provider || ':' || incident_id || ':' || update_id AS event_id,
       created_at   AS ts,
       proc_time    AS ingested_at,
       provider     AS service,
       'alert'      AS source,
       'status_update' AS type,
       incident_id,
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
--    'monitoring' counts as open: some providers never post investigating.
INSERT INTO incident_lifecycle
SELECT incident_id, provider AS service, 'opened' AS `type`,
       created_at AS ts, incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             -- ORDER BY a SINGLE time attribute: this is what Flink recognises
             -- as deduplication (keep-first), which is append-only and so can
             -- feed a Kafka sink. Adding a second sort key makes it a general
             -- Rank, whose changelog contains updates, and the sink rejects the
             -- job outright with "doesn't support consuming update and delete
             -- changes". proc_time (not created_at) keeps emission immediate
             -- rather than waiting on the 90s watermark.
             PARTITION BY provider, incident_id
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL
    AND LOWER(status) IN ('investigating', 'identified', 'monitoring')
    -- Only RECENT opens. The source is a re-emitting poller reading from
    -- earliest, and a resubmitted job starts with empty dedup state, so without
    -- this every incident in 3 years of history is 'opened' again: a sample of
    -- 3,000 lifecycle records held 1,429 opens of which 10 were under a day old.
    -- The Autopilot would page a human about outages from 2022.
    AND created_at > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
)
WHERE seq = 1;

INSERT INTO incident_lifecycle
SELECT incident_id, provider AS service, 'resolved' AS `type`,
       created_at AS ts, incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             -- ORDER BY a SINGLE time attribute: this is what Flink recognises
             -- as deduplication (keep-first), which is append-only and so can
             -- feed a Kafka sink. Adding a second sort key makes it a general
             -- Rank, whose changelog contains updates, and the sink rejects the
             -- job outright with "doesn't support consuming update and delete
             -- changes". proc_time (not created_at) keeps emission immediate
             -- rather than waiting on the 90s watermark.
             PARTITION BY provider, incident_id
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL
    AND LOWER(status) IN ('resolved', 'completed')
)
WHERE seq = 1;

END;
