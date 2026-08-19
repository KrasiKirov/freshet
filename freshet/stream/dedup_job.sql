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
  WATERMARK FOR created_at AS created_at - INTERVAL '30' SECOND
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
  text         STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'normalized.updates',
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

EXECUTE STATEMENT SET
BEGIN

-- 1. Deduplication. The poller re-delivers everything each sweep; keep the FIRST
--    arrival of each (provider, incident, update) and drop every repeat.
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
       incident_name || ': ' || text AS text
FROM (
  SELECT *, ROW_NUMBER() OVER (
             PARTITION BY provider, incident_id, update_id
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL   -- a single unparseable record must not kill the job
)
WHERE seq = 1;

-- 2. Incident lifecycle, for the Autopilot. v1 had to INFER this by correlating
--    event types; the feeds state it outright, so it is a CASE over the update's
--    own status. Intermediate states (monitoring, in progress) signal nothing —
--    firing on those would re-brief the same incident repeatedly.
INSERT INTO incident_lifecycle
SELECT incident_id, provider AS service,
       CASE WHEN LOWER(status) IN ('investigating', 'identified') THEN 'opened'
            ELSE 'resolved' END AS `type`,
       created_at AS ts, incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             PARTITION BY provider, incident_id, update_id
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL
)
WHERE seq = 1
  AND LOWER(status) IN ('investigating', 'identified', 'resolved', 'completed');

END;
