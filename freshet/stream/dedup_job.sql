-- Freshet stream job: dedup, incident lifecycle, and correlated-degradation bursts.

CREATE TABLE raw_incidents (
  v             INT,
  provider      STRING,
  incident_id   STRING,
  update_id     STRING,
  body_digest   STRING,
  created_at    TIMESTAMP_LTZ(3),
  status        STRING,
  text          STRING,
  incident_name STRING,
  proc_time     AS PROCTIME()
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
  title        STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'normalized.updates',
  'properties.bootstrap.servers' = 'localhost:9092',
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
  `type`      STRING,
  ts          TIMESTAMP_LTZ(3),
  title       STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'incident.lifecycle',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json',
  'key.fields' = 'incident_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);


SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 'file:///tmp/freshet-flink-checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' =
    'RETAIN_ON_CANCELLATION';

CREATE TEMPORARY VIEW recent_transitions AS
SELECT provider, incident_id, incident_name, created_at, proc_time,
       CASE WHEN LOWER(status) IN ('resolved', 'completed', 'complete')
            THEN 'resolved' ELSE 'opened' END AS transition
FROM raw_incidents
WHERE created_at IS NOT NULL
  AND LOWER(status) IN ('investigating', 'identified', 'monitoring',
                        'resolved', 'completed', 'complete')
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

EXECUTE STATEMENT SET
BEGIN

-- 1. Deduplication.
INSERT INTO raw_deadletter
SELECT provider, incident_id, update_id, incident_name, status, text, proc_time
FROM raw_incidents
WHERE created_at IS NULL;

INSERT INTO normalized_updates
SELECT coalesce(v, 1) AS v,
       provider || ':' || incident_id || ':' || update_id AS event_id,
       created_at   AS ts,
       proc_time    AS ingested_at,
       provider     AS service,
       'alert'      AS source,
       'status_update' AS type,
       provider || ':' || incident_id AS incident_id,
       incident_name || ': ' || text AS text,
       incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             PARTITION BY provider, incident_id, update_id, coalesce(body_digest, '')
             ORDER BY proc_time ASC) AS seq
  FROM raw_incidents
  WHERE created_at IS NOT NULL   -- a single unparseable record must not kill the job
)
WHERE seq = 1;

-- 2. Incident lifecycle, for the Autopilot.
INSERT INTO incident_lifecycle
SELECT provider || ':' || incident_id AS incident_id, provider AS service,
       transition AS `type`, created_at AS ts, incident_name AS title
FROM (
  SELECT *, ROW_NUMBER() OVER (
             PARTITION BY provider, incident_id, transition
             ORDER BY proc_time ASC) AS seq
  FROM recent_transitions
)
WHERE seq = 1;

END;
