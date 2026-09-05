-- Freshet schema. Idempotent: safe to apply repeatedly.

-- 1. Extension
CREATE EXTENSION IF NOT EXISTS vector;


-- 2. Tables
CREATE TABLE IF NOT EXISTS vector_records (
    chunk_id    text PRIMARY KEY,
    chunk_index integer,
    event_id    text NOT NULL,
    incident_id text,
    service     text NOT NULL,
    ts          timestamptz NOT NULL,
    indexed_at  timestamptz NOT NULL,
    source      text NOT NULL,
    text        text NOT NULL,
    title       text,
    severity    text,                     -- 'SEV1'..'SEV4' or NULL
    type        text NOT NULL DEFAULT '',
    model       text,
    embedding   vector(768) NOT NULL,
    text_tsv    tsvector GENERATED ALWAYS AS (to_tsvector('english', text)) STORED
);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id             text PRIMARY KEY,
    title                   text NOT NULL DEFAULT '',
    opened_at               timestamptz NOT NULL,
    resolved_at             timestamptz,
    resolution_summary      text,
    primary_service         text,
    auto_opened             boolean NOT NULL DEFAULT false,
    briefed_at              timestamptz,
    postmortem_at           timestamptz,
    slack_ts                text,
    brief_delivered_at      timestamptz,
    postmortem_delivered_at timestamptz,
    brief_due_at            timestamptz,
    postmortem_needed       boolean NOT NULL DEFAULT false,
    thread_seen_ts          text,
    slack_channel_id        text
);

CREATE TABLE IF NOT EXISTS incident_services (
    incident_id text NOT NULL REFERENCES incidents(incident_id) ON DELETE CASCADE,
    service     text NOT NULL,
    PRIMARY KEY (incident_id, service)
);

CREATE TABLE IF NOT EXISTS incident_events (
    incident_id text NOT NULL REFERENCES incidents(incident_id) ON DELETE CASCADE,
    event_id    text NOT NULL,
    PRIMARY KEY (incident_id, event_id)
);

CREATE TABLE IF NOT EXISTS llm_budget (
    window_start timestamptz PRIMARY KEY,
    calls        integer NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component text PRIMARY KEY,
    beat_at   timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_heartbeat_log (
    component text NOT NULL,
    beat_at   timestamptz NOT NULL,
    PRIMARY KEY (component, beat_at)
);

CREATE TABLE IF NOT EXISTS index_stats (
    model       text PRIMARY KEY,
    centroid    vector(768) NOT NULL,
    n_chunks    bigint NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now()
);


-- 3. Indexes
CREATE INDEX IF NOT EXISTS vector_records_service_ts_idx
    ON vector_records (service, ts DESC);

CREATE INDEX IF NOT EXISTS vector_records_incident_idx
    ON vector_records (incident_id) WHERE incident_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS vector_records_text_tsv_idx
    ON vector_records USING GIN (text_tsv);

CREATE INDEX IF NOT EXISTS incident_services_service_idx ON incident_services (service);

CREATE UNIQUE INDEX IF NOT EXISTS incidents_one_open_auto_per_service
    ON incidents (primary_service) WHERE resolved_at IS NULL AND auto_opened;


-- Legacy migrations (section 4): for volumes created before a change landed. A FRESH
-- database makes every statement below a no-op.
--
-- tests/integration/test_schema_bootstraps.py parses the exact string
-- "-- Legacy migrations" above to split this file in two — keep that string
-- intact even if the rest of this comment gets reworded.

ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS model text;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS title text;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS chunk_index integer;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS severity text;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS type text NOT NULL DEFAULT '';
ALTER TABLE vector_records
    ADD COLUMN IF NOT EXISTS text_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED;

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS primary_service text;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS auto_opened boolean NOT NULL DEFAULT false;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS briefed_at    timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_at timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS slack_ts text;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS brief_delivered_at      timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_delivered_at timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS brief_due_at timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_needed boolean NOT NULL DEFAULT false;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS thread_seen_ts text;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS slack_channel_id text;

UPDATE vector_records
   SET chunk_index = coalesce((regexp_match(chunk_id, '_(\d+)$'))[1]::int, 0)
 WHERE chunk_index IS NULL;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'incidents' AND column_name = 'services') THEN
        INSERT INTO incident_services (incident_id, service)
        SELECT incident_id, unnest(services) FROM incidents
        ON CONFLICT DO NOTHING;

        INSERT INTO incident_events (incident_id, event_id)
        SELECT incident_id, unnest(event_ids) FROM incidents
        ON CONFLICT DO NOTHING;

        ALTER TABLE incidents DROP COLUMN services;
        ALTER TABLE incidents DROP COLUMN event_ids;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM incidents WHERE incident_id NOT LIKE '%:%')
       OR EXISTS (SELECT 1 FROM vector_records
                  WHERE incident_id IS NOT NULL AND incident_id NOT LIKE '%:%') THEN
        DELETE FROM incidents i
        WHERE i.primary_service IS NULL
          AND NOT EXISTS (SELECT 1 FROM vector_records v
                          WHERE v.incident_id = i.incident_id);

        ALTER TABLE incident_services DROP CONSTRAINT incident_services_incident_id_fkey;
        ALTER TABLE incident_events   DROP CONSTRAINT incident_events_incident_id_fkey;

        UPDATE vector_records   SET incident_id = service || ':' || incident_id
            WHERE incident_id IS NOT NULL AND incident_id NOT LIKE '%:%';
        UPDATE incidents        SET incident_id = primary_service || ':' || incident_id
            WHERE incident_id NOT LIKE '%:%';
        UPDATE incident_services SET incident_id = service || ':' || incident_id
            WHERE incident_id NOT LIKE '%:%';
        UPDATE incident_events
            SET incident_id = split_part(event_id, ':', 1) || ':' || incident_id
            WHERE incident_id NOT LIKE '%:%';

        DELETE FROM incident_events e
            WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.incident_id = e.incident_id);
        DELETE FROM incident_services s
            WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.incident_id = s.incident_id);

        ALTER TABLE incident_services ADD CONSTRAINT incident_services_incident_id_fkey
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE;
        ALTER TABLE incident_events ADD CONSTRAINT incident_events_incident_id_fkey
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE;
    END IF;
END $$;


-- 5. Applied version
CREATE TABLE IF NOT EXISTS schema_version (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;
