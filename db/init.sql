-- Freshet schema. Idempotent: safe to apply repeatedly.
-- 768 dims = BAAI/bge-base-en-v1.5, the default embedder (the stub matches it).
-- 384-dim MiniLM cannot index into this table; its benchmark numbers are a
-- frozen snapshot (see RESULTS.md M14).
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS vector_records (
    chunk_id    text PRIMARY KEY,
    event_id    text NOT NULL,
    incident_id text,
    service     text NOT NULL,
    ts          timestamptz NOT NULL,
    indexed_at  timestamptz NOT NULL,
    source      text NOT NULL,
    text        text NOT NULL,
    embedding   vector(768) NOT NULL
);

CREATE INDEX IF NOT EXISTS vector_records_service_ts_idx
    ON vector_records (service, ts DESC);

ALTER TABLE vector_records
    ADD COLUMN IF NOT EXISTS text_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED;

CREATE INDEX IF NOT EXISTS vector_records_text_tsv_idx
    ON vector_records USING GIN (text_tsv);

ALTER TABLE vector_records
    ADD COLUMN IF NOT EXISTS severity text;   -- 'SEV1'..'SEV4' or NULL
ALTER TABLE vector_records
    ADD COLUMN IF NOT EXISTS type text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS incidents (
    incident_id        text PRIMARY KEY,
    title              text NOT NULL DEFAULT '',
    opened_at          timestamptz NOT NULL,
    resolved_at        timestamptz,
    resolution_summary text
);

-- Incident<->service and incident<->event joins (FK integrity, indexable
-- lookups) replace the earlier denormalized services/event_ids text[] columns.
CREATE TABLE IF NOT EXISTS incident_services (
    incident_id text NOT NULL REFERENCES incidents(incident_id) ON DELETE CASCADE,
    service     text NOT NULL,
    PRIMARY KEY (incident_id, service)
);
CREATE INDEX IF NOT EXISTS incident_services_service_idx ON incident_services (service);

CREATE TABLE IF NOT EXISTS incident_events (
    incident_id text NOT NULL REFERENCES incidents(incident_id) ON DELETE CASCADE,
    event_id    text NOT NULL,
    PRIMARY KEY (incident_id, event_id)
);

-- One-time migration for volumes created before the join tables existed:
-- backfill from the old arrays, then drop them. Guarded on column existence
-- so re-running init.sql on an already-migrated volume is a no-op.
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

-- Atomic find-or-create for correlator-opened ("auto") incidents: at most one
-- open auto incident per service, enforced by a partial unique index so
-- concurrent normalizers can race the INSERT ... ON CONFLICT safely. Explicit
-- incidents (generator / status feeds, which carry their own incident_id) have
-- auto_opened = false and are exempt — a service can legitimately have several
-- concurrent open status-page incidents.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS primary_service text;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS auto_opened boolean NOT NULL DEFAULT false;
CREATE UNIQUE INDEX IF NOT EXISTS incidents_one_open_auto_per_service
    ON incidents (primary_service) WHERE resolved_at IS NULL AND auto_opened;

-- Autopilot (sub-project ①): durable idempotency markers so a brief / postmortem
-- fires at most once per incident even under at-least-once redelivery.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS briefed_at    timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_at timestamptz;

-- Autopilot ③: the Slack ts of the incident's brief message, so the postmortem
-- can post as a threaded reply under it.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS slack_ts text;
