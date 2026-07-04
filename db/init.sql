-- Freshet M2 schema. Idempotent: safe to apply repeatedly.
-- 384 dims = all-MiniLM-L6-v2 (and the stub embedder matches it).
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
    services           text[] NOT NULL DEFAULT '{}',
    opened_at          timestamptz NOT NULL,
    resolved_at        timestamptz,
    resolution_summary text,
    event_ids          text[] NOT NULL DEFAULT '{}'
);

-- Autopilot (sub-project ①): durable idempotency markers so a brief / postmortem
-- fires at most once per incident even under at-least-once redelivery.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS briefed_at    timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_at timestamptz;

-- Autopilot ③: the Slack ts of the incident's brief message, so the postmortem
-- can post as a threaded reply under it.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS slack_ts text;
