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
    embedding   vector(384) NOT NULL
);

CREATE INDEX IF NOT EXISTS vector_records_service_ts_idx
    ON vector_records (service, ts DESC);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id        text PRIMARY KEY,
    title              text NOT NULL DEFAULT '',
    services           text[] NOT NULL DEFAULT '{}',
    opened_at          timestamptz NOT NULL,
    resolved_at        timestamptz,
    resolution_summary text,
    event_ids          text[] NOT NULL DEFAULT '{}'
);
