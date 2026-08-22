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

-- Which model produced each embedding. Vectors from different models are not
-- comparable, but a mismatch is invisible in the scores: every similarity simply
-- collapses toward zero and the API abstains, looking exactly like "nothing
-- relevant". Recording the model lets that be detected and reported instead.
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS model text;

-- The incident's own title, so a citation can be labelled by what it IS
-- rather than by whichever sentence fragment the chunker produced.
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS title text;

-- The chunk ordinal, stored rather than parsed back out of the primary key.
-- chunk_id is "chk_<event_id>_<n>" and TWO queries regex-extracted the <n>:
-- orphan cleanup in the embedder, and chunk reassembly for the brief. Neither
-- could use an index, and nothing enforced the id shape they both assumed.
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS chunk_index integer;
-- One-time backfill. Idempotent: after the first pass no NULLs remain, and rows
-- whose id carries no ordinal settle at 0, which is what a single-chunk event is.
UPDATE vector_records
   SET chunk_index = coalesce((regexp_match(chunk_id, '_(\d+)$'))[1]::int, 0)
 WHERE chunk_index IS NULL;


CREATE INDEX IF NOT EXISTS vector_records_service_ts_idx
    ON vector_records (service, ts DESC);

-- Every brief, postmortem and thread reply reassembles ONE incident's updates
-- with `WHERE incident_id = %s`. Partial: status-feed rows always carry an
-- incident_id, but correlator-opened events may not, and there is no reason to
-- index the NULLs.
CREATE INDEX IF NOT EXISTS vector_records_incident_idx
    ON vector_records (incident_id) WHERE incident_id IS NOT NULL;

ALTER TABLE vector_records
    ADD COLUMN IF NOT EXISTS text_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED;

-- No ANN index yet, deliberately. At this corpus size an exact scan is fast and
-- exact; HNSW trades recall for latency and would need its own recall check to
-- stay honest. Add `USING hnsw (embedding vector_cosine_ops)` when row count or
-- query p95 actually justifies it — not before.
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
-- briefed_at/postmortem_at are LEASES (a claim to work), not records that the
-- work happened. These two record delivery, so an expired lease can retry a
-- crashed brief without ever re-posting one that actually landed.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS brief_delivered_at      timestamptz;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_delivered_at timestamptz;
-- When a brief becomes due. The debounce used to be a blocking sleep inside the
-- Kafka handler, which held the partition for 45s per incident and delayed every
-- offset behind it. Scheduling in Postgres lets the handler return immediately
-- (so the offset commits) while an idle tick delivers the brief when it is due.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS brief_due_at timestamptz;

-- Set when an incident resolves before its brief was delivered. The postmortem
-- claim requires a delivered brief, so a resolve arriving inside the debounce
-- window used to match nothing and be skipped forever — Kafka had already
-- committed the offset, so it never came back. Deferring it in Postgres lets the
-- drain post it once the brief lands.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS postmortem_needed boolean NOT NULL DEFAULT false;

-- Newest thread reply already answered, as a Slack ts string. Without it the
-- responder re-answers the whole thread on every poll.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS thread_seen_ts text;

-- The channel ID Slack returned when the brief was posted. chat.postMessage
-- accepts a #name, but conversations.replies requires the ID and resolving a
-- name needs channels:read — a scope the bot does not have and does not need,
-- because the post response already carries the ID.
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS slack_channel_id text;

-- LLM spend, counted per hour and kept in Postgres rather than in memory so a
-- restart cannot reset the budget (a crash-loop would otherwise bill without
-- bound). One row per hour; the daily cap sums the last 24.
CREATE TABLE IF NOT EXISTS llm_budget (
    window_start timestamptz PRIMARY KEY,
    calls        integer NOT NULL DEFAULT 0
);

-- Proof the pipeline was actually up. Freshness scores how fast an update became
-- queryable, but "ts >= min(indexed_at)" only excludes BACKFILL — it cannot tell
-- a slow pipeline from a stopped one. After a 14-hour outage the catch-up burst
-- scored as 9.8-hour staleness and reported streaming as 14x SLOWER than hourly
-- batch. The heartbeat makes the uptime window explicit, so only updates posted
-- while the pipeline was demonstrably running are scored.
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component text PRIMARY KEY,
    beat_at   timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_heartbeat_log (
    component text NOT NULL,
    beat_at   timestamptz NOT NULL,
    PRIMARY KEY (component, beat_at)
);


-- Namespace incident_id by provider. It is a PRIMARY KEY shared across 42
-- Statuspage tenants whose ids are only unique per tenant; a collision merges two
-- providers' incidents into one row and the brief cites the wrong provider.
-- event_id was already namespaced, incident_id was not. Guarded on the absence of
-- a colon, which no raw id can contain (the extractor captures \w+ / [\w-]+ only),
-- so re-running init.sql on a migrated volume is a no-op.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM incidents WHERE incident_id NOT LIKE '%:%') THEN
        -- Rows carrying neither a provider nor any indexed evidence (225 of 1,422
        -- when measured) are residue from the historical lifecycle flood. They
        -- cannot be namespaced and cannot be briefed. Cascades to the join tables.
        DELETE FROM incidents WHERE primary_service IS NULL;

        ALTER TABLE incident_services DROP CONSTRAINT incident_services_incident_id_fkey;
        ALTER TABLE incident_events   DROP CONSTRAINT incident_events_incident_id_fkey;

        UPDATE vector_records   SET incident_id = service || ':' || incident_id
            WHERE incident_id IS NOT NULL AND incident_id NOT LIKE '%:%';
        UPDATE incidents        SET incident_id = primary_service || ':' || incident_id
            WHERE incident_id NOT LIKE '%:%';
        UPDATE incident_services SET incident_id = service || ':' || incident_id
            WHERE incident_id NOT LIKE '%:%';
        -- incident_events has no service column; recover the provider from the
        -- event_id, which was namespaced all along.
        UPDATE incident_events
            SET incident_id = split_part(event_id, ':', 1) || ':' || incident_id
            WHERE incident_id NOT LIKE '%:%';

        -- 900 incident_events rows already dangled before this migration.
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
