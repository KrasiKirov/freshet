-- Freshet schema. Idempotent: safe to apply repeatedly.
--
-- Applied two ways, and BOTH must work: the Postgres container mounts this at
-- docker-entrypoint-initdb.d (a FRESH volume), and tests/integration/conftest.py
-- re-applies it to an EXISTING database on every run — so ordering is
-- load-bearing. test_schema_bootstraps.py asserts both paths reach the identical schema.
--
-- Structure: (1) extension, (2) tables — every column in its CREATE TABLE,
-- (3) indexes, (4) legacy migrations for volumes predating a change, (5) the
-- applied version. History that still MATTERS lives in section 4, guarded so a fresh volume skips it.
--
-- No migration framework, deliberately: one deployment, container-initialized,
-- no rolling upgrades. This file can't express a type change, NOT NULL, or a
-- drop — that need is the signal to adopt a real runner instead.
--
-- 768 dims = BAAI/bge-base-en-v1.5 (the stub matches it). 384-dim MiniLM
-- can't index here; its benchmark numbers are a frozen snapshot (RESULTS.md M14).

CREATE EXTENSION IF NOT EXISTS vector;


CREATE TABLE IF NOT EXISTS vector_records (
    chunk_id    text PRIMARY KEY,
    -- The chunk ordinal, stored rather than parsed from the primary key. Two
    -- queries regex-extracted it from "chk_<event_id>_<n>" (orphan cleanup,
    -- chunk reassembly) with no index and nothing enforcing the shape.
    chunk_index integer,
    event_id    text NOT NULL,
    incident_id text,
    service     text NOT NULL,
    ts          timestamptz NOT NULL,
    indexed_at  timestamptz NOT NULL,
    source      text NOT NULL,
    text        text NOT NULL,
    -- The incident's own title, so a citation can be labelled by what it IS
    -- rather than by whichever sentence fragment the chunker produced.
    title       text,
    severity    text,                     -- 'SEV1'..'SEV4' or NULL
    type        text NOT NULL DEFAULT '',
    -- Which model produced each embedding. Vectors from different models
    -- aren't comparable, but a mismatch is invisible: similarity just collapses toward zero and abstains.
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
    -- Atomic find-or-create for correlator-opened ("auto") incidents: at most
    -- one open auto incident per service, enforced by the partial unique
    -- index in section 3. Status-feed incidents (auto_opened=false) are exempt — a service can have several concurrent open ones.
    primary_service         text,
    auto_opened             boolean NOT NULL DEFAULT false,
    -- Autopilot idempotency: a brief/postmortem fires at most once per
    -- incident under at-least-once redelivery. These two are LEASES, not proof the work happened.
    briefed_at              timestamptz,
    postmortem_at           timestamptz,
    -- The Slack ts of the incident's brief message, so the postmortem can post
    -- as a threaded reply under it.
    slack_ts                text,
    -- ...and these two record DELIVERY, so an expired lease can retry a crashed
    -- brief without ever re-posting one that actually landed.
    brief_delivered_at      timestamptz,
    postmortem_delivered_at timestamptz,
    -- When a brief becomes due. The debounce used to block the Kafka handler
    -- for 45s/incident; scheduling here lets the offset commit immediately while an idle tick delivers it.
    brief_due_at            timestamptz,
    -- Set when an incident resolves before its brief delivered. The postmortem
    -- claim needs a delivered brief, so a resolve inside the debounce window
    -- used to match nothing and vanish (offset already committed). Deferred here so the drain posts it once the brief lands.
    postmortem_needed       boolean NOT NULL DEFAULT false,
    -- Newest thread reply already answered, as a Slack ts string. Without it the
    -- responder re-answers the whole thread on every poll.
    thread_seen_ts          text,
    -- The channel ID Slack returned when posted. chat.postMessage accepts a
    -- #name, but conversations.replies needs the ID — no channels:read scope
    -- needed, since the post response already carries it.
    slack_channel_id        text
);

-- Incident<->service and incident<->event joins (FK integrity, indexable
-- lookups) replace the earlier denormalized services/event_ids text[] columns.
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

-- LLM spend, counted per hour and kept in Postgres so a restart can't reset
-- the budget. One row per hour; the daily cap sums the last 24.
CREATE TABLE IF NOT EXISTS llm_budget (
    window_start timestamptz PRIMARY KEY,
    calls        integer NOT NULL DEFAULT 0
);

-- Proof the pipeline was actually up: "ts >= min(indexed_at)" only excludes
-- BACKFILL, not a stopped pipeline — after a 14h outage the catch-up burst
-- scored 9.8h staleness and reported streaming 14x slower than batch.
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component text PRIMARY KEY,
    beat_at   timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_heartbeat_log (
    component text NOT NULL,
    beat_at   timestamptz NOT NULL,
    PRIMARY KEY (component, beat_at)
);

-- The mean embedding of the index, per model. bge's cosine space is
-- anisotropic: RANDOM unrelated pairs average 0.594 and 12.2% clear the 0.70
-- floor — an absolute floor there is a percentile, not a semantic boundary.
-- Subtracting the centroid removes the shared component. Per model since vectors from different models share no geometry.
CREATE TABLE IF NOT EXISTS index_stats (
    model       text PRIMARY KEY,
    centroid    vector(768) NOT NULL,
    n_chunks    bigint NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now()
);


CREATE INDEX IF NOT EXISTS vector_records_service_ts_idx
    ON vector_records (service, ts DESC);

-- Every brief/postmortem/thread reply reassembles ONE incident's updates via
-- `WHERE incident_id = %s`. Partial: correlator-opened events may lack an incident_id, no reason to index NULLs.
CREATE INDEX IF NOT EXISTS vector_records_incident_idx
    ON vector_records (incident_id) WHERE incident_id IS NOT NULL;

-- No ANN index yet, deliberately: at this corpus size an exact scan is fast
-- and exact. Add `USING hnsw (embedding vector_cosine_ops)` when row count or query p95 justifies it.
CREATE INDEX IF NOT EXISTS vector_records_text_tsv_idx
    ON vector_records USING GIN (text_tsv);

CREATE INDEX IF NOT EXISTS incident_services_service_idx ON incident_services (service);

CREATE UNIQUE INDEX IF NOT EXISTS incidents_one_open_auto_per_service
    ON incidents (primary_service) WHERE resolved_at IS NULL AND auto_opened;


-- For volumes created before a change landed. A FRESH database makes every
-- statement below a no-op — asserted by
-- test_a_fresh_database_and_an_evolved_one_reach_the_same_schema. Each is guarded to stay idempotent.

-- Columns added to vector_records after its CREATE TABLE existed.
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS model text;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS title text;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS chunk_index integer;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS severity text;
ALTER TABLE vector_records ADD COLUMN IF NOT EXISTS type text NOT NULL DEFAULT '';
ALTER TABLE vector_records
    ADD COLUMN IF NOT EXISTS text_tsv tsvector
    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED;

-- Columns added to incidents after its CREATE TABLE existed.
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

-- Backfill the chunk ordinal for rows indexed before the column existed.
-- Idempotent: after the first pass no NULLs remain; a bare id settles at 0 (a single-chunk event).
UPDATE vector_records
   SET chunk_index = coalesce((regexp_match(chunk_id, '_(\d+)$'))[1]::int, 0)
 WHERE chunk_index IS NULL;

-- One-time migration for volumes predating the join tables: backfill from the
-- old arrays, then drop them. Guarded on column existence, so re-running is a no-op.
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

-- Namespace incident_id by provider: it's a PRIMARY KEY shared across 42
-- Statuspage tenants whose ids are only unique per tenant — a collision merges
-- two providers' incidents. Guarded on absence of a colon (no raw id has one, the extractor captures \w+/[\w-]+ only), so re-running is a no-op.
DO $$
BEGIN
    -- The guard must name EVERY table the block converts, not just the
    -- first: keyed on `incidents` alone, a bare-id vector_records row with no
    -- bare-id incidents row skipped the migration. incident_services/incident_events need no clause — both FK to incidents.
    IF EXISTS (SELECT 1 FROM incidents WHERE incident_id NOT LIKE '%:%')
       OR EXISTS (SELECT 1 FROM vector_records
                  WHERE incident_id IS NOT NULL AND incident_id NOT LIKE '%:%') THEN
        -- Rows with neither a provider nor indexed evidence can't be
        -- namespaced or briefed, so they go first — freeing the ids the UPDATE below claims.
        --
        -- Predicate is "no provider AND no evidence", NOT "id looks bare".
        -- Measured live: all 225 such rows were eval-fixture stubs (titles like
        -- 'openai: resolved', zero chunks) seeded by an eval run. Skipping them
        -- for carrying a colon left 26 real incidents unable to convert — their
        -- namespaced form already taken by a stub — and the migration died on a duplicate key.
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


-- Bump when a section above changes, so a stale volume is diagnosable.
CREATE TABLE IF NOT EXISTS schema_version (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;
