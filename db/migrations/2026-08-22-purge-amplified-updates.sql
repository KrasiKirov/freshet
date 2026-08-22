-- Purge rows written by the pre-fix parser for providers whose markup was not
-- understood. Their identity digested the live "Affected components" list, so each
-- incident accumulated one record per component flip (measured 2026-08-22, before
-- the fix -- see docs/ingestion-review.md):
--   openai:    5,360 chunks / 2,414 events / 97 incidents  (24.9 events per incident)
--   hashicorp: 1,790 chunks /   616 events / 26 incidents  (23.7 events per incident)
-- against a 2.8-7.5 baseline for every other provider. After the fix both parse at
-- 1.0 updates per incident.
--
-- The new parser re-emits these providers' in-window updates under new ids; the old
-- rows can never be overwritten because their chunk_ids no longer correspond to
-- anything the poller produces.
--
-- Deliberately scoped by service, NOT by text pattern: a text-based predicate would
-- also match legitimate updates that happen to quote a component name.
--
-- The `indexed_at` cutoff is what makes this safe to run AFTER the new poller has
-- already indexed fresh rows: those are newer than the cutoff and survive. It also
-- makes the migration idempotent -- a second run finds nothing left to delete.

BEGIN;

DELETE FROM vector_records
WHERE service IN ('openai', 'hashicorp')
  AND indexed_at < now() - interval '1 hour';

-- incident_events rows for those event_ids are now dangling.
DELETE FROM incident_events e
WHERE NOT EXISTS (SELECT 1 FROM vector_records v WHERE v.event_id = e.event_id);

-- incidents rows with no remaining evidence cannot be briefed and cannot be cited.
DELETE FROM incidents i
WHERE i.primary_service IN ('openai', 'hashicorp')
  AND NOT EXISTS (SELECT 1 FROM vector_records v WHERE v.incident_id = i.incident_id);

COMMIT;
