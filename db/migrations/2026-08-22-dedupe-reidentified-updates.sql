-- One-time cleanup after update_id changed meaning.
--
-- Identity went from a digest of (timestamp-text | body) to one of
-- (timestamp-text # ordinal-within-that-timestamp), so that a provider editing an
-- update corrects the indexed row instead of minting a second one. The digest
-- function also changed (sha1[:12] -> blake2s(6)).
--
-- Consequence: every update still inside a provider's feed window is re-emitted
-- once under a new event_id and appears twice in the index. Updates OUTSIDE that
-- window are never re-emitted and keep their old ids untouched, which is correct --
-- they are historical and immutable. The feed window is small (measured: github 25
-- entries / ~3 weeks, openai 93 / ~3 months), so this is bounded.
--
-- Deduplicate on (incident_id, ts, text), keeping the newest-indexed copy.
--
-- The TIMESTAMP is load-bearing and was missing from the first version of this
-- migration. Keying on text alone looks right — the ids differ by construction, so
-- text seems like what identifies "the same update" — but providers repeat
-- boilerplate verbatim WITHIN one incident. box:29247755 posted the identical
-- "We are continuing to monitor" line eleven times over three hours; on the live
-- index, text-only keying found 3,363 groups and would have deleted 3,661 rows,
-- against 3,447 groups and 3,454 rows for the correct key. The 207-row difference
-- is real updates: it would have flattened those incidents' timelines to a single
-- entry. freshet/ingest/statuspage.py already guards this one level up
-- (test_identical_body_text_at_different_times_stays_two_distinct_updates); the
-- same reasoning has to hold here.
--
-- Under the correct key the live index shows 3,440 groups of exactly two and 7 of
-- three — the expected re-identification signature, one old id and one new.
--
-- Run AFTER the poller has completed at least two full sweeps with the new adapter.
-- Idempotent: a second run finds no duplicate groups.

BEGIN;

-- Inspect before deleting. Both counts should be 0 after this migration.
CREATE TEMP TABLE _dupes ON COMMIT DROP AS
SELECT incident_id, ts, text, max(indexed_at) AS keep_after
FROM vector_records
WHERE incident_id IS NOT NULL
GROUP BY incident_id, ts, text
HAVING count(*) > 1;

DELETE FROM vector_records v
USING _dupes d
WHERE v.incident_id = d.incident_id
  AND v.ts = d.ts
  AND v.text = d.text
  AND v.indexed_at < d.keep_after;

-- Chunks are gone; their incident_events rows now dangle.
DELETE FROM incident_events e
WHERE NOT EXISTS (SELECT 1 FROM vector_records v WHERE v.event_id = e.event_id);

COMMIT;
