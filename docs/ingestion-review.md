# Ingestion, source-adapter and Kafka-schema review

Scope: `freshet/ingest/*`, `freshet/pipeline/*`, `freshet/common/kafka_io.py`,
`freshet/stream/dedup_job.sql`, topic/table design. Findings below were checked
against the running index (11,870 chunks / 1,251 incidents / 42 services) and the
live broker, not read off the source alone.

---

## P0 — The fallback parse path is amplifying two providers into 60% of the corpus

`statuspage.py` matches exactly one markup shape:

```
<small>Aug 21, 00:37 UTC</small><br> <strong>Resolved</strong> - body
```

`github` matches it (12 update blocks in the current entry). `openai` and
`hashicorp` do not — they serve a different shape entirely:

```html
<b>Status: Resolved</b><br/><br/>Our Engineering team has resolved the issue...
<br/><br/><b>Affected components</b><ul><li>HCP Terraform (Operational)</li>...</ul>
```

`_UPDATE.finditer` returns 0 blocks, so `parse_atom` takes the "degrade to one
record per revision" branch. Two things go wrong there:

1. **Identity is derived from volatile content.** `_make` digests
   `sha1(f"{marker}|{body}")` where `body` is `_plain(<whole content>)` — which
   includes the live *Affected components* list. Every time any component's
   parenthetical status changes, the digest changes, Flink sees a brand-new
   `update_id`, and a new row is embedded and indexed. Forever.
2. **Status is hardcoded `"unknown"`**, which is in neither lifecycle predicate
   (`investigating|identified|monitoring`, `resolved|completed`).

Measured blast radius:

| provider | incidents | events | events/incident |
|---|---|---|---|
| openai | 97 | 2,414 | **24.9** |
| hashicorp | 26 | 616 | **23.7** |
| github | 41 | 307 | 7.5 |
| cloudflare | 80 | 224 | 2.8 |
| datadog | 25 | 130 | 5.2 |

One openai incident (`01KSPYYC11CCY3KKXEK4E03CBG`) holds **555 chunks across 185
event_ids, every one stamped with the same `ts`**. Across the whole index openai
contributes 5,360 chunks of which only 3,120 are distinct text; hashicorp 1,790 of
1,543. Together those two providers are ~60% of the index, mostly repeated
`Affected components ... (Operational)` boilerplate.

Consequences:

- **Retrieval is polluted.** Nothing in `retrieval.py` dedupes by text, so a
  top-8 for an openai question can be eight near-copies of a component table.
  Both arms are affected — the boilerplate is dense-similar *and* BM25-rich.
- **These two providers can never be briefed.** `status="unknown"` is excluded
  from both `incident_lifecycle` branches, so no `opened` and no `resolved` is
  ever emitted for them. The loudest providers in the index are invisible to the
  entire Autopilot surface.
- **~25× wasted embedding compute** on those incidents.
- The `ts` for these records is frozen at the entry's `updated`, so every update
  in an incident shares one timestamp — ordering within the incident is lost and
  any freshness number computed over them is meaningless.

**Fix.** Add a second parser branch for the `<b>Status: X</b>` shape:

- status ← the text after `Status:`, lowercased (gives you real
  `resolved`/`investigating` → lifecycle starts firing for these providers);
- body ← content up to `<b>Affected components</b>`, with the component list
  **stripped before both hashing and indexing**;
- identity ← `sha1(status | stripped_body)`, so a component flapping does not
  mint a new update;
- timestamp ← the entry's `updated` (these feeds carry only the current state, so
  one entry really is one update).

Note the residual limitation worth documenting: for these providers `<content>`
holds only the latest state, so a backfill sees only each incident's final
update — unlike the Statuspage shape, which carries the whole thread.

Keep the current "unknown" path as a genuine last resort, but **do not index the
raw content**, and give it an identity that cannot churn (e.g. digest the entry's
`updated` alone).

---

## P1 — `incident_id` is not namespaced by provider

`_INCIDENT_ID` extracts the raw per-tenant id (`Incident/12345678` or
`/incidents/01KY...`). Flink passes it through unqualified, and it becomes:

- `incidents.incident_id` — a **PRIMARY KEY**,
- the filter in `_INDEXED_COUNT_SQL`, the brief's evidence query, and every
  Autopilot claim,
- the Kafka **partition key** of `incident.lifecycle`.

`event_id` is correctly namespaced (`provider:incident_id:update_id`);
`incident_id` is not. Current id shapes in the index: 1,030 ids of length 8,
98 of length 12, 123 of length 26. The 8-char space is small and per-tenant —
two of the 42 tenants sharing one id is a matter of time, and the failure is
silent: one provider's incident row absorbs another's, `incident_services` grows
a second service, and the brief cites the wrong provider's updates. No collision
exists in the index today, so this is cheap to fix now and expensive later.

**Fix.** Make it `provider:incident_id` at the Flink projection (both
`normalized_updates` and `incident_lifecycle`), plus a migration for the existing
rows. Keep `service` as-is for filtering.

---

## P1 — An ETag is remembered before the parse is known to have succeeded

`poller.py:186-189`

```python
if status == 304 or not body:
    return []
cache.remember(page.url, headers)      # ← stored unconditionally
return parse_atom(page.provider, body)
```

A truncated or momentarily malformed body makes `parse_atom` return `[]` (it
swallows `ET.ParseError` by design), but the validator is already persisted. The
next sweep gets a 304 and **those updates are never ingested** — not dead-lettered,
not logged, not recoverable until the provider revises the feed again. This is the
one silent-loss path in an ingest layer that otherwise routes everything to a DLQ.

**Fix.** `remember()` only after `parse_atom` returns a non-empty list, and log
(or dead-letter) a 200 that parses to zero entries.

---

## P1 — Poll-cache persistence is off in every shipped run path

`ConditionalCache.__init__` falls back to `os.environ.get("FRESHET_POLL_CACHE")`
and then to `""`, and `save()` returns immediately when the path is empty.
`FRESHET_POLL_CACHE` is set **nowhere** in the repo — not in the `poller` make
target, not in `run-forever`, not in `deploy/run-autopilot.sh`, not in
`.env.local`.

So the README's "ETag conditional requests (**persisted across restarts**)" and the
class's own "the single biggest politeness lever" are true of the code and false of
every way the code is actually launched. Every poller restart re-downloads all 42
feeds in full and re-emits months of history — which is also what forced the
7-day watermark in the Flink job.

**Fix.** Default the path (e.g. `~/.freshet/poll-cache.json` or `logs/poll-cache.json`)
rather than to `""`, so persistence is opt-*out*. Add a startup log line stating
which cache file is in use, or "cache disabled".

---

## P1 — Partition reality contradicts the code, and the key choice is wrong for it

`rpk topic list` on the running cluster:

```
deadletter.events   3    normalized.events   3   (orphan, v1)
deadletter.raw      3    normalized.updates  1
incident.lifecycle  1    raw.events          3   (orphan, v1)
                         raw.incidents       1
```

Three problems:

1. **`make up` never applied `-p 3`.** The `rpk topic create ... -p 3 || true`
   silently no-ops because the topics were already auto-created at 1 partition.
   `dedup_job.sql:105` asserts "a 3-partition topic (which `make up` creates)" —
   that has never been true for `incident.lifecycle`.
2. **`raw.incidents` is keyed by `update.dedup_key`**, which is unique per update.
   At 1 partition this is harmless. At 3 it is not: the keep-first lifecycle
   projection orders by `proc_time`, so which update is "the first open" becomes
   a race between partitions, and the `ts` on the `opened` event may belong to a
   later update. The key should be `provider:incident_id` — still well spread
   across 42 providers, but per-incident order is preserved and dedup (which is
   keyed state on the full triple) is unaffected.
3. **`normalized.updates` has no key at all** (no `key.format`/`key.fields` in the
   sink). It is also at 1 partition, so the embedder can never scale past one
   consumer — that, plus `commit_every=1`, is the real throughput ceiling, not the
   embedding itself.

Also: `deadletter.raw` and `deadletter.unusable` are not in the `make up` create
list (they exist by auto-creation), and `raw.events` / `normalized.events` are
dead v1 topics still on the broker.

**Fix.** Put every topic in the create list with explicit partitions and
retention; key `raw.incidents` and `normalized.updates` by
`provider:incident_id`; delete the two orphan topics; make the create step
`rpk topic alter-config`-aware or fail loudly instead of `|| true`.

---

## P1 — Corrections to an update can never propagate

Two coupled design choices make the pipeline blind to edits:

- Identity is `sha1(raw_timestamp_text | body)`. If a provider **edits** an
  update's text — a typo fix, an added detail — the digest changes and the edit
  arrives as a *new* update. The stale original stays in the index forever.
- Dedup is **keep-first** on `proc_time`. If a re-parse produces a *corrected*
  `created_at` for the same `update_id` (very common — `_parse_when` falls back to
  the entry's `updated` whenever the timezone abbreviation is unresolvable, and
  the fallback value moves as the entry is revised), the correction is dropped.

Net effect: the first version of a record the pipeline ever saw is the version it
keeps, and later divergence shows up as duplication rather than as an update.

**Fix.** Either make identity structural (`provider:incident_id:sequence` or the
raw timestamp text alone, so an edit updates in place), or switch the projection
to keep-last on a changelog and emit a compacted upsert stream. The first is much
cheaper and fits the current sinks.

---

## P2 — Politeness gaps the README claims are covered

- **No `Retry-After` handling.** `http_fetch` re-raises every non-304 `HTTPError`,
  so a 429 or 503 falls into `HostBackoff.failed()` → a 2-second retry, ignoring
  the interval the provider explicitly asked for. For a project whose ingest
  docstring opens with "politeness is a design requirement", this is the gap that
  matters most.
- **No `Accept-Encoding: gzip`.** urllib does not add it, so all 42 feeds transfer
  uncompressed on every cache miss.
  **Correction, measured while implementing this:** the expected 3–5× win does not
  exist. Sampling 14 of the 42 feeds, **zero** honour `Accept-Encoding: gzip` —
  every Atlassian-hosted Statuspage serves uncompressed regardless of what is
  asked for (647,817 bytes on the wire either way). Only the non-Statuspage
  providers compress: `status.openai.com` returns ~8 KB gzipped. Sending the header
  is still correct and costs nothing, but it is close to inert today. Note also
  that advertising `br` makes those CDNs return brotli, which the stdlib cannot
  decode — so the request must stay narrowly `gzip`.
- **No per-sweep jitter.** Only the *first* sweep is staggered
  (`random.uniform(0, 5)`); after that the cadence is a fixed 60 s, so every host
  is hit at the same second of every minute indefinitely.
- **Backoff is per-URL, not per-host**, despite the name and the README. Minor
  today (the 42 hostnames are distinct), but it will not do what it says if two
  feeds ever share a host.
- **`response.read()` is unbounded**, and untrusted third-party XML goes straight
  into stdlib `ElementTree`. Add a size cap (a status feed over ~2 MB is
  pathological) and consider `defusedxml` for the parse.

---

## P2 — The ingest stage has no metrics at all

`freshet/pipeline/metrics.py` instruments the embedder well. The poller — the
stage that actually determines freshness, and the one talking to 42 third
parties — exports nothing. There is no way to answer "is a provider 304-ing?",
"how many feeds are in backoff?", "did the parse yield drop?" other than by
reading `logs/poller.log`.

Minimum useful set: `freshet_poll_fetch_total{provider,status}`,
`freshet_poll_updates_parsed{provider}`, `freshet_poll_backoff_hosts`,
`freshet_poll_sweep_seconds`. `freshet_poll_updates_parsed` going to zero for a
provider is exactly the signal that would have caught the P0 above.

---

## P2 — An embedder-wide failure drains the stream into the DLQ

`make_handler` retries `emb.encode` three times (0.2 s, 0.4 s) and then
dead-letters. That is right for one poison message and wrong for a systemic
failure: an OOM, a missing model file, or a torch-thread misconfiguration
dead-letters *every* message as fast as the consumer can poll them. The upsert
path already reasons about this correctly ("dead-lettering them during a DB
outage would drain the stream into the DLQ") — the embed path needs the same
reasoning.

**Fix.** Track consecutive dead-letters; past a small threshold (say 10), stop
committing and exit non-zero. Crash-looping under supervision is the correct
failure mode; silently emptying the topic into the DLQ is not.

---

## P2 — Flink job: stale comments, dead watermark, asymmetric recency guard

- `WATERMARK FOR created_at AS created_at - INTERVAL '7' DAY`, but the comment
  above it says "30s tolerance" and two comments below say "the 90s watermark".
  All three are wrong relative to the code.
- More to the point: **nothing in the job uses event time.** All three
  projections order by `proc_time`. The watermark is dead weight that only
  invites the confusion above — it can simply be deleted.
- The `opened` branch is guarded by
  `created_at > CURRENT_TIMESTAMP - INTERVAL '24' HOUR` with a well-argued
  comment. The `resolved` branch has **no such guard**, so a cold replay emits a
  `resolved` for every historical incident. The consumer's
  `_DEFER_POSTMORTEM_SQL` sets `postmortem_needed = true` on any of them whose
  postmortem was never delivered — thousands of rows flagged, dormant only
  because the drain also requires `brief_delivered_at IS NOT NULL`. Add the same
  24-hour predicate; the resolving update's own `created_at` is "now", so it is
  safe for genuinely long-running incidents.
- The two `resolved`/`opened` subqueries are byte-identical apart from the status
  list and the filter — worth a single view or a `WHERE` on a shared CTE.

---

## P2 — Wire format has no version, and parse errors are configured invisible

`to_message()` emits seven bare fields with no `schema_version`. The project has
already been bitten by this twice — `status` → `type` on the lifecycle topic
(handled by a compatibility shim in `LifecycleEvent.from_json`) and `title` added
as optional on `Event`. Meanwhile `'json.ignore-parse-errors' = 'true'` on the
source means a producer-side shape change is dropped **without a row, without a
dead-letter and without a metric** — the SQL comment acknowledges this but nothing
counts it.

**Fix.** Add a `v` field to `to_message` and assert on it in the consumers; export
a Flink `numRecordsIn` vs. emitted-rows gauge, or drop
`ignore-parse-errors` in favour of a raw-string source column plus an explicit
parse-and-dead-letter branch.

---

## P3 — Smaller items, roughly in order of value

1. **`parse_atom` is outside the try in `poll_once`** (`poller.py:189`). Only
   `fetch` is guarded. `parse_atom` swallows `ParseError`, but any other
   exception propagates through `pool.map` → `poll_once` → `run`, killing the
   whole poller over one provider's markup. Move it inside.
2. **`log.info("sweep %d done in %.1fs (%d produced)", ..., produced)`** logs the
   *cumulative* counter as if it were the sweep's — the per-sweep number is what
   you want when watching a backfill drain.
3. **`min(MAX_BACKOFF_S, 2.0 ** n)`** overflows (`OverflowError`) once `n` reaches
   1024. The failure count persists across restarts and only resets on success, so
   a permanently dead feed reaches it in ~85 days. Clamp the exponent:
   `2.0 ** min(n, 10)`.
4. **`commit_every=1`** (the `consume_loop` default, which the embedder does not
   override) means a synchronous offset commit per message, and `emb.encode` is
   called with one event's 1–3 chunks — so the batching the module name promises
   never happens. Upserts are idempotent by construction, so `commit_every=50`
   plus a small accumulate-then-encode buffer is safe and is the single biggest
   throughput win available.
5. **`_OFFSETS` is US/EU-only and silently ambiguous.** `CST` is mapped to −6
   (US) though it is also China +8; `BST` to +1 though it is also Brazil −3. `IST`,
   `JST`, `AEST` are absent, so those fall back to the entry timestamp — the same
   quality loss as the P0 path, just quieter. Worth a counter on how often the
   fallback fires (see the metrics item above).
6. **Dead schema vocabulary.** `Severity`, most of `EventType`, `CHANGE_TYPES` and
   `REMEDIATION_TYPES` have no reference outside `schemas.py` — nothing in this
   pipeline ever populates them (`source='alert'` and `type='status_update'` are
   hardcoded in the Flink projection, `severity` is always NULL). They are v1
   residue on the canonical contract and on three DB columns. Delete or clearly
   quarantine them.
7. **`title_of()`'s legacy fallback** splits on the first `": "` while the comment
   directly above it notes that titles legitimately contain colons. Now that Flink
   sends `title` as its own field, the fallback is only for pre-migration messages
   still on the topic — worth an expiry date and a counter.
8. `hashlib.sha1` for the update digest will trip bandit/`ruff S324`. Not a
   security issue at all here, but `blake2s(digest_size=6)` is a free swap that
   keeps linters quiet.

---

## Suggested order of work

1. The `<b>Status:</b>` parser branch + strip the component list (P0) — it fixes
   corpus quality, retrieval, embedding cost, and unblocks two providers from the
   Autopilot surface in one change. Backfill by deleting those providers' rows and
   replaying.
2. Namespace `incident_id` (P1) — cheap now, silent corruption later.
3. `remember()` after a successful parse, and default the poll-cache path (P1) —
   two small diffs that close the only silent-loss path and make the README true.
4. Topic hygiene: explicit creation, `provider:incident_id` keys, delete orphans (P1).
5. Poller metrics (P2) — so the next issue of this class is visible in an hour
   rather than found by querying the index.
