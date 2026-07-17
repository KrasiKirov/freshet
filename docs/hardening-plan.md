# Hardening plan

Findings from a full project review (2026-07-15), and their status after two
fix passes. Theme of the review: the synthetic path is rigorous, but several
seams between it and the real-data path were broken or uncalibrated.

Validation (local, 2026-07-15): unit suite 230 passed, integration suite 25
passed against the live stack, `ruff check --select E9,F` clean, and
`make db-init` applied the migration to a pre-existing volume without incident.
The integration run caught one stale fixture: `test_resolve_posts_postmortem_once`
predated the flood guard (item 2) and inserted a resolved incident with no
`briefed_at`; the fixture now models the real opened→briefed→resolved flow, and
a new test locks in the guard (resolved-but-never-briefed → no postmortem).

## Fixed — pass 1 (real-data seams)

1. **Real incidents never resolved.** `correlate()` only closed incidents on the
   synthetic `healthy` event, while the status poller passes through raw
   Statuspage statuses. `RESOLUTION_TYPES` now includes `resolved`/`postmortem`,
   so live incidents close and the autopilot postmortem fires on real data.
2. **Postmortem flood guard.** The postmortem claim requires `briefed_at IS NOT
   NULL`, so historical already-resolved incidents replayed on the first poll
   never trigger postmortems for briefs that don't exist.
3. **`make replay` dimension mismatch.** Defaulted to MiniLM (384-dim) against
   the `vector(768)` schema. Now bge; the stale dim comment in `db/init.sql` is
   corrected.
4. **Abstention floor per-embedder.** MiniLM/stub 0.3, bge now **0.7** —
   calibrated empirically with `scripts/calibrate_abstention.py` (2026-07-15):
   on-corpus queries score ≥ 0.735 while the hardest off-corpus negative hits
   0.662, so 0.7 (the gap midpoint) gives 0/40 false abstentions and 16/16
   correct ones. The interim guess of 0.5 let 6/8 ops-flavored hard negatives
   through. `FRESHET_MIN_SIMILARITY` still overrides.
5. **Recency decay configurable.** `FRESHET_TAU_S` overrides the demo-tuned
   21-minute half-weight. *Open: benchmark runs recency-neutral while production
   applies decay — add a recency-on eval arm or pick a real-feed default.*
6. **Incident-scoped agent.** `investigate(since=…)`; the autopilot passes
   `opened_at − 2h`, applied as the default `search` lower bound.
7. **API polish.** `/query` hits expose `type`; `get_deps` init is lock-guarded.

## Fixed — pass 2 (deferred items resolved)

8. **The open ablation is implemented — and run (2026-07-15).** `make agent-eval`
   now has a keyless, deterministic `fixed-two-step` arm: the identical
   whole-corpus search, then anchor on the top spike hit and call
   `events_around` — the agent's temporal lookup with zero LLM. Result:
   fixed-two-step scores **1.000/1.000, exactly matching the agent**; the whole
   lift over single-shot (0.167/0.417) is the retrieval capability, not agency.
   Three-arm table published in RESULTS.md (M11).
9. **Correlate race fixed atomically.** `primary_service` + `auto_opened`
   columns and a partial unique index (one open auto incident per service);
   stray severe events find-or-create via `INSERT … ON CONFLICT`, so concurrent
   normalizers are safe. Explicit-id incidents (status feeds can legitimately
   have several open per service) are exempt. The single-writer caveat is gone.
10. **Producer batching.** `BufferedProducer` + `consume_loop(commit_every,
    pre_commit)`: the normalizer produces without per-message flushes and
    flush-checks the batch before offsets commit (`--commit-every`, default 100
    on the CLI; library default stays 1). At-least-once preserved: a crash
    redelivers at most one batch, idempotent upserts absorb duplicates.
    Quantified (2026-07-15, `make scale-demo`): embedder scaling is now
    near-linear (26→84 ev/s, 3.2× at 3 workers, bge) and a stub-embedder run
    puts the non-embedding pipeline at **834 ev/s** — the stage that previously
    capped at ~100. RESULTS.md M4 updated. `scripts/run_scaling_demo.sh` also
    had the item-3 bug (defaulted to 384-dim MiniLM against the 768 schema);
    its default is now bge.
11. **Resilient DB connections.** `connect()` returns a wrapper that retries
    connection-level failures (OperationalError/InterfaceError) with reconnect
    + backoff, bounded at 3 attempts. Query-level errors still raise
    immediately. Covers the API process and both workers.
12. **Prompt-injection hardening.** The composer, narrative, and agent system
    prompts now declare event text untrusted and forbid following instructions
    inside it. *Open: structural delimiting of evidence and an output check
    would strengthen this further; instruction-level hardening is not a
    guarantee.*
13. **CI lint.** `ruff check --select E9,F` added to CI; the 11 pre-existing
    unused imports it caught are fixed.
14. **Poller dedup.** `poll_once(seen=…)` suppresses re-producing updates
    already sent; the poll loop keeps the set across cycles.

## Still deferred (ordered by value)

1. **Real-data validation set.** The benchmark is co-designed with the
   generator; the synthesis eval saturates at 1.0. A small hand-labeled corpus
   (20–30 real incidents from public postmortems or replayed status-feed
   timelines) is the single highest-value addition. Needs curation, not code.
2. ~~**Run the new evals.**~~ Done 2026-07-15: three-arm table in RESULTS.md
   (item 8), bge floor calibrated to 0.7 (item 4), scale-demo re-run (item 10).
3. **Incident↔service join table.** The arrays (`services`, `event_ids`) still
   block FK integrity and efficient lookups; the race they caused is fixed, so
   this is now modeling cleanup rather than a correctness issue.
4. **API connection pool.** The resilient wrapper fixes the bricking failure
   mode; a `psycopg_pool` would additionally remove request serialization under
   concurrency. Demo-scale priority: low.
5. ~~**Embed-failure dead-lettering.**~~ Done 2026-07-17: `make_handler` retries
   encode 3× inline, then dead-letters the message; upsert failures still
   propagate (infrastructure, not poison — dead-lettering during a DB outage
   would drain the stream into the DLQ).
6. ~~**CI integration job.**~~ Done 2026-07-17: a second CI job brings up the
   compose stack and runs the integration suite under the stub embedder
   (`FRESHET_TEST_EMBEDDER`); embedding-semantics tests skip via
   `importorskip("sentence_transformers")`. Verified in a CI-simulated venv
   (`.[test]` only) — which also caught `run_eval` importing matplotlib at
   module scope (now lazy). Widening ruff / adding mypy still open.

Also done 2026-07-17: **minilm retired from live paths** — its 384-dim vectors
cannot index into the `vector(768)` schema, so `make_embedder("minilm")` now
fails fast with a clear message (frozen baseline JSON + RESULTS history remain);
`run_demo.sh` no longer defaults to it (the item-3 bug class, fourth instance).

## Migration note

`db/init.sql` stays idempotent; `make db-init` on an existing volume adds the
new columns/index. Existing rows get `auto_opened = false`, so the partial
unique index cannot fail on legacy data (pre-existing duplicate open incidents
are exempt from the constraint until resolved).
