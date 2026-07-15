# Hardening plan

Findings from a full project review (2026-07-15), and their status after two
fix passes. Theme of the review: the synthetic path is rigorous, but several
seams between it and the real-data path were broken or uncalibrated.

Validation: the unit suite (227 passed, 1 skipped) and the CI lint selection
(`ruff check --select E9,F`) were run against these changes in a sandbox clone
(under Python 3.10 — the sandbox lacks 3.12; re-run `make test` locally).
Integration tests (`make test-integration`) still need a local run against the
stack, since they require Docker.

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
4. **Abstention floor per-embedder.** MiniLM/stub 0.3, bge 0.5 (bge's cosine
   range is compressed — 0.3 never abstained), `FRESHET_MIN_SIMILARITY`
   override. *Open: calibrate the bge value empirically (off-corpus queries in
   the benchmark).*
5. **Recency decay configurable.** `FRESHET_TAU_S` overrides the demo-tuned
   21-minute half-weight. *Open: benchmark runs recency-neutral while production
   applies decay — add a recency-on eval arm or pick a real-feed default.*
6. **Incident-scoped agent.** `investigate(since=…)`; the autopilot passes
   `opened_at − 2h`, applied as the default `search` lower bound.
7. **API polish.** `/query` hits expose `type`; `get_deps` init is lock-guarded.

## Fixed — pass 2 (deferred items resolved)

8. **The open ablation is implemented.** `make agent-eval` now has a keyless,
   deterministic `fixed-two-step` arm: the identical whole-corpus search, then
   anchor on the top spike hit and call `events_around` — the agent's temporal
   lookup with zero LLM. *Open: run it against the committed benchmark and
   publish the three-arm table in RESULTS.md.*
9. **Correlate race fixed atomically.** `primary_service` + `auto_opened`
   columns and a partial unique index (one open auto incident per service);
   stray severe events find-or-create via `INSERT … ON CONFLICT`, so concurrent
   normalizers are safe. Explicit-id incidents (status feeds can legitimately
   have several open per service) are exempt. The single-writer caveat is gone.
10. **Producer batching.** `BufferedProducer` + `consume_loop(commit_every,
    pre_commit)`: the normalizer produces without per-message flushes and
    flush-checks the batch before offsets commit (`--commit-every`, default 100
    on the CLI; library default stays 1). At-least-once preserved: a crash
    redelivers at most one batch, idempotent upserts absorb duplicates. This
    removes the measured ~100 ev/s flush-per-event ceiling — re-run
    `make scale-demo` to quantify.
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
2. **Run the new evals.** The `fixed-two-step` numbers, the recalibrated bge
   abstention floor, and a `make scale-demo` re-run post-batching are all one
   local session away — the code paths are in place.
3. **Incident↔service join table.** The arrays (`services`, `event_ids`) still
   block FK integrity and efficient lookups; the race they caused is fixed, so
   this is now modeling cleanup rather than a correctness issue.
4. **API connection pool.** The resilient wrapper fixes the bricking failure
   mode; a `psycopg_pool` would additionally remove request serialization under
   concurrency. Demo-scale priority: low.
5. **Embed-failure dead-lettering.** Transient DB errors now retry (item 11),
   but a poison message that repeatedly fails embedding still crash-loops;
   dead-letter after N handler failures.
6. **CI integration job.** docker compose works in GitHub Actions; run the
   integration suite with the stub embedder to keep it fast. Also consider
   widening ruff and adding mypy.

## Migration note

`db/init.sql` stays idempotent; `make db-init` on an existing volume adds the
new columns/index. Existing rows get `auto_opened = false`, so the partial
unique index cannot fail on legacy data (pre-existing duplicate open incidents
are exempt from the constraint until resolved).
