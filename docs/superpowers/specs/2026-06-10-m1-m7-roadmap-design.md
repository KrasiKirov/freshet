# Freshet Roadmap Design — Milestones M1–M7

**Date:** 2026-06-10
**Status:** Approved
**Supersedes:** the phase sequencing in `freshet/BRIEF.md` §6 (content preserved, order changed). Architecture (§4), data contract, tech stack (§5), scope guardrails (§7), and acceptance criteria (§8) of the brief remain binding.

## Context and motivation

Phase 0 is complete: data contract, deterministic generator, Redpanda + pgvector compose stack, hello-world consumer, 10 tests, CI. A project review (2026-06-10) identified the main risk as execution distance — the differentiator (the eval harness) sits three strictly-gated phases away, and the headline metric (event-to-queryable freshness) is unmeasurable until ingestion is fully built.

Decisions made during brainstorming:

1. **Thin vertical slice first.** Minimal versions of normalizer → embedder → query reach pgvector and a measurable freshness number as early as possible; each stage is hardened afterwards.
2. **Observability lands immediately after the slice.** Prometheus + Grafana with one dashboard, so all later work develops against live gauges.
3. **All credibility extras in scope:** documented failure drills, a real batch-indexer baseline for the streaming-vs-batch comparison, and repo hygiene (LICENSE, packaging, untracking the gitignored brief).
4. **LLM answer-composer included** using the Anthropic API behind a pluggable interface. The core path (pipeline, retrieval, every eval number except answer-groundedness) remains fully keyless; a template composer is the no-key default.
5. **Plan shape:** seven sequential milestones replacing the brief's phase gates. Brief Phases 1–4 map to M4–M7.

## Milestones

### M1 — Hygiene & packaging

- Move code from `freshet/freshet/` to the repo root as an installable `freshet` package with `pyproject.toml`. Imports become `freshet.common.schemas` etc.; `PYTHONPATH=.` is retired.
- Add MIT `LICENSE`.
- `git rm --cached BRIEF_for_Claude_Code.md` (it is gitignored but still tracked).
- Update Makefile, CI, and README paths.

*Done when:* `pip install -e .` works, all existing tests pass, `make up && make smoke` still passes.

### M2 — Vertical slice (keystone)

Minimal but real versions of every remaining pipeline stage, end to end:

- **First Postgres migration:** enable `pgvector`; create `vector_records` (chunk_id PK, event_id, incident_id, service, ts, indexed_at, source, text, embedding vector). The `incidents` table waits for M4.
- **Normalizer (minimal):** consume `raw.events`, validate to `Event`, stamp `ingested_at`, republish to `normalized.events`. No incident correlation yet.
- **Embedder (minimal):** consume `normalized.events`, embed with local `sentence-transformers/all-MiniLM-L6-v2`, stamp `indexed_at`, idempotent upsert via `ON CONFLICT`. Offsets committed only after a successful upsert (at-least-once + idempotency).
- No chunking yet — generator events are short. Chunking arrives in M4 with postmortems.
- **Query (rough):** FastAPI `POST /query` doing vector-only top-k with optional service/time filters. Deliberately minimal; exists to prove the slice.
- **Freshness report:** a script reading `indexed_at − ts` from `vector_records`, printing p50/p95/p99. The headline metric exists from this milestone onward.

*Done when:* one make target runs generator → Kafka → normalizer → embedder → pgvector; a query returns relevant events with all three timestamps; the freshness report prints real percentiles.

### M3 — Observability

- `prometheus_client` exporters in normalizer and embedder: events-processed counters, ingest/index latency histograms, dead-letter counter (placeholder until M4).
- Prometheus scrapes the workers plus **Redpanda's built-in metrics endpoint** (consumer lag comes free; no extra exporter).
- Grafana with one provisioned dashboard, committed as JSON: freshness percentiles, consumer lag per group, throughput, dead-letter count.
- Two additional compose services, profile-gated so the lean stack stays available.

*Done when:* during a generator run the dashboard shows freshness and lag moving live.

### M4 — Ingestion hardening (completes brief Phase 1)

- Incident correlation in the normalizer; `incidents` table and state writes.
- Dead-letter topic with a poison-message test; never silently drop.
- Chunking for long texts (postmortems).
- Graceful shutdown; replay support (re-consume from offset 0 to re-index the corpus).
- Demonstrate consumer-group scaling: 1 → 3 embedder instances raises throughput (recorded; becomes drill material in M7).

*Done when:* brief Phase 1 done-criteria hold and the dashboard reflects dead-letters and scaling effects.

### M5 — Full query layer (brief Phase 2)

- Hybrid retrieval: pgvector cosine + Postgres full-text (`tsvector`) + metadata filters, fused with reciprocal-rank fusion.
- Exponential recency decay on fused scores.
- Abstention when top scores fall below a calibrated threshold.
- **Answer composer** behind a pluggable interface: Anthropic implementation writes grounded answers with `[event_id @ timestamp]` citations and honors abstention; a template/extractive composer is the no-key default. The repo runs fully without a key.
- Thin UI: one static page — query box, live event feed, freshness gauge (reads Prometheus). No framework, no scope creep.
- No rerank model in v1; the interface point is left open.

*Done when:* brief Phase 2 criteria hold; with `ANTHROPIC_API_KEY` set answers are LLM-composed, without it template-composed; weak retrieval abstains in both modes.

### M6 — Eval harness + batch baseline (brief Phase 3, the differentiator)

- Labeled set `(query, relevant_event_ids)` authored alongside the scripted scenarios (ground truth is known because the incidents are authored).
- Retrieval metrics: recall@k, precision@k, MRR, nDCG for keyword-only / vector-only / hybrid. Cases where hybrid loses are reported honestly.
- Freshness eval: p50/p95/p99 event→queryable at a sustained target rate, plus behavior under burst (throughput, consumer lag).
- **Batch baseline:** a real cron-style batch indexer over the same events into a separate table/namespace, run at configurable intervals; staleness measured as query-time minus newest-indexed-event. Produces the **streaming-vs-batch comparison graph** — the project's money graph.
- Small answer-groundedness check on a gold set (runs only when a key is present).
- All results dumped to committed JSON + matplotlib plots; `eval/run_eval.py` reproduces every headline number.

*Done when:* brief Phase 3 criteria hold and the streaming-vs-batch plot is committed.

### M7 — Failure drills + polish (brief Phase 4, extended)

Three documented drills, each with a captured dashboard graph:

1. Kill the embedder mid-stream → lag grows; restart → lag drains, no data loss.
2. Switch the embedding model → replay the topic → corpus re-indexed (durable-replay claim made real).
3. 10× generator burst → backpressure behavior observed and described.

Then polish: README rewritten to lead with results (problem → architecture diagram → why-Kafka/why-RAG → run steps → RESULTS with graphs → limitations → future work); `make demo` replays the scripted incident; honest limitations section; optional short writeup built around the freshness graph and the recall@k table.

*Done when:* every acceptance criterion in brief §8 checks off.

## Testing & CI strategy

- Unit tests use a **stub embedder** (deterministic fake vectors) so CI never downloads model weights; the current broker-less CI job is kept.
- Integration tests (real broker + Postgres via compose) live behind `make test-integration`, run locally and as a separate optional CI job.
- Every milestone lands with its tests; CI green is a merge condition throughout.

## Risks

- **CPU embedding latency** is the freshness bottleneck. Mitigation order: batch-encode multiple events per poll, then scale consumer instances.
- **Grafana provisioning is fiddly.** Commit dashboard JSON and provisioning YAML from the first M3 commit.
- **Model download (~90 MB)** stays out of CI via the stub embedder.

## Post-M7 candidate: M8 — one real connector

Not part of this roadmap's scope, recorded as the designated next step after M7. A single webhook connector (GitHub is the natural first: CI failures, pushes, releases from the author's active repos) translating real payloads into the canonical `Event` contract and producing to `raw.events`. The pipeline downstream works unchanged. This converts the project from a demonstration on synthetic data into something personally usable ("what broke in this repo recently, and what fixed the last similar failure?") and lets the README show the system running on real data. Aligns with brief §7's "future phases: real connectors via webhooks". Build only after M6's eval exists — the eval remains the differentiator.

## Out of scope (unchanged from brief §7)

Real connectors/OAuth/MCP, multi-tenancy/auth/RBAC, action-taking agents, skill induction, closed-loop monitoring, heavyweight orchestrators (Airflow/Dagster/Spark/Flink), full OpenTelemetry tracing, alerting rules.
