# Build Brief — Beacon: Real-Time Incident Intelligence

*Hand this to Claude Code as the project brief. It is self-contained: it tells you what to build, why, how, the exact build order, and how to know each step is done. Read it fully before writing code. Working name "Beacon" — rename freely.*

---

## 1. What we are building (in one paragraph)

A **freshness-first streaming-RAG system** for on-call engineers. It continuously ingests a live stream of operational events (alerts, deploys, metrics, incident-chat, postmortems) through Kafka, embeds and indexes them into a vector store within seconds, and serves a retrieval-augmented query API that answers questions like *"what's happening with `scheduler-api`?"* or *"what usually resolves this error pattern?"* with **cited, timestamped, recency-aware** answers. The entire system is organized around one measurable property — **event-to-queryable latency (freshness)** — and ships with an evaluation harness that proves it.

This is a **portfolio project** aimed at distributed-systems / backend / platform roles. It must be runnable by anyone with no real company data and no paid API key for the core path, and it must produce honest, reproducible eval numbers.

---

## 2. Why (purpose, value, and the bar)

**The problem.** During an incident, engineers waste most of their time *reconstructing context* — what changed, what's related, what fixed something similar before — from data scattered across many tools, under pressure. The information that matters most is the newest, which is exactly what a nightly-batch index misses.

**The value.** A knowledge layer that is current to within seconds and answers with citations + timestamps cuts that reconstruction time.

**Why this is worth building as a portfolio piece.** It demonstrates four things that infra-heavy employers actually probe: (a) designing an event-driven pipeline (topics, partitions, consumer groups, lag, delivery semantics, backpressure); (b) retrieval that works *and is measured*; (c) treating freshness/latency as an SLO with real numbers; (d) shipping something reproducible.

**The bar — read this twice.** Using Kafka and RAG is the *floor*; by 2026 that combination is common and proves nothing on its own. What makes this stand out is **rigor**: a measured freshness SLO, a retrieval-quality eval on a labeled set, and an honest streaming-vs-batch comparison that proves the recency claim. Build for the reviewer who reads the eval results, not the one who reads the dependency list. Never inflate a result; every claimed number must be reproducible from committed artifacts.

---

## 3. Non-negotiable principles (apply these throughout)

1. **Freshness is the spine.** Instrument event-to-queryable latency from day one. It is the headline metric and the demo's centerpiece. Do not bolt it on later.
2. **Kafka and RAG must each be load-bearing, and you must be able to say why.**
   - *Kafka:* continuous multi-source high-volume stream; embedding workers scale independently as a consumer group; durable replay re-indexes history after a model/chunking change; partition-by-service preserves ordering. Not a queue stand-in, not a cron job.
   - *RAG:* semantic retrieval over a large, constantly-changing unstructured corpus, grounding an LLM with citations and recency. Fine-tuning can't track a minute-by-minute corpus; RAG can.
   - Where it is *not* RAG: hard structured links (this alert → that deploy) are SQL joins on ids/timestamps, not vector search. Use vectors for the semantic/fuzzy parts and SQL for the structured parts, and keep the split explicit.
3. **Eval is a first-class component, not an afterthought.** It is the differentiator.
4. **Scope discipline.** The UI is a thin wrapper; the value is the pipeline + eval. Resist UI scope-creep. Anything in the "out of scope" list (§7) stays out of v1.
5. **Reproducible and shareable.** `docker-compose up` + a demo command must work with synthetic data, local embeddings, and no paid key for the core path. Commit eval results and plots.
6. **Honest engineering.** At-least-once delivery with idempotent upserts (don't reach for exactly-once). Route failures to a dead-letter topic. Say plainly in the README that retrieval-quality numbers on synthetic data are indicative, not a real-world benchmark.

---

## 4. How it works (architecture)

```
 synthetic generator ──produce──▶  Kafka: raw.events (partition key = service)
                                        │
                                        ▼
                         normalizer/enricher (consumer group)
                           - validate → canonical Event
                           - correlate events to incidents
                           - write incident state/timeline → Postgres
                           - stamp ingested_at
                                        │ produce
                                        ▼
                              Kafka: normalized.events
                                        │
                                        ▼
                          embedding workers (consumer group, scalable)
                           - chunk long text, embed
                           - stamp indexed_at
                           - idempotent upsert → pgvector (+ metadata)
                           - failures → dead-letter

 FastAPI query service:  hybrid retrieval (vector + keyword + metadata filters)
                          → recency weighting → optional rerank
                          → LLM grounding with citations + "as of" timestamps
                          → abstain when retrieval is weak

 Storage:  Postgres + pgvector  (one datastore: structured state + vectors)
 Metrics:  consumer lag, ingest latency, retrieval latency exposed
```

**The data contract (build exactly this — everything depends on it).** A canonical `Event` carries three timestamps that all freshness math derives from:
- `ts` — when the event occurred
- `ingested_at` — when the pipeline received it
- `indexed_at` — when it became retrievable

Plus: `event_id`, `service`, `source` (alert|deploy|metric|chat|postmortem), `type`, `severity?`, `incident_id?`, `text`, `structured` (dict), `refs` (list). An `Incident` aggregates `event_ids`, `services`, `opened_at`, `resolved_at?`, `resolution_summary?`. A `VectorRecord` carries `chunk_id`, `event_id`, `incident_id?`, `service`, `ts`, `indexed_at`, `text`, `source` (embedding stored in the pgvector column).

> A working Phase 0 reference implementation of the schema + synthetic generator already exists (pydantic models, a scripted incident scenario, JSONL + Kafka sinks, deterministic under a seed, 10 passing tests). If provided alongside this brief, use it as the starting point and keep the contract stable. If not provided, build it to the spec above first.

---

## 5. Tech stack (chosen for shareability + signal)

- **Broker:** Redpanda (Kafka-API compatible, single container, easy local) or Kafka in KRaft mode — via `docker-compose`. Concepts (topics, partitions, consumer groups, offsets, lag) are identical.
- **Language:** Python 3.12. `confluent-kafka` (clients), `pydantic` v2 (schemas), `FastAPI` (API).
- **Vector + state:** Postgres + `pgvector` (one datastore; enables metadata-filtered hybrid retrieval). Qdrant is the scale-up alternative.
- **Embeddings:** `sentence-transformers` local default (no key needed) behind a pluggable interface; optional API embeddings.
- **LLM:** pluggable (OpenAI/Anthropic) behind an interface; the pipeline and retrieval eval must run *without* it.
- **Consumers:** plain Python consumer-group apps (keep it simple).
- **Infra/quality:** `docker-compose` for the full stack; `pytest`; GitHub Actions CI.

Before adding any dependency not listed here, stop and justify it.

---

## 6. Build order (phases, each with a definition of done)

Work phase by phase. Do not start a phase before the previous one's "done" criteria are met. Write tests as you go; keep CI green.

**Phase 0 — Foundation.** Repo scaffold + shared `common/` package; canonical `Event`/`Incident`/`VectorRecord` schemas (with the three freshness timestamps); synthetic generator with background noise + one scripted, coherent incident (deploy → error spike → chat triage → rollback → recovery → postmortem), deterministic under a seed, with a JSONL sink (testable, no broker) and a Kafka sink; `docker-compose` (Redpanda + Postgres/pgvector); a produce→consume hello-world that validates events; tests + CI.
*Done when:* `docker-compose up` starts the broker + Postgres; the generator produces to Kafka and the hello-world consumer prints validated events; `pytest` is green and the generator is byte-reproducible under a fixed seed.

**Phase 1 — Streaming ingestion + embedding.** Normalizer consumer (validate, correlate to incidents, write state to Postgres, stamp `ingested_at`, republish to `normalized.events`); embedding worker consumer group (chunk, embed, stamp `indexed_at`, idempotent upsert to pgvector); dead-letter handling; freshness timestamps flowing through; lag/latency metrics.
*Done when:* events flow generator → Kafka → normalizer → embedder → pgvector and become retrievable; you can query pgvector and see records with all three timestamps; end-to-end latency is measurable; scaling the embedder consumer group increases throughput.

**Phase 2 — RAG query layer.** FastAPI `POST /query`; hybrid retrieval (vector + keyword + metadata filters); recency weighting; optional rerank; LLM grounding with citations + "as of" timestamps; abstention on weak retrieval; thin UI (query box, live event feed, freshness/lag gauge).
*Done when:* a question returns a grounded answer citing specific events with timestamps; recency-sensitive queries surface the newest relevant events; the UI shows live ingestion and current freshness.

**Phase 3 — Evaluation harness (the differentiator).** Build a labeled set of `(query, relevant_event_ids)` over the synthetic data (you control ground truth because the incidents are authored). Report: freshness (event→queryable p50/p95/p99); **streaming-vs-batch freshness comparison** (the money graph); retrieval quality (`recall@k`, `precision@k`, `MRR`, `nDCG`) comparing keyword-only / vector-only / hybrid; answer groundedness + correctness on a small gold set; throughput + consumer lag under burst. Dump all results to committed JSON + plots.
*Done when:* `eval/run_eval.py` reproduces every headline number; the streaming-vs-batch freshness plot exists; hybrid retrieval is shown to beat both single-mode baselines (or the cases where it doesn't are reported honestly).

**Phase 4 — Polish for sharing.** README that leads with results (problem → architecture diagram → why-Kafka/why-RAG → run steps → RESULTS with graphs → limitations → future work); a `make demo` that replays the scripted incident; CI; honest limitations section. Optionally a short writeup post built around the freshness graph and the `recall@k` table.
*Done when:* a stranger can clone, `docker-compose up`, run the demo, and read real eval numbers, in minutes.

---

## 7. Scope guardrails

**Out of scope for v1 (do not build these):** real enterprise connectors / OAuth / MCP servers (the synthetic generator stands in); multi-tenancy, auth/SSO, RBAC; agents that take actions (the system answers; humans act); executable-skill induction; closed-loop "flag when X is wrong" monitoring.

**Future phases (note as roadmap, don't build now):** real connectors via webhooks/MCP; executable response-skill induction from the accumulated corpus; closed-loop monitoring. These lean on joins/induction/monitoring, not RAG, so they sit beside the core, not inside it.

---

## 8. Acceptance criteria for the whole project

- One command brings up the stack; one command runs the demo; the core path needs no paid API key.
- The pipeline sustains a target event rate with **p95 event-to-queryable latency in the low seconds**, measured and graphed.
- A **streaming-vs-batch** plot demonstrates streaming reduces answer staleness by orders of magnitude.
- Hybrid retrieval beats vector-only and keyword-only on a labeled set, with `recall@k` reported.
- Answers cite specific, timestamped events and abstain when evidence is thin.
- `pytest` + CI green; eval results and plots committed and reproducible.
- README explains *why Kafka* and *why RAG* in load-bearing terms.

---

## 9. Working agreements for the coding agent

- Test as you build; keep CI green; small, reviewable commits per logical unit.
- Keep the `Event` data contract stable once Phase 0 is set — downstream depends on it.
- At-least-once + idempotent upserts; failures to dead-letter; never silently drop.
- Don't expand the UI beyond the thin spec. Don't add dependencies beyond §5 without justification.
- When a result is weak or a baseline wins, report it honestly in the eval — negative results are still results and read as maturity.
- If a design choice is genuinely ambiguous, state the assumption in a comment and proceed; don't stall.

---

## 10. Start here

1. Confirm/establish Phase 0 (use the reference implementation if provided, else build to §4's contract).
2. Stand up `docker-compose` (Redpanda + Postgres/pgvector) and prove produce→consume→validate.
3. Then proceed to Phase 1. Do not skip ahead to the query layer before ingestion + embedding are flowing and freshness is measurable.

Build the freshness SLO and the eval first-class. That is what turns this from "I used Kafka and RAG" into "I built a streaming retrieval system and proved it's fresh."
