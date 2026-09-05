"""Prometheus metrics shared by the pipeline workers.

Defined at module level on the default registry so unit tests can read
observations without any HTTP server. Note: because the module defines the
full metric set at import, BOTH workers' endpoints expose every metric —
each worker only increments its own, the rest sit at zero. Dashboards must
therefore aggregate with sum() across instances (which is also what scaled
multi-instance workers will need). Freshness buckets are sized for the
project's SLO story: the interesting range is sub-second to a few minutes.
"""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger(__name__)

LATENCY_BUCKETS = (0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)

DEADLETTER_EVENTS = Counter(
    "freshet_deadletter_total",
    "Messages routed to the dead-letter topic (normalizer + embedder)",
)

EMBEDDER_MESSAGES = Counter(
    "freshet_embedder_messages",
    "Kafka messages successfully indexed (one per update, not per chunk)",
)

INDEXED_EVENTS = Counter(
    "freshet_embedder_events_total",
    "Events embedded and upserted into pgvector",
)
FRESHNESS = Histogram(
    "freshet_freshness_seconds",
    "Event->queryable freshness: seconds from ts to indexed_at",
    buckets=LATENCY_BUCKETS,
)
PIPELINE_LATENCY = Histogram(
    "freshet_pipeline_latency_seconds",
    "Pipeline latency: seconds from ingested_at to indexed_at",
    buckets=LATENCY_BUCKETS,
)


LLM_CALLS = Counter(
    "freshet_llm_calls_total",
    "LLM compose calls admitted by the budget",
)
LLM_SECONDS = Histogram(
    "freshet_llm_seconds",
    "Wall-clock seconds per LLM compose call",
    buckets=LATENCY_BUCKETS,
)
LLM_TRUNCATED = Counter(
    "freshet_llm_truncated_total",
    "Responses that hit max_tokens instead of finishing",
)
DROPPED_CITATIONS = Counter(
    "freshet_dropped_citations_total",
    "Citations stripped because their event_id was not in the evidence",
)
ABSTENTIONS = Counter(
    "freshet_retrieval_abstentions_total",
    "Queries where the best cosine fell below the calibrated floor",
)


POLL_FETCH = Counter(
    "freshet_poll_fetch_total",
    "Feed fetches by provider and outcome (200 / 304 / error / skipped)",
    ["provider", "status"],
)
POLL_UPDATES = Counter(
    "freshet_poll_updates_parsed_total",
    "Updates parsed out of a feed body, by provider",
    ["provider"],
)
POLL_BACKOFF_HOSTS = Gauge(
    "freshet_poll_backoff_hosts",
    "Hosts currently skipped because they are backing off",
)
POLL_SWEEP_SECONDS = Histogram(
    "freshet_poll_sweep_seconds",
    "Wall time for one sweep over every feed",
    buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)

TIMESTAMP_FALLBACK = Counter(
    "freshet_timestamp_fallback_total",
    "Updates whose HTML timestamp could not be resolved and used the entry's revision time",
)

UNKNOWN_WIRE_VERSION = Counter(
    "freshet_unknown_wire_version_total",
    "Events carrying a wire version this worker does not know",
)


def start_metrics_server(port: int) -> None:
    """Expose /metrics on the given port; 0 disables (tests, library callers).

    A port already in use is logged and ignored rather than raised: metrics are
    observability, and a second worker (or a stale one holding the port) must not
    stop this one from indexing. The worker's actual job does not depend on it.
    """
    if not port:
        return
    try:
        start_http_server(port)
    except OSError as exc:
        log.warning("metrics server disabled: port %d unavailable (%s)", port, exc)
