"""Prometheus metrics shared by the pipeline workers.

Defined at module level on the default registry so the normalizer and embedder
each expose their own metrics when run as separate processes, and unit tests
can read observations without any HTTP server. Freshness buckets are sized for
the project's SLO story: the interesting range is sub-second to a few minutes.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

LATENCY_BUCKETS = (0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)

NORMALIZED_EVENTS = Counter(
    "freshet_normalizer_events_total",
    "Events validated and republished by the normalizer",
)
INVALID_EVENTS = Counter(
    "freshet_normalizer_invalid_total",
    "Payloads that failed validation (dead-letter topic arrives in M4)",
)
INGEST_LAG = Histogram(
    "freshet_ingest_lag_seconds",
    "Seconds from event time (ts) to pipeline receipt (ingested_at)",
    buckets=LATENCY_BUCKETS,
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


def start_metrics_server(port: int) -> None:
    """Expose /metrics on the given port; 0 disables (tests, library callers)."""
    if port:
        start_http_server(port)
