"""Streaming-vs-batch data-staleness model — the money graph's math.

Staleness at query time t = t - (timestamp of the newest event that is queryable
by t). Streaming makes an event queryable ~freshness seconds after it occurs; a
periodic batch indexer makes it queryable only at the next batch boundary. Inputs
are epoch-second floats; the caller (run_eval.staleness_curves) feeds a steady
synthesized event stream at the generator's cadence — the comparison isolates
ingestion cadence, and the batch schedule is modeled (we do not wait a real
night)."""

from __future__ import annotations

import math


def batch_queryable_at(event_ts: list[float], interval_s: float, t0: float) -> list[float]:
    """Each event becomes queryable at the next batch boundary at or after it."""
    out = []
    for ts in event_ts:
        n = math.ceil((ts - t0) / interval_s)
        out.append(t0 + n * interval_s)
    return out


def streaming_queryable_at(event_ts: list[float], freshness_s: float) -> list[float]:
    """Each event becomes queryable freshness_s after it occurs."""
    return [ts + freshness_s for ts in event_ts]


def staleness_at(t: float, event_ts: list[float], queryable_at: list[float]) -> float | None:
    """t minus the newest event-ts whose data is queryable by t; None if nothing
    is queryable yet."""
    newest = None
    for ts, q in zip(event_ts, queryable_at, strict=True):
        if q <= t and (newest is None or ts > newest):
            newest = ts
    return None if newest is None else t - newest


def staleness_series(
    event_ts: list[float], queryable_at: list[float], sample_times: list[float]
) -> list[float | None]:
    return [staleness_at(t, event_ts, queryable_at) for t in sample_times]
