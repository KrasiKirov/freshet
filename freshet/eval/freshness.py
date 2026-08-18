"""The single Freshet measurement: end-to-end data staleness.

t0 is the provider's own posting time, NOT the moment we fetched the update. That
deliberately includes the poll wait we do not control, because it is the delay a
user actually experiences. Reporting fetch->queryable instead would flatter the
number by excluding its dominant term.

**Only live arrivals count.** An update posted three years ago and indexed during
a backfill has a staleness of three years, which says nothing about the pipeline.
The filter is self-calibrating: an update is LIVE if it was posted after we
started indexing, i.e. `ts >= min(indexed_at)`. Anything earlier was history we
caught up on, and scoring it measures when the pipeline was switched on rather
than how fast it is. Measured without this guard, a 24h window reported a mean
staleness of 41,995s and a ratio of 0.06 — streaming apparently LOSING to hourly
batch, purely from backfill.

Real status feeds are slow (~50 updates/day across 42 providers), so `n` grows
by roughly 2/hour and is reported alongside every figure.

Run (stack up, poller + stream + embedder running):
    python -m freshet.eval.freshness --since-minutes 120
"""
from __future__ import annotations

import argparse
import json
import math
import os

RESULTS = "results/freshness.json"
BATCH_INTERVAL_S = 3600.0     # the hourly-batch index we compare against


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile. `values` need not be pre-sorted."""
    if not values:
        raise ValueError("no values")
    vals = sorted(values)
    k = max(0, min(len(vals) - 1, math.ceil(p / 100 * len(vals)) - 1))
    return vals[k]


def streaming_staleness(posted_at: float, queryable_at: float) -> float:
    """Seconds from the provider posting an update to it being queryable."""
    return queryable_at - posted_at


def batch_staleness(posted_at: float, interval_s: float = BATCH_INTERVAL_S) -> float:
    """What the same update would have cost under a fixed batch cadence: it waits
    for the next refresh boundary after it was posted. Uniformly-arriving events
    therefore average interval/2 — the ~1800s hourly figure is a derivation from
    the cadence, not an estimate."""
    return interval_s - (posted_at % interval_s)


def summarize(streaming: list[float], batch: list[float]) -> dict:
    """Headline plus the distribution. The mean is what the ratio uses; the
    percentiles are there because a mean alone hides a long tail."""
    n = len(streaming)
    if n == 0:
        return {"streaming_mean_s": 0.0, "batch_mean_s": 0.0, "ratio": 0.0, "n": 0}
    s_mean = sum(streaming) / n
    b_mean = sum(batch) / len(batch)
    return {
        "streaming_mean_s": round(s_mean, 2),
        "streaming_p50_s": round(percentile(streaming, 50), 2),
        "streaming_p95_s": round(percentile(streaming, 95), 2),
        "batch_mean_s": round(b_mean, 2),
        "batch_interval_s": BATCH_INTERVAL_S,
        "ratio": round(b_mean / s_mean, 2) if s_mean else 0.0,
        "n": n,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure end-to-end staleness.")
    parser.add_argument("--since-minutes", type=float, default=None,
                        help="override: score updates posted within this window "
                             "instead of the automatic live-arrival filter")
    args = parser.parse_args()

    from freshet.common.db import connect

    conn = connect()
    try:
        if args.since_minutes is None:
            # Live arrivals only: posted after we began indexing.
            rows = conn.execute(
                """
                SELECT EXTRACT(EPOCH FROM ts)::float8,
                       EXTRACT(EPOCH FROM indexed_at)::float8
                FROM vector_records
                WHERE ts >= (SELECT min(indexed_at) FROM vector_records)
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT EXTRACT(EPOCH FROM ts)::float8,
                       EXTRACT(EPOCH FROM indexed_at)::float8
                FROM vector_records
                WHERE ts >= now() - (%(mins)s * interval '1 minute')
                """,
                {"mins": args.since_minutes},
            ).fetchall()
    finally:
        conn.close()

    streaming = [streaming_staleness(posted, indexed) for posted, indexed in rows]
    batch = [batch_staleness(posted) for posted, _ in rows]
    report = summarize(streaming, batch)
    report["filter"] = ("live arrivals (ts >= min(indexed_at))"
                    if args.since_minutes is None
                    else f"posted within {args.since_minutes} minutes")
    report["note"] = (
        "t0 = the provider's own posting time, so the poll wait we do not control "
        "is included. Only LIVE arrivals are scored (posted after indexing began); "
        "backfilled history would otherwise report the moment the pipeline was "
        "switched on rather than its speed. The batch arm is derived from the "
        "refresh cadence: uniformly-arriving events wait interval/2 on average."
    )

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
