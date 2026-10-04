"""Measure provider timestamp to acknowledged index completion.

Version 2 uses event_indexing.first_queryable_at, recorded after all chunk
writes have completed in autocommit mode. This is a conservative upper bound
on first visibility. Replays preserve it and update last_queryable_at instead.
Legacy rows have no first-index receipt and are excluded from the benchmark.

By default, score only provider updates posted in the current embedder-heartbeat
span. --since-minutes explicitly overrides that live-arrival window. This is an
embedder liveness signal, not proof of uptime for every upstream component.
n counts distinct events; n_rows counts their chunks. The hourly batch arm is
modeled over every boundary phase; it is not an independently deployed system.

Run: make freshness
"""
from __future__ import annotations

import argparse
import json
import math
import os
from datetime import UTC, datetime

RESULTS = "results/freshness.json"
BATCH_INTERVAL_S = 3600.0     # the hourly-batch index we compare against
ALIGNMENT_COUNT = int(BATCH_INTERVAL_S)  # one alignment per second-offset the
                                          # boundary could sit at


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


def batch_staleness(posted_at: float, interval_s: float = BATCH_INTERVAL_S,
                     phase_s: float = 0.0) -> float:
    """What the same update would have cost under a fixed batch cadence: it waits
    for the next refresh boundary after it was posted. `phase_s` is where that
    boundary sits within the interval (0 = HH:00:00 exactly) — one arbitrary
    choice out of `interval_s` possible second-offsets. Uniformly-arriving events
    average interval/2 at any single phase; see `batch_alignment_sweep` for the
    figure that does not depend on picking one."""
    return interval_s - ((posted_at - phase_s) % interval_s)


def batch_alignment_sweep(posted_ats: list[float], interval_s: float = BATCH_INTERVAL_S,
                           n_alignments: int = ALIGNMENT_COUNT) -> list[float]:
    """Mean batch wait at each of `n_alignments` boundary phases across the
    interval. One value per phase — this is the distribution that a single
    offset (e.g. phase 0) draws one arbitrary sample from. Arrivals clustered at
    one phase (real ones cluster at the top of the hour) inflate that phase's
    mean without moving this distribution's own mean, which is what makes the
    swept average alignment-independent rather than a best or worst case."""
    if not posted_ats:
        return []
    step = interval_s / n_alignments
    n = len(posted_ats)
    return [
        sum(batch_staleness(p, interval_s, phase_s=k * step) for p in posted_ats) / n
        for k in range(n_alignments)
    ]


def summarize(streaming: list[float], posted_ats: list[float],
              interval_s: float = BATCH_INTERVAL_S) -> dict:
    """Headline plus the distribution. The batch arm is alignment-independent:
    it is the mean of `batch_alignment_sweep`, not one phase's number, so it
    cannot be inflated or flattered by which second-offset the workload happens
    to cluster against. min/median/max of that same sweep are reported alongside
    so the sensitivity to alignment is visible; `batch_mean_s_top_of_hour` is the
    single HH:00:00-aligned figure this replaces, kept for comparison."""
    n = len(streaming)
    if n == 0:
        return {"streaming_mean_s": 0.0, "batch_mean_s": 0.0, "ratio": 0.0, "n": 0}
    s_mean = sum(streaming) / n
    sweep = batch_alignment_sweep(posted_ats, interval_s)
    b_mean = sum(sweep) / len(sweep)
    b_min = min(sweep)
    b_max = max(sweep)
    b_median = percentile(sweep, 50)
    b_top_of_hour = sweep[0]  # phase 0: the boundary sitting at HH:00:00 exactly

    def ratio_of(b: float) -> float:
        return round(b / s_mean, 2) if s_mean else 0.0

    return {
        "streaming_mean_s": round(s_mean, 2),
        "streaming_p50_s": round(percentile(streaming, 50), 2),
        "streaming_p95_s": round(percentile(streaming, 95), 2),
        "batch_mean_s": round(b_mean, 2),
        "batch_mean_s_min": round(b_min, 2),
        "batch_mean_s_median": round(b_median, 2),
        "batch_mean_s_max": round(b_max, 2),
        "batch_mean_s_top_of_hour": round(b_top_of_hour, 2),
        "batch_interval_s": interval_s,
        "ratio": ratio_of(b_mean),
        "ratio_min": ratio_of(b_min),
        "ratio_median": ratio_of(b_median),
        "ratio_max": ratio_of(b_max),
        "ratio_top_of_hour": ratio_of(b_top_of_hour),
        "n": n,
    }


def distinct_updates(rows: list[tuple[float, float, str]]) -> list[tuple[float, float]]:
    """Collapse chunk rows to one (posted_at, queryable_at) per update.

    `vector_records` holds one row per CHUNK, so a multi-chunk update would
    otherwise be scored — and counted in `n` — once per chunk. Each chunk is
    joined to the same event-level first_queryable_at receipt, so keeping one
    row per event_id counts completion once."""
    seen: dict[str, tuple[float, float]] = {}
    for posted, queryable, event_id in rows:
        seen.setdefault(event_id, (posted, queryable))
    return list(seen.values())


_EMPTY_RUN_KEYS = ("streaming_mean_s", "streaming_p50_s", "streaming_p95_s",
                   "batch_mean_s", "batch_mean_s_min", "batch_mean_s_median",
                   "batch_mean_s_max", "batch_mean_s_top_of_hour",
                   "ratio", "ratio_min", "ratio_median", "ratio_max",
                   "ratio_top_of_hour", "n_rows")


def finalize_report(report: dict) -> dict:
    """Strip the numeric fields when nothing was scored.

    n = 0 is NOT a 0.0 ratio — it is the absence of a measurement. Emitting
    zeros invites quoting a result the run never produced, which is exactly how
    a pipeline outage once read as "streaming is 14x slower than batch".
    """
    if report.get("n", 0) > 0:
        return report
    report["status"] = "not yet measured"
    report["explanation"] = (
        "No eligible first-index completion receipts in the selected window. "
        "Run the poller, stream and embedder together for several hours, "
        "then re-run.")
    for key in _EMPTY_RUN_KEYS:
        report.pop(key, None)
    return report


def index_snapshot(conn, now: datetime | None = None) -> dict:
    """Describe index currentness without calling it pipeline latency.

    A provider timestamp tells us how recent the newest searchable event is. It
    does not tell us when the provider published it, so this is a corpus-health
    signal, not a replacement for the live-arrival benchmark below.
    """
    now = now or datetime.now(UTC)
    row = conn.execute(
        """
        SELECT count(DISTINCT event_id),
               count(DISTINCT event_id) FILTER (
                   WHERE ts >= %(now)s - interval '24 hours'),
               count(DISTINCT event_id) FILTER (
                   WHERE indexed_at >= %(now)s - interval '24 hours'),
               max(ts), max(indexed_at)
        FROM vector_records
        """, {"now": now}).fetchone()
    n_events, posted_24h, indexed_24h, newest_ts, newest_indexed = row
    age_s = ((now - newest_ts).total_seconds() if newest_ts is not None else None)
    return {
        "measured_at": now.isoformat(),
        "n_events": int(n_events),
        "n_events_posted_last_24h": int(posted_24h),
        "n_events_indexed_last_24h": int(indexed_24h),
        "newest_provider_timestamp": (
            newest_ts.isoformat() if newest_ts is not None else None),
        "newest_provider_age_s": round(age_s, 2) if age_s is not None else None,
        "newest_indexed_at": (
            newest_indexed.isoformat() if newest_indexed is not None else None),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure end-to-end staleness.")
    parser.add_argument("--since-minutes", type=float, default=None,
                        help="override: score updates posted within this window "
                             "instead of the automatic live-arrival filter")
    parser.add_argument("--min-n", type=int,
                        default=int(os.environ.get("FRESHNESS_MIN_N", "0")),
                        help="exit non-zero if fewer than N live arrivals were "
                             "scored; guards against reporting an empty run")
    args = parser.parse_args()

    from freshet.common.db import connect
    from freshet.common.heartbeat import continuous_run_start

    measured_at = datetime.now(UTC)
    conn = connect()
    try:
        snapshot = index_snapshot(conn, measured_at)
        if args.since_minutes is None:
            # only the current continuous run: excludes backfill and downtime catch-up
            run_start = continuous_run_start(conn)
            if run_start is None:
                rows = []
                filter_desc = "no pipeline heartbeat: nothing can be scored"
            else:
                rows = conn.execute(
                    """
                    SELECT EXTRACT(EPOCH FROM v.ts)::float8,
                           EXTRACT(EPOCH FROM i.first_queryable_at)::float8,
                           v.event_id
                    FROM vector_records v JOIN event_indexing i USING (event_id)
                    WHERE v.ts >= %(start)s AND i.first_queryable_at >= %(start)s
                    """, {"start": run_start}).fetchall()
                filter_desc = (f"current continuous run (since {run_start.isoformat()})")
        else:
            rows = conn.execute(
                """
                SELECT EXTRACT(EPOCH FROM v.ts)::float8,
                       EXTRACT(EPOCH FROM i.first_queryable_at)::float8,
                       v.event_id
                FROM vector_records v JOIN event_indexing i USING (event_id)
                WHERE v.ts >= now() - (%(mins)s * interval '1 minute')
                  AND i.first_queryable_at IS NOT NULL
                """,
                {"mins": args.since_minutes},
            ).fetchall()
            filter_desc = f"posted within {args.since_minutes} minutes"
    finally:
        conn.close()

    n_rows = len(rows)
    updates = distinct_updates(rows)
    streaming = [streaming_staleness(posted, indexed) for posted, indexed in updates]
    posted_ats = [posted for posted, _ in updates]
    report = summarize(streaming, posted_ats)
    report["n_rows"] = n_rows
    report["filter"] = (filter_desc if args.since_minutes is None
                        else f"posted within {args.since_minutes} minutes")
    report["note"] = (
        "Freshness uses first_queryable_at, recorded after acknowledged chunk writes. "
        "Replays preserve it; last_queryable_at records reindexing separately. "
        "Legacy rows without first-index receipts are excluded. The default window "
        "excludes catch-up history; index_snapshot reports corpus currentness separately. "
        "The hourly batch arm is modeled, not a deployed comparison system."
    )
    report["measurement_version"] = 2
    report["index_snapshot"] = snapshot

    report = finalize_report(report)

    os.makedirs("results", exist_ok=True)
    with open(RESULTS, "w") as fh:
        json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2))

    if report["n"] < args.min_n:
        raise SystemExit(
            f"[freshness] n={report['n']} is below --min-n={args.min_n}: "
            f"not enough live arrivals for this to be a measurement")


if __name__ == "__main__":
    main()
