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
by roughly 2/hour and is reported alongside every figure. `n` counts distinct
UPDATES (event_id); `n_rows` is the underlying chunk-row count, reported
separately because `vector_records` holds one row per chunk and a multi-chunk
update must not be counted once per chunk.

**The batch arm has no natural alignment.** `batch_staleness` models an hourly
refresh whose boundary sits at some phase within the interval — HH:00:00 is only
one of `interval_s` possible second-offsets, and real arrivals cluster at the
top of the hour (scheduled maintenance windows start on the hour by nature), so
scoring phase 0 alone rewards or punishes whichever alignment this run's
workload happens to cluster against. The reported arm is therefore the mean over
EVERY possible phase (`batch_alignment_sweep`), with the min/median/max also
reported so the sensitivity to alignment stays visible instead of hidden behind
one number.

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
    otherwise be scored — and counted in `n` — once per chunk. Chunks belonging
    to the same event_id carry identical ts/indexed_at (verified against the
    live table: 0 of 581 multi-chunk events there disagree), so keeping any one
    row per event_id is exact, not an approximation."""
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
        "No live arrivals scored: every indexed update was posted before indexing "
        "began. Run the poller, stream and embedder together for several hours, "
        "then re-run.")
    for key in _EMPTY_RUN_KEYS:
        report.pop(key, None)
    return report


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

    conn = connect()
    try:
        if args.since_minutes is None:
            # Only the CURRENT continuous run: `ts >= min(indexed_at)` excluded
            # backfill but not downtime catch-up — after a 14h outage the late
            # burst scored 9.8h staleness, reporting streaming as 14x slower than batch.
            run_start = continuous_run_start(conn)
            if run_start is None:
                rows = []
                filter_desc = "no pipeline heartbeat: nothing can be scored"
            else:
                rows = conn.execute(
                    """
                    SELECT EXTRACT(EPOCH FROM ts)::float8,
                           EXTRACT(EPOCH FROM indexed_at)::float8,
                           event_id
                    FROM vector_records
                    WHERE ts >= %(start)s AND indexed_at >= %(start)s
                    """, {"start": run_start}).fetchall()
                filter_desc = (f"current continuous run (since {run_start.isoformat()})")
        else:
            rows = conn.execute(
                """
                SELECT EXTRACT(EPOCH FROM ts)::float8,
                       EXTRACT(EPOCH FROM indexed_at)::float8,
                       event_id
                FROM vector_records
                WHERE ts >= now() - (%(mins)s * interval '1 minute')
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
        "t0 = the provider's own posting time, so the poll wait we do not control "
        "is included. Only LIVE arrivals are scored (posted after indexing began); "
        "backfilled history would otherwise report the moment the pipeline was "
        "switched on rather than its speed. `n` counts distinct updates "
        "(event_id); `n_rows` is the underlying chunk-row count, since "
        "vector_records holds one row per chunk. The batch arm is the mean "
        "batch wait over EVERY possible refresh-boundary phase, not one "
        "alignment: a single phase (e.g. HH:00:00 exactly) can be inflated or "
        "flattered by which offset this run's arrivals happen to cluster "
        "against, and real arrivals cluster at the top of the hour because "
        "scheduled maintenance windows start on the hour. "
        "batch_mean_s_top_of_hour and ratio_top_of_hour report that single "
        "aligned figure for comparison; batch_mean_s_min/_median/_max show the "
        "spread across all alignments. Only the pipeline's CURRENT continuous "
        "run is scored, proven by heartbeat: a restart or an outage starts a "
        "new run rather than charging the catch-up burst to the pipeline's "
        "speed."
    )

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
