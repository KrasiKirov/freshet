"""Freshness report: percentiles of event->queryable latency (indexed_at - ts)
read from vector_records. This is the project's headline metric; the full eval
harness (M6) builds on the same numbers.

Run (after the slice has indexed events):
    python -m freshet.eval.freshness
"""

from __future__ import annotations

import math


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile. `values` need not be pre-sorted."""
    if not values:
        raise ValueError("no values")
    vals = sorted(values)
    k = max(0, min(len(vals) - 1, math.ceil(p / 100 * len(vals)) - 1))
    return vals[k]


def freshness_report(latencies_s: list[float]) -> dict[str, float]:
    return {
        "count": len(latencies_s),
        "p50_s": percentile(latencies_s, 50),
        "p95_s": percentile(latencies_s, 95),
        "p99_s": percentile(latencies_s, 99),
    }


def main() -> None:
    from freshet.common.db import connect

    conn = connect()
    try:
        rows = conn.execute(
            "SELECT EXTRACT(EPOCH FROM (indexed_at - ts))::float8 FROM vector_records"
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        print("no records in vector_records — run the slice first (make slice)")
        return
    rep = freshness_report([r[0] for r in rows])
    print(
        f"event->queryable freshness over {rep['count']} records:"
        f"  p50={rep['p50_s']:.2f}s  p95={rep['p95_s']:.2f}s  p99={rep['p99_s']:.2f}s"
    )


if __name__ == "__main__":
    main()
