"""What the running Flink job read versus what it emitted, per operator.

`json.ignore-parse-errors = 'true'` stays on in dedup_job.sql — without it a single
malformed record kills the job, which is worse than dropping it. But the rows it
swallows never become rows, so they cannot be dead-lettered and cannot be counted
downstream: the SQL's own comment admits they vanish "without a row, without a
dead-letter and without a metric".

The third of those is fixable without touching the job's failure semantics, because
Flink already counts them. A source vertex whose read-records climbs while the
operators behind it stay flat is a producer that has drifted from the schema. That
is otherwise completely invisible.

    python -m freshet.stream.health

Reads Flink's REST API on localhost:8081. Prints nothing alarming on its own — the
numbers are only meaningful against a baseline, so record one in RESULTS.md.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "http://localhost:8081"


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def running_job_ids(base: str) -> list[str]:
    return [j["id"] for j in _get(f"{base}/jobs")["jobs"] if j["status"] == "RUNNING"]


def vertex_rows(base: str, job_id: str) -> list[tuple[str, int, int]]:
    """(operator name, records in, records out) for every vertex of one job."""
    out = []
    for vertex in _get(f"{base}/jobs/{job_id}")["vertices"]:
        metrics = vertex.get("metrics") or {}
        out.append((vertex["name"],
                    int(metrics.get("read-records", 0)),
                    int(metrics.get("write-records", 0))))
    return out


def render(base: str) -> str:
    jobs = running_job_ids(base)
    if not jobs:
        return "no RUNNING job on this cluster"
    lines = []
    for job_id in jobs:
        lines.append(f"job {job_id}")
        for name, read, wrote in vertex_rows(base, job_id):
            # A source reports 0 read-records (it reads from Kafka, not from an
            # upstream operator); its write-records is what entered the pipeline.
            lines.append(f"  {name[:70]:70} in={read:>10} out={wrote:>10}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL, help="Flink REST endpoint")
    args = parser.parse_args()
    try:
        print(render(args.url))
    except (urllib.error.URLError, OSError) as exc:
        print(f"[stream-health] no Flink REST API at {args.url} ({exc}); "
              f"is the job running? `make stream`", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
