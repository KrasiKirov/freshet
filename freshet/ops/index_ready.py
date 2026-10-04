"""Wait until the Kafka-to-index path has drained its replay backlog.

The first poll of a cold cache can emit the full history exposed by a provider's
feed. That is useful for building the corpus, but it is not the same thing as a
current index. This check makes the distinction operational: the Flink stream
and the pgvector embedder must both report zero consumer lag before an evaluation
is allowed to run.

    python -m freshet.ops.index_ready
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import time
from collections.abc import Callable

DEFAULT_CONTAINER = "freshet-redpanda"
DEFAULT_TIMEOUT_S = 300.0
DEFAULT_INTERVAL_S = 5.0
GROUPS = ("freshet-stream", "embedder")
_TOTAL_LAG = re.compile(r"^TOTAL-LAG\s+(\d+)\s*$", re.MULTILINE)


def read_lag(group: str, *, container: str = DEFAULT_CONTAINER,
             run: Callable = subprocess.run) -> int:
    """Return a Redpanda consumer group's total lag, or raise with context."""
    result = run(
        ["docker", "exec", container, "rpk", "group", "describe", group],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "command failed").strip()
        raise RuntimeError(f"cannot inspect consumer group {group!r}: {detail}")
    match = _TOTAL_LAG.search(result.stdout)
    if match is None:
        raise RuntimeError(f"consumer group {group!r} returned no TOTAL-LAG")
    return int(match.group(1))


def wait_ready(*, timeout_s: float = DEFAULT_TIMEOUT_S,
               interval_s: float = DEFAULT_INTERVAL_S,
               container: str = DEFAULT_CONTAINER,
               groups: tuple[str, ...] = GROUPS,
               clock: Callable[[], float] = time.monotonic,
               sleep: Callable[[float], None] = time.sleep,
               read: Callable[[str], int] | None = None,
               emit: Callable[[str], None] = print) -> dict[str, int]:
    """Wait until every group has zero lag and return the final lag snapshot."""
    reader = read or (lambda group: read_lag(group, container=container))
    deadline = clock() + timeout_s
    last: dict[str, int] = {}
    while True:
        errors: list[str] = []
        last = {}
        for group in groups:
            try:
                last[group] = reader(group)
            except RuntimeError as exc:
                errors.append(str(exc))
        if not errors and all(lag == 0 for lag in last.values()):
            emit("[index-ready] stream and embedder lag are both 0")
            return last
        if clock() >= deadline:
            detail = "; ".join(errors) if errors else ", ".join(
                f"{group}={lag}" for group, lag in last.items())
            raise TimeoutError(
                f"index did not catch up within {timeout_s:.0f}s ({detail}); "
                "leave the poller, stream, and embedder running and retry")
        detail = "; ".join(errors) if errors else ", ".join(
            f"{group}={lag}" for group, lag in last.items())
        emit(f"[index-ready] waiting for catch-up ({detail})")
        sleep(min(interval_s, max(0.0, deadline - clock())))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float,
                        default=float(os.environ.get("FRESHET_READY_TIMEOUT_S", DEFAULT_TIMEOUT_S)))
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_S)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    args = parser.parse_args()
    wait_ready(timeout_s=args.timeout, interval_s=args.interval,
               container=args.container)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
