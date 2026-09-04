"""Keep every pipeline process alive for the length of a measurement run.

The launchd agent used to supervise `freshet.autopilot` alone, but the autopilot
is not what freshness measures. The poller and the embedder are, and neither had
a supervisor: a crashed embedder ends the CURRENT CONTINUOUS RUN (a heartbeat gap
over 300s, see freshet/common/heartbeat.py) and silently discards every live
arrival scored so far.

Restarts are throttled at RESTART_BACKOFF_S. That is deliberately shorter than
the 300s heartbeat tolerance, so an ordinary crash-and-restart does not break the
run, and long enough that a child failing on start-up cannot spin -- the autopilot
does LLM work, and a crash loop is a spend loop.

    python -m freshet.ops.supervisor
"""
from __future__ import annotations

import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

RESTART_BACKOFF_S = 30.0
POLL_INTERVAL_S = 1.0


@dataclass
class Child:
    """One supervised process. `argv` is executed as-is; `log_path` receives both
    streams, appended so a restart does not truncate the evidence."""

    name: str
    argv: list[str]
    log_path: str
    proc: Any = None
    log_handle: Any = None
    started_at: float = 0.0
    restarts: int = 0


def _spawn(child: Child) -> subprocess.Popen:
    # Deliberately not a context manager (ruff SIM115): the handle has to outlive
    # this function for as long as the child writes to it. The previous handle is
    # closed here instead, so a night of restarts does not leak descriptors.
    handle = open(child.log_path, "a", buffering=1)  # noqa: SIM115
    if child.log_handle is not None:
        child.log_handle.close()
    child.log_handle = handle
    return subprocess.Popen(child.argv, stdout=handle, stderr=subprocess.STDOUT)


def _log(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%S%z')} {message}", flush=True)


def supervise(children: list[Child], *,
              spawn: Callable[[Child], Any] = _spawn,
              clock: Callable[[], float] = time.monotonic,
              sleep: Callable[[float], None] = time.sleep,
              backoff_s: float = RESTART_BACKOFF_S,
              should_stop: Callable[[], bool] = lambda: False,
              poll_s: float = POLL_INTERVAL_S,
              log: Callable[[str], None] = _log) -> None:
    """Start every child, restart any that exits, and terminate all on stop."""
    for child in children:
        _start(child, spawn, clock, log)

    while not should_stop():
        sleep(poll_s)
        for child in children:
            if child.proc.poll() is None:
                continue
            # Throttle from the START of the dead run, not from its death: a child
            # that ran for hours restarts immediately, one that died on start-up
            # waits out the backoff.
            waited = clock() - child.started_at
            if waited < backoff_s:
                log(f"supervisor: {child.name} died after {waited:.0f}s; "
                    f"waiting {backoff_s - waited:.0f}s before restart")
                sleep(backoff_s - waited)
            child.restarts += 1
            _start(child, spawn, clock, log)

    for child in children:
        log(f"supervisor: stopping {child.name} (restarts={child.restarts})")
        child.proc.terminate()
    for child in children:
        try:
            child.proc.wait(timeout=20)
        except Exception:      # a stuck child must not block the rest
            log(f"supervisor: {child.name} did not exit within 20s; sending SIGKILL")
            child.proc.kill()
            try:
                # Confirm the kill landed rather than merely issuing it: an orphan
                # here means the next run starts with two of this child producing
                # into the same topic.
                child.proc.wait(timeout=5)
            except Exception:
                log(f"supervisor: {child.name} did not exit after SIGKILL")


def _start(child: Child, spawn: Callable[[Child], Any],
           clock: Callable[[], float], log: Callable[[str], None]) -> None:
    child.proc = spawn(child)
    child.started_at = clock()
    log(f"supervisor: started {child.name} (restarts={child.restarts})")


def main() -> None:
    """Supervise the three processes a measurement run depends on.

    The Flink job and the containers are NOT supervised here: both outlive a
    process restart, and re-submitting a stream job that is already running
    doubles the dedup state and therefore the embedder's workload (see the
    cancel loop in `make stream`).
    """
    python = sys.executable
    children = [
        Child("poller", [python, "-m", "freshet.ingest.poller"], "logs/poller.log"),
        Child("embedder", [python, "-m", "freshet.pipeline.embedder"], "logs/embedder.log"),
        Child("autopilot", [python, "-m", "freshet.autopilot",
                            "--brokers", "localhost:9092", "--sink", "slack"],
              "logs/autopilot.log"),
    ]

    stopping = {"now": False}

    def _stop(_signum, _frame):
        stopping["now"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    supervise(children, should_stop=lambda: stopping["now"])


if __name__ == "__main__":
    main()
