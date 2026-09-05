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

# A child dying this fast failed to reach a dependency, not its own work.
FAST_DEATH_S = 15.0
# Consecutive fast deaths (within FAST_DEATH_S) before halting as a dependency outage.
MAX_FAST_DEATHS = 6


class DependencyDown(RuntimeError):
    """Raised when children are dying too fast to be failing at their own work."""


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
    fast_deaths: int = 0


def _spawn(child: Child) -> subprocess.Popen:
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
              fast_death_s: float = FAST_DEATH_S,
              max_fast_deaths: int = MAX_FAST_DEATHS,
              log: Callable[[str], None] = _log) -> None:
    """Start every child, restart any that exits, and terminate all on stop.

    Raises `DependencyDown` -- after terminating and waiting out every other
    child, the same as a normal shutdown -- if one child dies `max_fast_deaths`
    times in a row within `fast_death_s` of its own start. That pattern means a
    dependency the children need (Postgres, Redpanda) is gone, not that the
    children themselves are broken; restarting into it forever would just hide
    the outage instead of surfacing it.
    """
    for child in children:
        _start(child, spawn, clock, log)

    while not should_stop():
        sleep(poll_s)
        for child in children:
            if child.proc.poll() is None:
                continue
            # waited is measured from the child's start, not its death
            waited = clock() - child.started_at
            if waited < backoff_s:
                log(f"supervisor: {child.name} died after {waited:.0f}s; "
                    f"waiting {backoff_s - waited:.0f}s before restart")
                sleep(backoff_s - waited)
            if waited < fast_death_s:
                child.fast_deaths += 1
                if child.fast_deaths >= max_fast_deaths:
                    _shutdown(children, log)
                    raise DependencyDown(
                        f"{child.name} died within {fast_death_s:.0f}s "
                        f"{child.fast_deaths} times in a row -- a dependency it "
                        f"needs is gone (check `docker ps`); refusing to keep "
                        f"restarting into it")
            else:
                child.fast_deaths = 0
            child.restarts += 1
            _start(child, spawn, clock, log)

    _shutdown(children, log)


def _shutdown(children: list[Child], log: Callable[[str], None]) -> None:
    """Terminate every child and confirm each one actually exits, escalating to
    SIGKILL if it ignores SIGTERM. Shared by the normal stop path and the
    DependencyDown halt path so a halt is exactly as orphan-safe as a clean
    shutdown -- neither leaves a child of this run alive to double-produce into
    the next one."""
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
                # confirm the kill landed, not just issued
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

    try:
        supervise(children, should_stop=lambda: stopping["now"])
    except DependencyDown as exc:
        _log(f"supervisor: HALTING -- {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
