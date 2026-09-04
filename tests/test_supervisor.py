"""The supervisor exists so a ten-hour measurement survives a crashed child.

Freshness scores only the CURRENT continuous run: a heartbeat gap over 300s
(freshet/common/heartbeat.py GAP_TOLERANCE_S) discards everything accumulated
so far. A child that dies unsupervised does not slow the measurement down, it
silently restarts it at zero.
"""
import subprocess

from freshet.ops.supervisor import Child, supervise


class FakeProc:
    """Exits after `exits_after` polls, then reports `code` forever."""

    def __init__(self, exits_after: int = 10**9, code: int = 1) -> None:
        self.polls = 0
        self.exits_after = exits_after
        self.code = code
        self.terminated = False

    def poll(self):
        self.polls += 1
        return self.code if self.polls > self.exits_after else None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return self.code


class FakeStuckProc:
    """Ignores SIGTERM: terminate() is recorded but the process stays alive, so
    wait() keeps timing out until kill() has actually been called."""

    def __init__(self) -> None:
        self.terminated = False
        self.killed = False
        self.wait_calls = 0

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        self.wait_calls += 1
        if not self.killed:
            raise subprocess.TimeoutExpired(cmd="stuck", timeout=timeout)
        return 1

    def kill(self):
        self.killed = True


def _clock():
    """Monotonic clock that advances 1s per call, so backoff arithmetic is exact."""
    ticks = iter(range(0, 10**6))
    return lambda: float(next(ticks))


def test_a_child_that_exits_is_restarted():
    spawned = []

    def spawn(child):
        proc = FakeProc(exits_after=1)
        spawned.append(child.name)
        return proc

    stops = iter([False, False, True])
    supervise([Child("poller", ["poller"], "logs/p.log")],
              spawn=spawn, clock=_clock(), sleep=lambda s: None,
              should_stop=lambda: next(stops), log=lambda m: None)

    assert spawned.count("poller") >= 2, "the dead child was never restarted"


def test_a_healthy_child_is_left_alone():
    """Restarting a working embedder would break the heartbeat run for nothing."""
    spawned = []

    def spawn(child):
        spawned.append(child.name)
        return FakeProc()

    stops = iter([False, False, False, True])
    supervise([Child("embedder", ["embedder"], "logs/e.log")],
              spawn=spawn, clock=_clock(), sleep=lambda s: None,
              should_stop=lambda: next(stops), log=lambda m: None)

    assert spawned == ["embedder"], "a live child was restarted"


def test_a_child_that_dies_instantly_is_throttled():
    """A crash loop must not become a spend loop: the autopilot calls an LLM on
    start-up work, and launchd's own guard (ThrottleInterval 60) is bypassed
    because launchd now supervises this process, not the children."""
    slept = []

    def spawn(child):
        return FakeProc(exits_after=0)

    stops = iter([False, False, True])
    supervise([Child("autopilot", ["autopilot"], "logs/a.log")],
              spawn=spawn, clock=_clock(), sleep=slept.append,
              backoff_s=30.0, should_stop=lambda: next(stops),
              log=lambda m: None)

    assert max(slept) >= 29.0, f"restart was not throttled: slept {slept}"


def test_shutdown_terminates_every_child():
    """A supervisor that leaves orphans behind means the next run starts with two
    pollers producing into the same topic."""
    procs = []

    def spawn(child):
        proc = FakeProc()
        procs.append(proc)
        return proc

    children = [Child("poller", ["poller"], "logs/p.log"),
                Child("embedder", ["embedder"], "logs/e.log")]
    supervise(children, spawn=spawn, clock=_clock(), sleep=lambda s: None,
              should_stop=lambda: True, log=lambda m: None)

    assert all(p.terminated for p in procs), "a child survived shutdown"


def test_shutdown_kills_a_child_that_ignores_sigterm():
    """A poller that ignores SIGTERM and survives the supervisor's own exit is an
    orphan: it keeps producing into the topic for the rest of the run, so the next
    run starts with two pollers producing into the same topic."""
    proc = FakeStuckProc()

    supervise([Child("poller", ["poller"], "logs/p.log")],
              spawn=lambda child: proc, clock=_clock(), sleep=lambda s: None,
              should_stop=lambda: True, log=lambda m: None)

    assert proc.killed, "a child that ignored SIGTERM was left running past shutdown"
    assert proc.wait_calls >= 2, "kill() was issued but never confirmed with a second wait()"


def test_restarts_are_counted_per_child():
    """The count goes in the run's notes: a measurement taken over a pipeline that
    restarted nine times is a different claim from one that never restarted."""
    def spawn(child):
        return FakeProc(exits_after=0)

    child = Child("poller", ["poller"], "logs/p.log")
    stops = iter([False, False, True])
    supervise([child], spawn=spawn, clock=_clock(), sleep=lambda s: None,
              should_stop=lambda: next(stops), log=lambda m: None)

    assert child.restarts >= 2
