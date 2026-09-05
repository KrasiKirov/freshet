"""The supervisor exists so a ten-hour measurement survives a crashed child.

Freshness scores only the CURRENT continuous run: a heartbeat gap over 300s
(freshet/common/heartbeat.py GAP_TOLERANCE_S) discards everything accumulated
so far. A child that dies unsupervised does not slow the measurement down, it
silently restarts it at zero.
"""
import subprocess

import pytest

from freshet.ops.supervisor import Child, DependencyDown, supervise


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


def test_a_dependency_outage_halts_instead_of_looping_forever():
    """The first real run restarted children 412 times over three hours against a
    stopped Postgres and looked, in the log, exactly like a healthy run. A child
    that dies within a second, repeatedly, is a dependency that is gone."""
    def spawn(child):
        return FakeProc(exits_after=0)

    with pytest.raises(DependencyDown) as caught:
        supervise([Child("embedder", ["embedder"], "logs/e.log")],
                  spawn=spawn, clock=_clock(), sleep=lambda s: None,
                  should_stop=lambda: False, max_fast_deaths=3,
                  log=lambda m: None)

    assert "embedder" in str(caught.value)


def test_a_child_that_ran_a_while_does_not_count_toward_the_outage():
    """Restarts spread over hours are ordinary. Only deaths inside FAST_DEATH_S
    are evidence of a missing dependency, or a long healthy run would eventually
    trip the halt for no reason."""
    slow = iter([0.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0])

    def spawn(child):
        return FakeProc(exits_after=0)

    stops = iter([False, False, False, True])
    child = Child("poller", ["poller"], "logs/p.log")
    supervise([child], spawn=spawn, clock=lambda: next(slow),
              sleep=lambda s: None, should_stop=lambda: next(stops),
              max_fast_deaths=2, log=lambda m: None)

    assert child.restarts >= 2, "the run should have continued, not halted"


def test_the_fast_death_streak_resets_when_a_child_survives():
    """One flaky restart must not accumulate toward a halt hours later: a child
    that dies fast, then runs past fast_death_s, then dies fast again must read
    as ONE fast death, not two -- a non-reset counter would trip max_fast_deaths
    here and halt on a pattern that isn't a dependency outage."""
    # start=0.0; death#1 waited=5.0 (fast); restart at 5.0; death#2 waited=25.0
    # (survived -- resets the streak); restart at 30.0; death#3 waited=2.0 (fast).
    ticks = iter([0.0, 5.0, 5.0, 30.0, 30.0, 32.0, 32.0])

    def spawn(child):
        return FakeProc(exits_after=0)

    stops = iter([False, False, False, True])
    child = Child("poller", ["poller"], "logs/p.log")
    try:
        supervise([child], spawn=spawn, clock=lambda: next(ticks),
                  sleep=lambda s: None, should_stop=lambda: next(stops),
                  max_fast_deaths=2, fast_death_s=10.0, log=lambda m: None)
    except DependencyDown:
        pytest.fail("the streak did not reset: a restart that survived past "
                    "fast_death_s still counted toward the halt")

    assert child.fast_deaths == 1, (
        f"expected exactly one fast death after the mid-sequence reset, "
        f"got {child.fast_deaths}")


def test_the_halt_path_shuts_down_every_child_like_a_normal_exit():
    """DependencyDown must go through the same _shutdown as a clean stop. An
    earlier draft raised straight past it, orphaning every other child on the
    one path where the process is about to exit -- a human caught it in review,
    with no regression test. An orphaned poller means the next run starts with
    two producers on raw.incidents."""
    stuck = FakeStuckProc()
    healthy = FakeProc(exits_after=0)  # dies fast every restart -- trips the halt

    def spawn(child):
        return stuck if child.name == "poller" else healthy

    children = [Child("poller", ["poller"], "logs/p.log"),
                Child("embedder", ["embedder"], "logs/e.log")]

    with pytest.raises(DependencyDown):
        supervise(children, spawn=spawn, clock=_clock(), sleep=lambda s: None,
                  should_stop=lambda: False, max_fast_deaths=2,
                  log=lambda m: None)

    assert stuck.terminated, "the halt path never terminated the stuck child"
    assert stuck.killed, "the halt path never escalated to SIGKILL for the stuck child"
    assert healthy.terminated, "the halt path never terminated the other child"
