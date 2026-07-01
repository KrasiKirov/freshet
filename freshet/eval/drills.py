"""Failure drills — resilience claims turned into demonstrated evidence.

Each drill drives the real stack, samples a metric over time, asserts the
resilience property, and writes a committed PNG. Run via `make drills` (stack up,
.[embed] .[eval]). Not a unit test — a live demonstration harness; the asserted
properties below are the pass/fail gate.

  1. worker_recovery  — kill the embedder mid-stream; lag grows. Restart it; lag
                        drains to zero with no data loss (every event indexed).
  2. replay_reindex   — after a 'model change', `make replay` re-indexes the whole
                        corpus in place (row count unchanged, indexed_at advances).
  3. burst_backpressure — a 10x burst spikes consumer lag, which then drains as the
                        embedder catches up (bounded backpressure, no loss).
"""

from __future__ import annotations

import subprocess
import sys
import time

from freshet.common.db import connect
from freshet.eval import plots

BROKERS = "localhost:9092"
RESULTS = "results"


def _lag(group: str, topic: str) -> int:
    """Total consumer-group lag for a topic via rpk (0 if the group is absent)."""
    out = subprocess.run(
        ["docker", "exec", "freshet-redpanda", "rpk", "group", "describe", group],
        capture_output=True, text=True,
    ).stdout
    total = 0
    for line in out.splitlines():
        parts = line.split()
        # rpk rows: TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(parts) >= 5 and parts[0] == topic and parts[4].lstrip("-").isdigit():
            total += max(0, int(parts[4]))
    return total


def _count(conn) -> int:
    return conn.execute("SELECT count(*) FROM vector_records").fetchone()[0]


def _produce(count: int, seed: int) -> int:
    """Produce a burst synchronously (returns once all events are enqueued)."""
    subprocess.run(
        [sys.executable, "-m", "freshet.generator", "--sink", "kafka",
         "--brokers", BROKERS, "--count", str(count), "--seed", str(seed),
         "--live", "--live-spacing", "0"],
        check=True,
    )
    return count + 9  # + scripted incident


def _produce_bg(count: int, seed: int, spacing: float):
    """Produce continuously in the BACKGROUND so events keep flowing during a
    drill — essential for worker_recovery: lag must grow while the embedder is
    dead, which only happens if there is ongoing inflow."""
    return subprocess.Popen(
        [sys.executable, "-m", "freshet.generator", "--sink", "kafka",
         "--brokers", BROKERS, "--count", str(count), "--seed", str(seed),
         "--live", "--live-spacing", str(spacing)],
    )


def _spawn(module: str, group: str, embedder: str = "stub"):
    # --metrics-port 0 on BOTH workers: drills sample lag via rpk, not Prometheus,
    # and a non-zero default would make the next drill's worker collide on the port.
    args = [sys.executable, "-m", module, "--brokers", BROKERS, "--group", group,
            "--metrics-port", "0"]
    if "embedder" in module:
        args += ["--embedder", embedder]
    return subprocess.Popen(args)


def _stop(*procs) -> None:
    """Terminate processes and WAIT for them to exit, so a drill's workers are
    fully gone (ports released, group left) before the next drill starts."""
    for p in procs:
        if p is not None:
            p.terminate()
    for p in procs:
        if p is None:
            continue
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


def _reset_topics() -> None:
    """Recreate the shared topics so each drill starts with empty topics —
    otherwise a replay reads the previous drill's events too."""
    names = ["raw.events", "normalized.events", "deadletter.events"]
    subprocess.run(["docker", "exec", "freshet-redpanda", "rpk", "topic", "delete", *names],
                   capture_output=True)
    subprocess.run(["docker", "exec", "freshet-redpanda", "rpk", "topic", "create", *names, "-p", "3"],
                   capture_output=True)


def worker_recovery(count: int = 400) -> None:
    conn = connect()
    conn.execute("DELETE FROM vector_records")
    _reset_topics()
    norm = _spawn("freshet.pipeline.normalizer", "drill-norm")
    emb = _spawn("freshet.pipeline.embedder", "drill-emb")
    # continuous inflow over the drill window so lag grows while the embedder is
    # dead (spacing chosen so production spans ~40s, past the restart point)
    total = count + 9
    producer = _produce_bg(count, seed=int(time.time()), spacing=0.1)
    try:
        times, lags, markers = [], [], []
        t0 = time.monotonic()
        killed = restarted = False
        while True:
            t = time.monotonic() - t0
            lag = _lag("drill-emb", "normalized.events")
            times.append(t)
            lags.append(lag)
            if t > 8 and not killed:
                emb.terminate(); emb.wait(); killed = True
                markers.append((t, "embedder killed"))
            elif t > 25 and not restarted:
                emb = _spawn("freshet.pipeline.embedder", "drill-emb")
                restarted = True
                markers.append((t, "embedder restarted"))
            elif restarted and lag == 0 and _count(conn) >= total:
                break
            if t > 120:
                break
            time.sleep(1.0)
        indexed = _count(conn)
        plots.plot_timeseries(
            times, [float(x) for x in lags],
            f"{RESULTS}/drill_worker_recovery.png",
            "Worker recovery: consumer lag on kill / restart",
            "embedder lag (messages)", markers,
        )
        assert indexed == total, f"DATA LOSS: produced {total}, indexed {indexed}"
        print(f"worker_recovery OK: {total} produced, {indexed} indexed, no loss")
    finally:
        _stop(producer, norm, emb)
        conn.close()


def replay_reindex(count: int = 100) -> None:
    conn = connect()
    conn.execute("DELETE FROM vector_records")
    _reset_topics()
    norm = _spawn("freshet.pipeline.normalizer", "drill-norm2")
    emb = _spawn("freshet.pipeline.embedder", "drill-emb2", embedder="stub")
    try:
        total = _produce(count, seed=7)
        deadline = time.monotonic() + 60
        while _count(conn) < total and time.monotonic() < deadline:
            time.sleep(1.0)
        before = conn.execute(
            "SELECT count(*), max(indexed_at) FROM vector_records"
        ).fetchone()
        emb.terminate(); emb.wait()
        # 'model change' -> replay the whole corpus under a fresh group
        subprocess.run(
            [sys.executable, "-m", "freshet.pipeline.embedder", "--brokers", BROKERS,
             "--group", f"reindex-{int(time.time())}", "--embedder", "stub",
             "--metrics-port", "0", "--idle-timeout", "10"],
            check=True,
        )
        after = conn.execute(
            "SELECT count(*), max(indexed_at) FROM vector_records"
        ).fetchone()
        assert after[0] == before[0], f"row count changed on replay: {before[0]} -> {after[0]}"
        assert after[1] > before[1], "indexed_at did not advance on replay"
        print(f"replay_reindex OK: {after[0]} rows re-indexed in place, indexed_at advanced")
    finally:
        _stop(norm)
        conn.close()


def burst_backpressure(count: int = 2000) -> None:
    conn = connect()
    conn.execute("DELETE FROM vector_records")
    _reset_topics()
    norm = _spawn("freshet.pipeline.normalizer", "drill-norm3")
    emb = _spawn("freshet.pipeline.embedder", "drill-emb3", embedder="stub")
    try:
        # produce the whole burst as fast as possible (spacing 0)
        subprocess.run(
            [sys.executable, "-m", "freshet.generator", "--sink", "kafka",
             "--brokers", BROKERS, "--count", str(count), "--seed", "3",
             "--live", "--live-spacing", "0"],
            check=True,
        )
        total = count + 9
        times, lags = [], []
        t0 = time.monotonic()
        peak = 0
        while True:
            t = time.monotonic() - t0
            lag = _lag("drill-emb3", "normalized.events")
            times.append(t); lags.append(lag); peak = max(peak, lag)
            if lag == 0 and _count(conn) >= total:
                break
            if t > 180:
                break
            time.sleep(1.0)
        plots.plot_timeseries(
            times, [float(x) for x in lags],
            f"{RESULTS}/drill_burst_backpressure.png",
            f"Backpressure: lag under a {count}-event burst, then drain",
            "embedder lag (messages)", [(0.0, f"{total} events produced")],
        )
        assert _count(conn) == total, "data loss under burst"
        print(f"burst_backpressure OK: peak lag {peak}, drained to 0, {total} indexed")
    finally:
        _stop(norm, emb)
        conn.close()


def main() -> None:
    import os
    os.makedirs(RESULTS, exist_ok=True)
    worker_recovery()
    replay_reindex()
    burst_backpressure()


if __name__ == "__main__":
    main()
