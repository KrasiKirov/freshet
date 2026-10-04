from __future__ import annotations

import subprocess

import pytest

from freshet.ops.index_ready import read_lag, wait_ready


def _result(stdout: str = "", returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_read_lag_parses_rpk_total_lag():
    result = read_lag("embedder", run=lambda *args, **kwargs: _result(
        "GROUP embedder\nTOTAL-LAG        17\n"))
    assert result == 17


def test_read_lag_reports_missing_group():
    with pytest.raises(RuntimeError, match="embedder"):
        read_lag("embedder", run=lambda *args, **kwargs: _result(
            returncode=1, stderr="group not found"))


def test_wait_ready_polls_until_both_groups_are_empty():
    values = {"freshet-stream": [4, 0, 0], "embedder": [9, 2, 0]}
    waits = []
    now = [0.0]

    def read(group):
        return values[group].pop(0)

    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds

    assert wait_ready(timeout_s=20, interval_s=1, read=read,
                      clock=lambda: now[0], sleep=sleep,
                      emit=lambda _: None) == {"freshet-stream": 0, "embedder": 0}
    assert waits == [1, 1]


def test_wait_ready_times_out_with_lag_details():
    with pytest.raises(TimeoutError, match="embedder=3"):
        wait_ready(timeout_s=0, read=lambda group: 3,
                   clock=lambda: 0, sleep=lambda _: None,
                   emit=lambda _: None)
