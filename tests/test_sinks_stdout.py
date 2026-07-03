from freshet.autopilot.brief import Findings
from freshet.autopilot.sinks.stdout import StdoutSink


def _f():
    return Findings(service="api", status="open", cause_text="bad deploy",
                    cause_cite="[ev1 @ 2026-07-01 00:00:00]", fix_text=None, fix_cite=None,
                    runbook="restart the worker", narrative=None)


def test_stdout_sink_prints_cited_brief(capsys):
    StdoutSink().deliver(_f())
    out = capsys.readouterr().out
    assert "INCIDENT BRIEF" in out
    assert "bad deploy" in out and "[ev1 @ 2026-07-01 00:00:00]" in out
