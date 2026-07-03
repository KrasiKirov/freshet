from freshet.autopilot.brief import Findings, IMPACT_STUB
from freshet.autopilot.sinks.slack import slack_blocks


def _f(**kw):
    base = dict(service="api", status="open", cause_text="bad deploy",
                cause_cite="[ev1 @ 2026-07-01 00:00:00]", fix_text=None, fix_cite=None,
                runbook="restart the worker", narrative=None)
    base.update(kw)
    return Findings(**base)


def test_blocks_are_header_section_context():
    b = slack_blocks(_f())
    assert [blk["type"] for blk in b] == ["header", "section", "context"]
    assert "api" in b[0]["text"]["text"]


def test_section_cites_cause():
    txt = slack_blocks(_f())[1]["text"]["text"]
    assert "bad deploy" in txt and "[ev1 @ 2026-07-01 00:00:00]" in txt


def test_context_has_runbook_and_impact_stub():
    ctx = slack_blocks(_f())[2]["elements"][0]["text"]
    assert "restart the worker" in ctx and IMPACT_STUB in ctx


def test_narrative_preferred_over_cause_lines():
    txt = slack_blocks(_f(narrative="Cause: X [evX @ 2026-07-01 00:00:00].",
                          cause_text=None, cause_cite=None))[1]["text"]["text"]
    assert "Cause: X [evX @ 2026-07-01 00:00:00]." in txt
    assert "not identified" not in txt


def test_missing_cause_uses_fallback():
    txt = slack_blocks(_f(cause_text=None, cause_cite=None))[1]["text"]["text"]
    assert "not identified from retrieved evidence" in txt
