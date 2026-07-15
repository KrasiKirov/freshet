"""The LLM narrative is standard Markdown; Slack section blocks use mrkdwn (single-
asterisk bold, no ATX headings). _to_mrkdwn must convert it or Slack renders `**` and
`##` literally."""
from freshet.autopilot.sinks.slack import _to_mrkdwn


def test_double_asterisk_bold_becomes_single():
    assert _to_mrkdwn("**Root Cause:** the deploy") == "*Root Cause:* the deploy"


def test_multiple_bolds_on_one_line():
    assert _to_mrkdwn("**a** then **b**") == "*a* then *b*"


def test_atx_heading_becomes_bold():
    assert _to_mrkdwn("## Root cause — scheduler-api") == "*Root cause — scheduler-api*"


def test_single_asterisk_bold_is_left_alone():
    # the keyless composer already emits Slack-correct single-asterisk bold
    assert _to_mrkdwn("*Cause:* the deploy") == "*Cause:* the deploy"


def test_citation_brackets_are_not_touched():
    assert _to_mrkdwn("rolled back [evt_322669861e85 @ 2026-07-15 14:22]") == \
        "rolled back [evt_322669861e85 @ 2026-07-15 14:22]"


def test_no_double_asterisks_survive_a_realistic_brief():
    brief = "**Root Cause:** v2.15.0 deploy.\n\n## Timeline\n1. spike\n**Fix:** rollback"
    assert "**" not in _to_mrkdwn(brief)
