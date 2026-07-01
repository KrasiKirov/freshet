from datetime import datetime, timezone

from freshet.common.schemas import EventSource
from freshet.generator.scenarios import build_runbooks


def test_runbook_source_exists():
    assert EventSource.RUNBOOK.value == "runbook"


def test_build_runbooks_one_per_service():
    start = datetime(2026, 6, 6, 8, 0, tzinfo=timezone.utc)
    svcs = ["scheduler-api", "task-queue"]
    docs = build_runbooks(start, svcs)
    assert [d.service for d in docs] == svcs
    assert all(d.source is EventSource.RUNBOOK and d.type == "runbook" for d in docs)
    assert all(d.ts == start and d.text for d in docs)


from freshet.generator.scenarios import build_scenario
from freshet.common.schemas import EventType


def test_build_scenario_parameterized_service():
    from datetime import datetime, timezone
    start = datetime(2026, 6, 6, 8, 0, tzinfo=timezone.utc)
    evs = build_scenario(start, "INC-0007", service="billing-api")
    assert all(e.service == "billing-api" for e in evs)
    assert all(e.incident_id == "INC-0007" for e in evs)
    types = {e.type for e in evs}
    assert EventType.DEPLOY_STARTED in types and EventType.ROLLBACK in types
    assert build_scenario(start, "INC-0001")[0].service == "scheduler-api"
