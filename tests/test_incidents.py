from freshet.common.schemas import Event, EventSource, Severity
from freshet.pipeline.incidents import incident_title, is_severe


def _ev(**kw) -> Event:
    base = dict(service="scheduler-api", source=EventSource.ALERT, type="error_spike", text="x")
    base.update(kw)
    return Event(**base)


def test_is_severe_by_type():
    assert is_severe(_ev(type="error_spike"))
    assert is_severe(_ev(type="latency_spike", source=EventSource.METRIC))
    assert is_severe(_ev(type="rollback", source=EventSource.DEPLOY))


def test_is_severe_by_severity():
    assert is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV1))
    assert is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV2))


def test_noise_is_not_severe():
    assert not is_severe(_ev(type="metric_sample", source=EventSource.METRIC))
    assert not is_severe(_ev(type="healthy", source=EventSource.ALERT))
    assert not is_severe(_ev(type="message", source=EventSource.CHAT, severity=Severity.SEV4))


def test_incident_title():
    assert incident_title(_ev(type="error_spike")) == "scheduler-api: error_spike"
