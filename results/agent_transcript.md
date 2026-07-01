# Agent investigation: scheduler-api-00

**Incident:** INC-0001 | **Archetype:** deploy_regression

## Step 0: Model reasoning
I'll investigate the scheduler-api-00 incident systematically. Let me start by searching for error spikes and anomalies related to this service.

## Step 0: `search`
```json
{
  "query": "scheduler-api-00 error spike anomaly incident",
  "service": "scheduler-api-00"
}
```
**Result preview:** `[{"event_id": "evt_3773afbd67f9", "ts": "2026-06-06T08:02:30+00:00", "type": "message", "text": "alice: errors on scheduler-api-00 just spiked \u2014 investigating"}, {"event_id": "evt_38c08712b8bc", `

## Step 0: `get_runbook`
```json
{
  "service": "scheduler-api-00"
}
```
**Result preview:** `{"service": "scheduler-api", "runbook": "scheduler-api runbook: on elevated 5xx or latency, check the most recent deploy first and roll back the latest version if it correlates; escalate to on-call if`

## Step 1: Model reasoning
Good, I have a clear picture. Let me now get events around the spike time to confirm the sequence of changes just before the incident.

## Step 1: `get_events_around`
```json
{
  "service": "scheduler-api-00",
  "timestamp": "2026-06-06T08:02:00+00:00",
  "window_s": 300
}
```
**Result preview:** `[{"event_id": "evt_f06c025b413f", "ts": "2026-06-06T08:00:30+00:00", "type": "deploy_started", "text": "Deploy v2.15.0 of scheduler-api-00 started by ci-bot"}, {"event_id": "evt_6196e1988ad9", "ts": "`

## Step 2: Model reasoning
The full timeline is now confirmed. Let me submit the findings.

## Step 2: submit\_findings
- **cause\_id:** `evt_f06c025b413f`
- **fix\_id:** `evt_076fb9d179e0`
- **narrative:** The scheduler-api-00 incident was caused by the deployment of v2.15.0 (triggered by ci-bot at 08:00:30 UTC). Within ~90 seconds of the deploy, the 5xx error rate spiked from below threshold to 11% (08:02:00 UTC). On-call engineers Alice and Bob quickly correlated the spike with the new deployment. Following the runbook guidance to roll back the latest version when it correlates with elevated 5xx errors, a rollback to the previous version was initiated at 08:04:30 UTC. By 08:06:00 UTC — roughly 90 seconds after the rollback — the error rate returned to below the 5% threshold and the service was healthy again. The postmortem noted an action item to add a guard to prevent the faulty code from being deployed again.

---
**Steps used:** 3
**Cause hit:** True (expected `evt_f06c025b413f`, got `evt_f06c025b413f`)
**Fix hit:** True (expected `evt_076fb9d179e0`, got `evt_076fb9d179e0`)