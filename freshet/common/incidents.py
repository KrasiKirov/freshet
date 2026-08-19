"""Find-or-create the incidents row Autopilot claims against.

The embedder writes this as it indexes; Autopilot also writes it on the claim
path so a lifecycle event that beats the embedder can still brief.
"""

from __future__ import annotations

from datetime import datetime

ENSURE_INCIDENT_SQL = (
    "INSERT INTO incidents (incident_id, title, opened_at, primary_service, auto_opened)"
    " VALUES (%s, %s, %s, %s, false)"
    " ON CONFLICT (incident_id) DO UPDATE"
    " SET title = CASE WHEN incidents.title = '' THEN EXCLUDED.title ELSE incidents.title END,"
    "     primary_service = coalesce(incidents.primary_service, EXCLUDED.primary_service)"
)

ENSURE_SERVICE_SQL = (
    "INSERT INTO incident_services (incident_id, service) VALUES (%s, %s)"
    " ON CONFLICT DO NOTHING"
)


def ensure_incident(conn, incident_id: str | None, service: str,
                    opened_at: datetime, title: str = "") -> None:
    if not incident_id:
        return
    conn.execute(ENSURE_INCIDENT_SQL, (incident_id, title or "", opened_at, service))
    conn.execute(ENSURE_SERVICE_SQL, (incident_id, service))
