"""Remove alert lifecycle rows that do not reference a real alert."""

from __future__ import annotations

id = 78
description = "alert lifecycle orphan cleanup"


def up(conn) -> None:
    conn.execute(
        """
        DELETE FROM alert_lifecycle_events ale
        WHERE NOT EXISTS (
          SELECT 1 FROM alerts a WHERE a.id = ale.alert_id
        )
        """
    )
    conn.execute(
        """
        DELETE FROM alert_acks aa
        WHERE NOT EXISTS (
          SELECT 1 FROM alerts a WHERE a.id = aa.alert_id
        )
        """
    )
    conn.execute(
        """
        DELETE FROM alert_resolutions ar
        WHERE NOT EXISTS (
          SELECT 1 FROM alerts a WHERE a.id = ar.alert_id
        )
        """
    )
    conn.execute(
        """
        DELETE FROM alert_shelves ash
        WHERE NOT EXISTS (
          SELECT 1 FROM alerts a WHERE a.id = ash.alert_id
        )
        """
    )
