"""
Durable fallback-freshness tracking.

Nothing in the registry previously recorded when a fallback_driver_id chain
was last confirmed to actually work live -- the only way to know was to
either trust it blindly or manually --driver-id test it, and there was no
record of which of the 71 fallback targets had ever been checked at all.

This module appends one row per fallback target ACTUALLY EXERCISED in a
run (i.e. it shows up as some primary's served_by_driver_id) to
output/fallback_audit_log.csv, upserted by driver_id so the log always
reflects each target's most recent confirmed status rather than growing
unbounded across every run. A target never reached this run (because its
primary already succeeded directly) is left alone -- this is a record of
what was actually exercised, not a synthetic check of everything.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .atomic_io import write_atomic
from .models import utc_now

log = logging.getLogger(__name__)

_COLUMNS = ["driver_id", "primary_driver_id", "outcome", "monthly_observations", "checked_at", "notes"]


def record_fallback_audit(manifest_entries: list[dict[str, Any]], fallback_target_ids: set[str],
                          log_path: Path) -> int:
    """
    Scans this run's manifest for entries actually served by a registered
    fallback (served_by_driver_id != driver_id, and that server is a known
    fallback target), and upserts one row per such target into log_path.

    Returns the number of fallback targets recorded this run.
    """
    if not manifest_entries or not fallback_target_ids:
        return 0

    checked_at = utc_now().isoformat(timespec="seconds")
    new_rows = []
    for entry in manifest_entries:
        served_by = entry.get("served_by_driver_id")
        if not served_by or served_by == entry.get("driver_id"):
            continue
        if served_by not in fallback_target_ids:
            continue
        new_rows.append({
            "driver_id": served_by,
            "primary_driver_id": entry["driver_id"],
            "outcome": entry.get("outcome"),
            "monthly_observations": entry.get("monthly_observations", 0),
            "checked_at": checked_at,
            "notes": entry.get("message", ""),
        })

    if not new_rows:
        return 0

    new_df = pd.DataFrame(new_rows, columns=_COLUMNS)
    if log_path.exists():
        try:
            existing = pd.read_csv(log_path, dtype=str, keep_default_na=False)
        except (OSError, ValueError):
            existing = pd.DataFrame(columns=_COLUMNS)
        existing = existing[~existing["driver_id"].isin(new_df["driver_id"])]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values("driver_id").reset_index(drop=True)
    write_atomic(log_path, lambda tmp: combined.to_csv(tmp, index=False))
    log.info("Fallback audit log: recorded %d target(s) exercised this run -> %s", len(new_rows), log_path)
    return len(new_rows)
