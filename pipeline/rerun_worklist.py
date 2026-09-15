"""
Rerun worklist: which failed drivers are actually worth another automated
attempt right now.

Built from an existing run_manifest.json plus the live driver_registry.csv
and input/manual/ contents -- a convenience/tracking layer over data that
already exists, not a new source of truth (mirrors success_flags.py's
relationship to the manifest).

Scoped to this session's explicit focus: drivers that have a real
automated source but could not be fetched. Three categories are
deliberately excluded, each for a different reason:
  - already successful (outcome in success/skipped) -- nothing to rerun
  - already manually resolved -- a file sits in input/manual/<driver_id>.*
    right now, regardless of whether the last run's manifest knew about it
    yet (a file dropped after that run still counts)
  - manual-upload category (access_mode in paid_or_restricted/login, or
    connector == manual_yaml) -- these were never going to be fetched by
    an automated source in the first place; re-attempting them wastes a
    cycle on something a human, not a bug fix, has to resolve
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .atomic_io import write_atomic

log = logging.getLogger(__name__)

_MANUAL_UPLOAD_ACCESS_MODES = {"paid_or_restricted", "login"}
_MANUAL_UPLOAD_CONNECTORS = {"manual_yaml"}


def _manually_resolved_ids(manual_dir: Path) -> set[str]:
    if not manual_dir.exists():
        return set()
    return {p.stem for p in manual_dir.iterdir() if p.is_file()}


def build_worklist_df(manifest_entries: list[dict[str, Any]], registry_df: pd.DataFrame,
                      manual_dir: Path) -> pd.DataFrame:
    if not manifest_entries:
        return pd.DataFrame(columns=["driver_id", "commodity", "region", "connector",
                                     "source_url", "likely_bot_blocked", "message"])

    manifest_df = pd.DataFrame(manifest_entries)
    failed = manifest_df[~manifest_df["outcome"].isin(["success", "skipped"])].copy()
    if "manual_drop" in failed.columns:
        failed = failed[~failed["manual_drop"].astype(bool)]

    already_manual = _manually_resolved_ids(manual_dir)
    if already_manual:
        failed = failed[~failed["driver_id"].isin(already_manual)]

    reg = registry_df.set_index("driver_id") if "driver_id" in registry_df.columns else registry_df
    access_mode = failed["driver_id"].map(reg.get("access_mode", pd.Series(dtype=str)))
    connector = failed["driver_id"].map(reg.get("connector", pd.Series(dtype=str)))
    is_manual_upload = (
        access_mode.fillna("").isin(_MANUAL_UPLOAD_ACCESS_MODES)
        | connector.fillna("").isin(_MANUAL_UPLOAD_CONNECTORS)
    )
    failed = failed[~is_manual_upload.values]

    cols = [c for c in ("driver_id", "commodity", "region", "connector", "source_url",
                        "likely_bot_blocked", "message") if c in failed.columns]
    out = failed[cols].sort_values(["commodity", "region", "driver_id"]).reset_index(drop=True)
    log.info("Rerun worklist: %d driver(s) have a real automated source but could not be "
            "fetched (excluded %d manual-upload-category and %d already-manually-resolved)",
            len(out), int(is_manual_upload.sum()), len(already_manual))
    return out


def write_worklist_excel(df: pd.DataFrame, path: Path) -> Path | None:
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        log.warning("openpyxl not installed; skipping rerun worklist (pip install openpyxl)")
        return None

    def _write(tmp_path: Path) -> None:
        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Worklist", index=False)

    write_atomic(path, _write)
    log.info("Wrote rerun worklist (%d drivers) -> %s", len(df), path)
    return path
