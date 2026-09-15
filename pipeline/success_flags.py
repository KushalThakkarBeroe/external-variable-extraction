"""
Success-flag file: which drivers are already good enough to skip re-fetching.

After a run, a driver's manifest entry says whether it succeeded and how
dense its monthly coverage is. This module turns that into a small Excel
file — one row per driver, with a `meets_threshold` column — that a future
run can read (via --skip-flagged) to avoid re-fetching drivers already known
to clear the bar. The file is regenerated fresh from the manifest at the end
of every run, so a driver reused via --skip-flagged (outcome SKIPPED, real
coverage_ratio carried forward from when it was actually fetched) keeps
propagating as "still good" indefinitely, with no separate merge step.

The threshold check itself is deliberately simple and hand-editable: open
the Excel file, flip a row's meets_threshold to FALSE, and that driver gets
fetched live again next run — no code change needed.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .atomic_io import write_atomic
from .models import DriverSpec, Outcome, Tier, utc_now

log = logging.getLogger(__name__)

_ELIGIBLE_OUTCOMES = (Outcome.SUCCESS, Outcome.SKIPPED)


# --------------------------------------------------------------- building

def _approximate_coverage_ratio(entry: dict[str, Any]) -> float:
    """
    Fallback for manifest entries written before coverage_ratio was
    persisted (see CascadeOrchestrator._quality_gate). Approximates the
    density check from fields that were already in the manifest:
    monthly_observations divided by the span between first_month and
    last_month. Exact once every entry carries a real coverage_ratio; only
    used for older runs.
    """
    monthly_obs = int(entry.get("monthly_observations", 0) or 0)
    first_month = entry.get("first_month")
    last_month = entry.get("last_month")
    if monthly_obs <= 0 or not first_month or not last_month:
        return 0.0
    try:
        first = pd.Period(first_month, freq="M")
        last = pd.Period(last_month, freq="M")
    except (ValueError, TypeError):
        return 0.0
    span_months = (last - first).n + 1
    if span_months <= 0:
        return 0.0
    return min(1.0, monthly_obs / span_months)


def build_flags_df(manifest_entries: list[dict[str, Any]], threshold: float = 0.4) -> pd.DataFrame:
    """
    One row per driver manifest entry. `meets_threshold` is what
    apply_skip_flagged() reads to decide whether to skip a driver next run —
    it requires a real outcome (success or a previously-reused skip, never a
    failure), at least one populated month, and coverage_ratio >= threshold.
    """
    rows = []
    for entry in manifest_entries:
        outcome = entry.get("outcome", "")
        monthly_obs = int(entry.get("monthly_observations", 0) or 0)

        coverage_ratio = entry.get("coverage_ratio")
        coverage_ratio = float(coverage_ratio) if coverage_ratio is not None else _approximate_coverage_ratio(entry)

        meets_threshold = bool(
            outcome in _ELIGIBLE_OUTCOMES and monthly_obs > 0 and coverage_ratio >= threshold
        )

        rows.append({
            "driver_id": entry.get("driver_id"),
            "commodity": entry.get("commodity"),
            "region": entry.get("region"),
            "driver": entry.get("driver"),
            "connector": entry.get("connector"),
            "source": entry.get("source"),
            "source_url": entry.get("source_url"),
            "outcome": outcome,
            "coverage_ratio": round(coverage_ratio, 4),
            "meets_threshold": meets_threshold,
            "history_years": entry.get("history_years"),
            "monthly_observations": monthly_obs,
            "first_month": entry.get("first_month"),
            "last_month": entry.get("last_month"),
            "retrieved_at": entry.get("retrieved_at"),
            "tier_used": entry.get("tier_used"),
            "quality_notes": "; ".join(entry.get("quality_notes") or []),
        })
    return pd.DataFrame(rows)


def write_flags_excel(df: pd.DataFrame, path: Path, threshold: float) -> Path | None:
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        log.warning("openpyxl not installed; skipping success-flag file (pip install openpyxl)")
        return None

    ordered = df.sort_values(["meets_threshold", "commodity", "driver_id"], ascending=[False, True, True])

    def _write(tmp_path: Path) -> None:
        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            ordered.to_excel(writer, sheet_name="Flags", index=False)

    write_atomic(path, _write)

    flagged = int(df["meets_threshold"].sum()) if "meets_threshold" in df.columns else 0
    log.info("Wrote success-flag file (%d/%d drivers meet the %.0f%% threshold) -> %s",
             flagged, len(df), threshold * 100, path)
    return path


# ------------------------------------------------------------------ reading

def _to_bool(series: pd.Series) -> pd.Series:
    """
    Tolerant boolean coercion for a hand-edited Excel column: real booleans
    round-trip through openpyxl fine, but a user typing TRUE/FALSE/1/0/yes
    into a cell should also work.
    """
    return series.apply(
        lambda v: str(v).strip().upper() in ("TRUE", "1", "1.0", "YES") if pd.notna(v) else False
    )


def load_flags(flag_path: Path) -> pd.DataFrame:
    """
    Read the success-flag Excel file. Returns an empty (but correctly
    columned) DataFrame if the file is missing or unreadable — callers
    should treat that as "nothing flagged" rather than an error, so
    --skip-flagged never crashes a run, it just has nothing to skip.
    """
    empty = pd.DataFrame(columns=["driver_id", "meets_threshold"])
    if not flag_path.exists():
        log.warning("Flag file not found at %s; nothing will be skipped this run", flag_path)
        return empty
    try:
        df = pd.read_excel(flag_path)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read flag file %s (%s); nothing will be skipped this run", flag_path, exc)
        return empty
    if "driver_id" not in df.columns or "meets_threshold" not in df.columns:
        log.warning("Flag file %s is missing expected columns; nothing will be skipped this run", flag_path)
        return empty
    df = df.copy()
    df["meets_threshold"] = _to_bool(df["meets_threshold"])
    return df


def flagged_driver_ids(flags_df: pd.DataFrame) -> set[str]:
    if flags_df.empty or "meets_threshold" not in flags_df.columns:
        return set()
    return set(flags_df.loc[flags_df["meets_threshold"], "driver_id"].astype(str))


# ------------------------------------------------------------------ apply

def apply_skip_flagged(queue: list[DriverSpec], flags_df: pd.DataFrame,
                       orchestrator, writer, interim_dir: Path) -> list[DriverSpec]:
    """
    Removes drivers already flagged as sufficiently complete from the fetch
    queue, reconstructing each one's result from its cached interim
    observations (output/interim/<driver_id>_observations.csv) via
    orchestrator.reuse_cached_result() and recording it through writer.add()
    — so this run's outputs (monthly panel, trackers, manifest) still
    include it, without a live fetch.

    Never drops a driver silently: if the flag file has nothing usable, or a
    specific flagged driver has no cached interim file, or reconstruction
    yields no usable data (e.g. the run window moved since it was cached),
    that driver is left in the queue for a normal live fetch instead.

    Returns the remaining queue (drivers that still need a live fetch).
    """
    ids = flagged_driver_ids(flags_df)
    if not ids:
        return queue

    by_id = {str(row["driver_id"]): row for _, row in flags_df.iterrows()}
    remaining: list[DriverSpec] = []
    reused = 0

    for spec in queue:
        row = by_id.get(spec.driver_id) if spec.driver_id in ids else None
        if row is None:
            remaining.append(spec)
            continue

        interim_path = interim_dir / f"{spec.driver_id}_observations.csv"
        if not interim_path.exists():
            log.warning("%s is flagged for skip but has no cached observations at %s; fetching live",
                       spec.driver_id, interim_path)
            remaining.append(spec)
            continue

        try:
            raw = pd.read_csv(interim_path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not read cached observations for %s (%s); fetching live",
                       spec.driver_id, exc)
            remaining.append(spec)
            continue

        if {"obs_date", "value"}.issubset(raw.columns):
            raw = raw[["obs_date", "value"]]

        tier_used = row.get("tier_used")
        try:
            tier = Tier(int(tier_used)) if pd.notna(tier_used) else spec.declared_tier
        except (TypeError, ValueError):
            tier = spec.declared_tier

        retrieved_at = pd.to_datetime(row.get("retrieved_at"), utc=True, errors="coerce")
        retrieved_at = utc_now() if pd.isna(retrieved_at) else retrieved_at.to_pydatetime()

        result, monthly = orchestrator.reuse_cached_result(
            spec, raw, retrieved_at=retrieved_at,
            source_url=str(row.get("source_url") or "") or spec.source_url,
            tier_used=tier,
        )
        if monthly.empty or monthly["value"].notna().sum() == 0:
            log.warning("%s is flagged for skip but cached data no longer rolls up to anything "
                       "usable (run window may have moved); fetching live", spec.driver_id)
            remaining.append(spec)
            continue

        writer.add(spec, result, monthly, 0.0)
        reused += 1

    log.info("Reused %d driver(s) already flagged sufficiently complete; %d queued for live fetch",
             reused, len(remaining))
    return remaining
