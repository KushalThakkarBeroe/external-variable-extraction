#!/usr/bin/env python3
"""
Final, deduplicated combined report across the whole registry plus a
dedicated view of how the padma "alternate source / same source" 151-driver
batch behaved.

Reads:
  - input/driver_registry.csv                         current live registry
  - .backup_before_padma_drop_removal_.../driver_registry.csv
                                                        pre-drop snapshot (to compute what was removed)
  - output/monthly/run_report_v3.0.xlsx                original 512-driver baseline
  - output/monthly/run_report_*.xlsx                   every fresh run produced since (pilot + full batch)
  - output/source_comparison_manifest_*.json           every reconciled padma-151 batch manifest

Writes one new workbook, output/monthly/run_report_final_combined_<ts>.xlsx:
  - Summary          full-registry totals + the "previously successful but
                     removed" callout + the 151-batch summary table
  - All Drivers      one row per CURRENT registry root driver, deduplicated,
                     best-known outcome (freshest actual fetch if touched
                     today, else the v3.0 baseline), our internal _PADMA /
                     _CMPTEST / _PREV machinery rows excluded
  - Successful / Failed & HITL   same split, from the same deduplicated set
  - Dropped (Removed)            the drivers removed from the registry by the
                                  padma-drop cleanup, with their baseline outcome,
                                  so a removed *successful* driver is never
                                  mistaken for a fresh failure
  - 151 Source Comparison        one row per padma-151 candidate with its case
                                  and reconciled decision

Example
-------
    python combine_final_report.py
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import openpyxl                                 # noqa: E402
import pandas as pd                              # noqa: E402
from openpyxl.styles import Font, PatternFill    # noqa: E402

from pipeline.source_comparison import load_registry_rows, norm   # noqa: E402

ROOT = Path(__file__).parent

_INTERNAL_SUFFIXES = ("_PADMA", "_CMPTEST", "_PREV")


def _is_internal_row(driver_id: str) -> bool:
    return any(driver_id.endswith(suf) or f"{suf}{n}" in driver_id
               for suf in _INTERNAL_SUFFIXES for n in ("", "2", "3", "4"))


def _clean_quality_notes(value) -> str:
    """Normalizes a quality_notes cell to a plain string, never NaN/None.

    openpyxl returns None for a blank cell; once that list of row-dicts goes
    through pd.DataFrame(...), pandas silently upgrades None to float('nan')
    -- and bool(float('nan')) is True in Python, so a naive truthiness check
    would misclassify every "no notes" success as "has notes". Must be
    normalized to "" right where a cell value first enters a DataFrame.
    """
    if isinstance(value, list):
        return "; ".join(value)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def load_report_all_drivers(path: Path) -> pd.DataFrame:
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb["All Drivers"]
    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    rows = [dict(zip(header, r)) for r in ws.iter_rows(min_row=2, values_only=True)]
    return pd.DataFrame(rows)


def find_fresh_reports(curated_dir: Path) -> list[Path]:
    """Every run_report_*.xlsx EXCEPT v3.0 and its derivatives -- i.e. every
    report an actual run_pipeline.py invocation produced today, in
    chronological order (oldest first, so a later dict.update() naturally
    keeps the newest result when a driver_id appears in more than one)."""
    all_reports = [Path(p) for p in glob.glob(str(curated_dir / "run_report_2*.xlsx"))]
    all_reports = [p for p in all_reports if not p.name.startswith("~$") and "_source_comparison" not in p.stem]
    return sorted(all_reports, key=lambda p: p.stat().st_mtime)


def build_best_known_outcomes(baseline_df: pd.DataFrame, fresh_reports: list[Path]) -> dict[str, dict]:
    """driver_id -> {outcome, coverage_ratio, monthly_observations, quality_notes,
    source: 'baseline' | '<report filename>'}"""
    best: dict[str, dict] = {}
    for _, row in baseline_df.iterrows():
        best[row["driver_id"]] = {
            "outcome": row.get("outcome"),
            "coverage_ratio": float(row.get("coverage_ratio") or 0.0),
            "monthly_observations": int(row.get("monthly_observations") or 0),
            "quality_notes": _clean_quality_notes(row.get("quality_notes")),
            "commodity": row.get("commodity"),
            "region": row.get("region"),
            "driver": row.get("driver"),
            "source": row.get("source") or "",
            "source_url": row.get("source_url") or "",
            "result_source": "baseline (run_report_v3.0)",
        }
    for report_path in fresh_reports:
        try:
            df = load_report_all_drivers(report_path)
        except Exception as exc:  # noqa: BLE001
            print(f"  (skipping unreadable report {report_path.name}: {exc})")
            continue
        for _, row in df.iterrows():
            did = row.get("driver_id")
            if not did:
                continue
            best[did] = {
                "outcome": row.get("outcome"),
                "coverage_ratio": float(row.get("coverage_ratio") or 0.0),
                "monthly_observations": int(row.get("monthly_observations") or 0),
                "quality_notes": _clean_quality_notes(row.get("quality_notes")),
                "commodity": row.get("commodity"),
                "region": row.get("region"),
                "driver": row.get("driver"),
                "source": row.get("source") or "",
                "source_url": row.get("source_url") or "",
                "result_source": f"fresh ({report_path.stem})",
            }
    return best


def load_failure_categories(v3_path: Path) -> dict[str, str]:
    """driver_id -> failure_category, read from run_report_v3.0.xlsx's own
    'Failed & HITL' sheet (the evidence-based categorization built earlier
    this session). Only ever meaningful for a failed driver; a driver that's
    currently succeeding gets no category regardless of what's in here."""
    wb = openpyxl.load_workbook(v3_path, read_only=True)
    if "Failed & HITL" not in wb.sheetnames:
        return {}
    ws = wb["Failed & HITL"]
    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    if "failure_category" not in header:
        return {}
    idx = {h: i for i, h in enumerate(header)}
    out = {}
    for r in ws.iter_rows(min_row=2, values_only=True):
        did = r[idx["driver_id"]]
        cat = r[idx["failure_category"]]
        if did and cat:
            out[did] = cat
    return out


def find_backup_pre_drop_registry(root: Path) -> Path | None:
    matches = sorted(root.glob(".backup_before_padma_drop_removal_*"), key=lambda p: p.name)
    for m in matches:
        candidate = m / "driver_registry.csv"
        if candidate.exists():
            return candidate
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry", default="input/driver_registry.csv")
    parser.add_argument("--baseline-report", default="output/monthly/run_report_v3.0.xlsx")
    parser.add_argument("--curated-dir", default="output/monthly")
    parser.add_argument("--manifest-glob", default="output/source_comparison_manifest_*.json")
    parser.add_argument("--output-name", default=None,
                        help="override the auto-timestamped output filename, e.g. run_report_v4.0.xlsx")
    args = parser.parse_args()

    registry_path = ROOT / args.registry
    baseline_path = ROOT / args.baseline_report
    curated_dir = ROOT / args.curated_dir

    failure_categories = load_failure_categories(baseline_path)
    print(f"Loaded {len(failure_categories)} failure_category value(s) from {baseline_path.name}")

    # ---- 1. current registry: root drivers, excluding our own machinery rows
    _, reg_rows = load_registry_rows(registry_path)
    referenced = set(r["fallback_driver_id"] for r in reg_rows if r["fallback_driver_id"])
    current_roots = [r for r in reg_rows
                     if r["driver_id"] not in referenced and not _is_internal_row(r["driver_id"])]
    current_root_ids = {r["driver_id"] for r in current_roots}
    print(f"Current registry: {len(reg_rows)} total rows, {len(current_roots)} real root drivers "
          f"(after excluding fallback rows and internal _PADMA/_CMPTEST/_PREV machinery rows)")

    # ---- 2. baseline + fresh reports -> best known outcome per driver_id
    baseline_df = load_report_all_drivers(baseline_path)
    fresh_reports = find_fresh_reports(curated_dir)
    print(f"Fresh reports found (newest wins on overlap): {[p.name for p in fresh_reports]}")
    best = build_best_known_outcomes(baseline_df, fresh_reports)

    # ---- 3. removed drivers: root drivers in the pre-drop backup but not in the current registry
    backup_path = find_backup_pre_drop_registry(ROOT)
    removed_rows = []
    if backup_path:
        _, backup_rows = load_registry_rows(backup_path)
        backup_referenced = set(r["fallback_driver_id"] for r in backup_rows if r["fallback_driver_id"])
        backup_roots = [r for r in backup_rows if r["driver_id"] not in backup_referenced]
        for r in backup_roots:
            if r["driver_id"] not in current_root_ids:
                b = best.get(r["driver_id"], {})
                removed_rows.append({
                    "driver_id": r["driver_id"], "commodity": r["commodity"], "driver_name": r["driver_name"],
                    "region": r["region"], "baseline_outcome": b.get("outcome", "not in baseline"),
                    "was_successful": b.get("outcome") == "success",
                })
        print(f"Removed-from-registry drivers found: {len(removed_rows)} "
              f"(comparing current registry against {backup_path})")
    else:
        print("WARNING: no pre-drop registry backup found; 'previously successful but removed' count will be 0")
    removed_and_successful = sum(1 for r in removed_rows if r["was_successful"])

    # ---- 4. build the deduplicated All Drivers view
    all_drivers_rows = []
    for r in current_roots:
        did = r["driver_id"]
        b = best.get(did)
        if b is None:
            outcome = "never_attempted"
            row = {
                "driver_id": did, "commodity": r["commodity"], "region": r["region"], "driver": r["driver_name"],
                "outcome": outcome, "coverage_ratio": 0.0, "monthly_observations": 0,
                "quality_notes": "", "result_source": "n/a (added to registry, never run)",
            }
        else:
            outcome = b["outcome"]
            row = {
                "driver_id": did, "commodity": r["commodity"], "region": r["region"], "driver": r["driver_name"],
                "outcome": outcome, "coverage_ratio": b["coverage_ratio"],
                "monthly_observations": b["monthly_observations"],
                "quality_notes": b["quality_notes"],  # already normalized to a plain string by build_best_known_outcomes
                "result_source": b["result_source"],
            }
        # Only a failed driver gets a category -- a currently-succeeding one
        # never shows a stale category from whenever it used to fail.
        row["failure_category"] = "" if outcome == "success" else failure_categories.get(did, "")
        all_drivers_rows.append(row)
    all_drivers_df = pd.DataFrame(all_drivers_rows).sort_values(["commodity", "region", "driver_id"])
    dup_ids = all_drivers_df["driver_id"][all_drivers_df["driver_id"].duplicated()].tolist()
    if dup_ids:
        print(f"WARNING: duplicate driver_id(s) found after dedup, this should not happen: {dup_ids}")

    total = len(all_drivers_df)
    succeeded = all_drivers_df["outcome"] == "success"
    has_notes = all_drivers_df["quality_notes"].astype(bool)
    clean = succeeded & ~has_notes
    partial = succeeded & has_notes
    failed = ~succeeded

    print(f"\nFull-registry totals: {total} drivers | {int(succeeded.sum())} succeeded "
          f"({int(clean.sum())} clean, {int(partial.sum())} partial) | {int(failed.sum())} failed")
    print(f"Previously successful but since dropped/removed: {removed_and_successful} "
          f"(of {len(removed_rows)} total removed)")

    # ---- 5. 151 source-comparison batch view
    manifest_paths = sorted(ROOT.glob(args.manifest_glob))
    all_candidates = []
    for mp in manifest_paths:
        all_candidates.extend(json.loads(mp.read_text(encoding="utf-8")))
    print(f"\nSource-comparison manifests loaded: {[p.name for p in manifest_paths]} "
          f"-> {len(all_candidates)} candidates total")

    seen_driver_keys = set()
    dedup_candidates = []
    for c in all_candidates:
        key = (norm(c["commodity"]), norm(c["driver_name"]), norm(c["region"]))
        if key in seen_driver_keys:
            continue
        seen_driver_keys.add(key)
        dedup_candidates.append(c)
    if len(dedup_candidates) != len(all_candidates):
        print(f"  (deduplicated {len(all_candidates) - len(dedup_candidates)} candidate(s) appearing in more than one batch)")

    batch_df = pd.DataFrame(dedup_candidates)
    decision_counts = batch_df["decision_tag"].value_counts().to_dict() if not batch_df.empty else {}
    already_successful_going_in = int((batch_df["case"] == "comparison").sum()) if not batch_df.empty else 0

    print(f"151-batch decision breakdown: {decision_counts}")

    # =========================================================================
    # write the workbook
    # =========================================================================
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = curated_dir / (args.output_name or f"run_report_final_combined_{timestamp}.xlsx")

    header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    header_font = Font(bold=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # ---- Summary sheet: two tables ----
        summary_rows = [
            {"Metric": "Total drivers in current registry (deduplicated, root drivers only)", "Count": total},
            {"Metric": "Succeeded (total)", "Count": int(succeeded.sum())},
            {"Metric": "  Succeeded, Clean", "Count": int(clean.sum())},
            {"Metric": "  Succeeded, Partial (less than 8 years of history)", "Count": int(partial.sum())},
            {"Metric": "Failed / never attempted", "Count": int(failed.sum())},
            {"Metric": "", "Count": None},
            {"Metric": "Removed from the registry (padma 'Drop' cleanup)", "Count": len(removed_rows)},
            {"Metric": "  Of which were SUCCESSFUL before removal -- not a regression, a deliberate drop",
             "Count": removed_and_successful},
        ]
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

        batch_summary_rows = [
            {"Metric": "151-driver padma 'alternate/same source' batch -- total candidates", "Count": len(dedup_candidates)},
            {"Metric": "  Already successful going in (comparison case)", "Count": already_successful_going_in},
            {"Metric": "  Comparison: alternate source WON (swapped in)", "Count": decision_counts.get("swapped", 0)},
            {"Metric": "  Comparison: kept previous source", "Count": decision_counts.get("kept_previous", 0)},
            {"Metric": "  Swap-in: NEW success (was failing before)", "Count": decision_counts.get("new_success", 0)},
            {"Metric": "  Swap-in: alternate also failed", "Count": decision_counts.get("still_failing", 0)},
            {"Metric": "  Skipped (no usable source URL)", "Count": decision_counts.get("skipped", 0)},
            {"Metric": "  Not yet run (no decision recorded)", "Count": int(batch_df["decision_tag"].eq("").sum()) if not batch_df.empty else len(dedup_candidates)},
            {"Metric": "", "Count": None},
            {"Metric": "Net NEW successes this batch adds to the overall total above", "Count": decision_counts.get("new_success", 0)},
        ]
        pd.DataFrame(batch_summary_rows).to_excel(writer, sheet_name="Summary", index=False,
                                                    startrow=len(summary_rows) + 3)

        # ---- All Drivers / Successful / Failed & HITL ----
        all_drivers_df.to_excel(writer, sheet_name="All Drivers", index=False)
        all_drivers_df[succeeded.values].to_excel(writer, sheet_name="Successful", index=False)
        all_drivers_df[~succeeded.values].to_excel(writer, sheet_name="Failed & HITL", index=False)

        # ---- Dropped (Removed) ----
        if removed_rows:
            pd.DataFrame(removed_rows).to_excel(writer, sheet_name="Dropped (Removed)", index=False)

        # ---- 151 Source Comparison ----
        if not batch_df.empty:
            cols = ["driver_id", "commodity", "driver_name", "region", "case", "padma_source_status",
                    "alt_source_url", "baseline_outcome", "baseline_coverage_ratio",
                    "baseline_monthly_observations", "decision_tag", "decision_detail", "reason"]
            cols = [c for c in cols if c in batch_df.columns]
            batch_df[cols].to_excel(writer, sheet_name="151 Source Comparison", index=False)

        # ---- By Commodity ----
        # "succeeded_with_quality_notes" (the run_pipeline.py-native definition)
        # is replaced here with "succeeded_at_least_70pct_rows_filled" per the
        # user's request -- a different, coverage_ratio-driven definition, not
        # just a label rename, so it's recomputed from scratch per commodity.
        by_commodity_rows = []
        for commodity, group in all_drivers_df.groupby("commodity", sort=True):
            is_success = group["outcome"] == "success"
            succeeded_n = int(is_success.sum())
            at_least_70 = int((is_success & (group["coverage_ratio"] >= 0.7)).sum())
            failed_n = int((~is_success).sum())
            by_commodity_rows.append({
                "commodity": commodity,
                "drivers_attempted": len(group),
                "succeeded": succeeded_n,
                "succeeded_at_least_70pct_rows_filled": at_least_70,
                "failed": failed_n,
                "total_monthly_observations": int(group["monthly_observations"].sum()),
            })
        pd.DataFrame(by_commodity_rows).to_excel(writer, sheet_name="By Commodity", index=False)

    # ---- header styling pass ----
    wb = openpyxl.load_workbook(out_path)
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        for cell in next(ws.iter_rows(min_row=1, max_row=1)):
            cell.fill = header_fill
            cell.font = header_font
    wb.save(out_path)

    print(f"\nWrote final combined report -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
