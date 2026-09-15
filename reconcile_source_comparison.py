#!/usr/bin/env python3
"""
Phase 3 of the padma alternate/same-source comparison feature.

Reads a batch's candidate manifest (written by prepare_source_candidates.py)
plus the fresh run_report_*.xlsx produced by an intervening, unmodified
`python run_pipeline.py --driver-id-file ...` run, decides a winner for each
candidate, applies the *final* non-destructive registry mutation (swap-in
candidates need none -- they're already correctly wired as a fallback;
comparison candidates get their winning source promoted to primary, the
loser preserved as a fallback, and the temporary probe row removed), and
writes a color-coded copy of the report with a new
"source_comparison_result" column.

Example
-------
    python reconcile_source_comparison.py --manifest output/source_comparison_manifest_pilot.json
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.source_comparison import (   # noqa: E402
    DATE_TAG, backup_registry, load_registry_rows, read_manifest, write_manifest, write_registry,
)

import openpyxl                              # noqa: E402
from openpyxl.styles import PatternFill        # noqa: E402
from openpyxl.utils import get_column_letter   # noqa: E402

ROOT = Path(__file__).parent

TAG_COLORS = {
    "swapped": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),   # green
    "kept_previous": PatternFill(start_color="DCE6F1", end_color="DCE6F1", fill_type="solid"),  # blue
    "new_success": PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),  # green
    "still_failing": PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),  # red/amber
    "skipped": PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"),   # grey
    "probe": PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid"),     # light grey
}


def find_latest_report(curated_dir: Path) -> Path:
    candidates = sorted(
        [p for p in curated_dir.glob("run_report_*.xlsx") if not p.name.startswith("~$")],
        key=lambda p: p.stat().st_mtime, reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No run_report_*.xlsx found in {curated_dir}")
    return candidates[0]


def load_fresh_results(report_path: Path) -> dict[str, dict]:
    wb = openpyxl.load_workbook(report_path, read_only=True)
    ws = wb["All Drivers"]
    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(header)}
    out = {}
    for r in ws.iter_rows(min_row=2, values_only=True):
        did = r[idx["driver_id"]]
        out[did] = {
            "outcome": r[idx["outcome"]],
            "coverage_ratio": float(r[idx["coverage_ratio"]] or 0.0),
            "monthly_observations": int(r[idx["monthly_observations"]] or 0),
            "connector": r[idx["connector"]],
            "source": r[idx["source"]],
            "source_url": r[idx["source_url"]],
        }
    return out


def reconcile_one(c, fresh: dict[str, dict]) -> tuple[str, str]:
    """Returns (tag_text, tag_key) where tag_key indexes TAG_COLORS."""
    if c.case == "skip":
        return f"Skipped - {c.reason}", "skipped"

    if c.case == "swap_in":
        r = fresh.get(c.fallback_new_id)
        if r and r["outcome"] == "success":
            return "New: alternate source succeeded (was failing before)", "new_success"
        return "Alternate also failed (kept previous source as primary; alt remains a fallback)", "still_failing"

    if c.case == "comparison":
        r = fresh.get(c.comparison_temp_id)
        alt_ok = bool(r and r["outcome"] == "success")
        alt_cov = r["coverage_ratio"] if alt_ok else -1.0
        alt_obs = r["monthly_observations"] if alt_ok else 0
        base_cov = c.baseline_coverage_ratio
        base_obs = c.baseline_monthly_observations

        if alt_ok and (alt_cov > base_cov or (alt_cov == base_cov and alt_obs > base_obs)):
            return (f"Swapped: alternate source retrieved more data "
                    f"(coverage {alt_cov:.2f} vs {base_cov:.2f}, obs {alt_obs} vs {base_obs})", "swapped")
        if alt_ok:
            return (f"Kept: previous source already best "
                    f"(coverage {base_cov:.2f} vs alt's {alt_cov:.2f}, obs {base_obs} vs {alt_obs})", "kept_previous")
        return "Alternate attempted, failed to retrieve data (kept previous source)", "kept_previous"

    return "Unrecognized case", "skipped"


def swap_primary_fields(fieldnames, by_id, c, fresh):
    """Comparison winner case: original row's fields become a new fallback
    row (preserving the old, working source); the temp probe's fields are
    promoted onto the original row (now the primary)."""
    original = by_id[c.driver_id]
    probe = by_id[c.comparison_temp_id]

    old_fallback_id = f"{c.driver_id}_PREV"
    n = 2
    while old_fallback_id in by_id:
        old_fallback_id = f"{c.driver_id}_PREV{n}"
        n += 1

    old_source_row = {fn: original.get(fn, "") for fn in fieldnames}
    old_source_row.update({
        "driver_id": old_fallback_id,
        "driver_name": f"{original['driver_name']} (previous source, demoted {DATE_TAG})",
        "fallback_driver_id": original.get("fallback_driver_id", ""),
        "transform_hint": (f"[padma source-comparison {DATE_TAG}] demoted from primary -- the padma-suggested "
                            f"alternate retrieved more data. Preserved as a fallback, not deleted."),
    })

    transfer_fields = ["connector", "source_name", "source_url", "endpoint_or_locator", "access_mode",
                       "native_frequency", "rollup_method", "unit"]
    for fn in transfer_fields:
        original[fn] = probe.get(fn, "")
    original["fallback_driver_id"] = old_fallback_id
    original["is_proxy"] = "Y"
    original["proxy_note"] = (f"Source swapped {DATE_TAG} per padma alternate-source comparison "
                              f"(coverage {fresh.get(c.comparison_temp_id, {}).get('coverage_ratio', 0):.2f} "
                              f"vs previous {c.baseline_coverage_ratio:.2f}).")
    original["transform_hint"] = (f"[padma source-comparison {DATE_TAG}] promoted to primary; previous source "
                                  f"preserved as fallback {old_fallback_id}.")

    return old_source_row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--registry", default="input/driver_registry.csv")
    parser.add_argument("--report", default=None, help="override auto-detected latest run_report_*.xlsx")
    parser.add_argument("--curated-dir", default="output/monthly")
    args = parser.parse_args()

    manifest_path = ROOT / args.manifest
    registry_path = ROOT / args.registry
    curated_dir = ROOT / args.curated_dir

    candidates = read_manifest(manifest_path)
    report_path = ROOT / args.report if args.report else find_latest_report(curated_dir)
    print(f"Reading fresh results from {report_path}")
    fresh = load_fresh_results(report_path)

    fieldnames, rows = load_registry_rows(registry_path)
    by_id = {r["driver_id"]: r for r in rows}
    backup_dir = backup_registry(registry_path, "source_comparison_reconcile")
    print(f"Backed up registry to {backup_dir}")

    results = []
    rows_to_remove: set[str] = set()
    new_fallback_rows = []

    for c in candidates:
        tag_text, tag_key = reconcile_one(c, fresh)
        results.append((c, tag_text, tag_key))
        c.decision_tag = tag_key
        c.decision_detail = tag_text

        if c.case == "comparison" and c.comparison_temp_id:
            if tag_key == "swapped":
                new_fallback_rows.append(swap_primary_fields(fieldnames, by_id, c, fresh))
            rows_to_remove.add(c.comparison_temp_id)   # probe row always removed after reconciliation

    rows = [r for r in rows if r["driver_id"] not in rows_to_remove]
    rows.extend(new_fallback_rows)
    write_registry(registry_path, fieldnames, rows)
    print(f"Registry reconciled: removed {len(rows_to_remove)} temp probe row(s), "
          f"added {len(new_fallback_rows)} demoted-source fallback row(s). New total: {len(rows)} rows")

    # Persist the decision back into the manifest -- combine_final_report.py
    # (or anything else later) reads this instead of re-deriving it.
    write_manifest(candidates, manifest_path)
    print(f"Persisted decisions back into {manifest_path}")

    # ---- summary -----------------------------------------------------------
    counts = {"new_success": 0, "still_failing": 0, "swapped": 0, "kept_previous": 0, "skipped": 0}
    for c, tag_text, tag_key in results:
        counts[tag_key] = counts.get(tag_key, 0) + 1

    print()
    print("=" * 78)
    print(f"Batch summary ({len(candidates)} candidates)")
    print("=" * 78)
    print(f"  Swap-in, NEW success (was failing)  : {counts['new_success']:>4}   <- adds to the 190 baseline")
    print(f"  Swap-in, still failing               : {counts['still_failing']:>4}")
    print(f"  Comparison, alternate WON (swapped)  : {counts['swapped']:>4}   (already counted in 190, quality improved)")
    print(f"  Comparison, kept previous source     : {counts['kept_previous']:>4}   (already counted in 190, unchanged)")
    print(f"  Skipped (no usable URL)              : {counts['skipped']:>4}")
    print("-" * 78)
    net_new = counts["new_success"]
    print(f"  Net NEW successes to add to 190      : {net_new:>4}  ->  190 + {net_new} = {190 + net_new}")
    print("=" * 78)

    # ---- color-coded report column ------------------------------------------
    out_report_path = report_path.with_name(report_path.stem + "_source_comparison.xlsx")
    shutil.copy(report_path, out_report_path)

    tag_by_driver_id: dict[str, tuple[str, str]] = {}
    for c, tag_text, tag_key in results:
        if c.driver_id:
            tag_by_driver_id[c.driver_id] = (tag_text, tag_key)
        if c.case == "comparison" and c.comparison_temp_id:
            tag_by_driver_id[c.comparison_temp_id] = (
                f"Comparison probe for {c.driver_id} -- see that row for the decision", "probe")
        if c.case == "swap_in" and c.fallback_new_id:
            tag_by_driver_id[c.fallback_new_id] = (
                f"Swap-in fallback source for {c.driver_id} -- see that row for the decision", "probe")

    wb = openpyxl.load_workbook(out_report_path)
    for sheet_name in ("All Drivers", "Successful", "Failed & HITL"):
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
        if "driver_id" not in header:
            continue
        did_col = header.index("driver_id") + 1
        new_col = ws.max_column + 1
        ws.cell(row=1, column=new_col, value="source_comparison_result")

        for row_idx in range(2, ws.max_row + 1):
            did = ws.cell(row=row_idx, column=did_col).value
            entry = tag_by_driver_id.get(did)
            if entry is None:
                continue
            tag_text, tag_key = entry
            ws.cell(row=row_idx, column=new_col, value=tag_text)
            fill = TAG_COLORS.get(tag_key)
            if fill is not None:
                for col in range(1, new_col + 1):
                    ws.cell(row=row_idx, column=col).fill = fill

    wb.save(out_report_path)
    print(f"\nColor-coded report -> {out_report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
