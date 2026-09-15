#!/usr/bin/env python3
"""
Phase 1 of the padma alternate/same-source comparison feature.

Builds the candidate list (padma's "Alternate Source" / "Same Source" /
"Alternate Region/Source Suggested" flagged drivers, cross-referenced against
the current registry and the last known run outcome), then mutates
input/driver_registry.csv non-destructively so the selected batch can be
exercised through the *existing, unmodified* run_pipeline.py:

  - swap-in candidates (currently failing): alternate source appended as a
    new fallback row at the tail of the existing chain.
  - comparison candidates (currently succeeding): a standalone temporary
    probe row, not wired into any fallback chain, so it can be fetched
    independently without touching the already-working primary row.

Nothing in pipeline/cascade.py, connectors.py, or run_pipeline.py is
modified by this script or by the feature as a whole.

Examples
--------
Pilot (10-15 stratified across swap-in/comparison/skip), fresh registry backup:
    python prepare_source_candidates.py --batch-tag pilot --limit 12

Everything not already covered by an earlier batch:
    python prepare_source_candidates.py --batch-tag full --exclude-manifest output/source_comparison_manifest_pilot.json
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.source_comparison import (   # noqa: E402
    apply_candidate_registry_rows, backup_registry, build_candidates,
    load_registry_rows, read_manifest, write_manifest, write_registry,
)

ROOT = Path(__file__).parent


def stratified_sample(candidates: list, limit: int) -> list:
    """Proportional pick across case buckets (swap_in / comparison / skip),
    sorted by driver_id within each bucket for reproducibility -- no RNG."""
    buckets: dict[str, list] = {"swap_in": [], "comparison": [], "skip": []}
    for c in candidates:
        buckets.setdefault(c.case, []).append(c)
    for b in buckets.values():
        b.sort(key=lambda c: c.driver_id)

    total = sum(len(b) for b in buckets.values())
    if total <= limit:
        return candidates

    picked: list = []
    for case, bucket in buckets.items():
        if not bucket:
            continue
        share = max(1, round(limit * len(bucket) / total))
        picked.extend(bucket[:share])
    return picked[:limit] if len(picked) > limit else picked


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--padma-path", default="output/monthly/padma-External_Driver_Validation_Final.xlsx")
    parser.add_argument("--registry", default="input/driver_registry.csv")
    parser.add_argument("--baseline-report", default="output/monthly/run_report_v3.0.xlsx")
    parser.add_argument("--batch-tag", required=True, help="e.g. 'pilot' or 'full' -- names the output manifest/ids files")
    parser.add_argument("--limit", type=int, default=None, help="stratified sample size (omit for all in-scope candidates)")
    parser.add_argument("--exclude-manifest", default=None,
                        help="path to a prior batch's manifest -- its driver_ids are excluded from this batch")
    args = parser.parse_args()

    padma_path = ROOT / args.padma_path
    registry_path = ROOT / args.registry
    baseline_path = ROOT / args.baseline_report

    candidates = build_candidates(padma_path, registry_path, baseline_path)
    print(f"Total in-scope padma candidates: {len(candidates)}")

    if args.exclude_manifest:
        prior = read_manifest(ROOT / args.exclude_manifest)
        prior_ids = {c.driver_id for c in prior}
        before = len(candidates)
        candidates = [c for c in candidates if c.driver_id not in prior_ids]
        print(f"Excluded {before - len(candidates)} already covered by {args.exclude_manifest}")

    if args.limit is not None:
        candidates = stratified_sample(candidates, args.limit)

    by_case = {"swap_in": 0, "comparison": 0, "skip": 0}
    for c in candidates:
        by_case[c.case] = by_case.get(c.case, 0) + 1
    print(f"This batch ({args.batch_tag}): {len(candidates)} total -> "
          f"{by_case.get('swap_in', 0)} swap-in, {by_case.get('comparison', 0)} comparison, "
          f"{by_case.get('skip', 0)} skip")

    fieldnames, rows = load_registry_rows(registry_path)
    backup_dir = backup_registry(registry_path, f"source_comparison_{args.batch_tag}")
    print(f"Backed up registry to {backup_dir}")

    candidates = apply_candidate_registry_rows(fieldnames, rows, candidates)
    write_registry(registry_path, fieldnames, rows)
    print(f"Registry updated in place: {registry_path} ({len(rows)} total rows)")

    manifest_path = ROOT / "output" / f"source_comparison_manifest_{args.batch_tag}.json"
    write_manifest(candidates, manifest_path)
    print(f"Wrote candidate manifest -> {manifest_path}")

    ids_path = ROOT / "output" / f"source_comparison_ids_{args.batch_tag}.txt"
    ids = []
    for c in candidates:
        if c.case == "swap_in" and c.driver_id:
            ids.append(c.driver_id)   # the ROOT id -- cascade walks into the new fallback automatically
        elif c.case == "comparison" and c.comparison_temp_id:
            ids.append(c.comparison_temp_id)  # the standalone probe id
    ids_path.write_text("\n".join(ids) + "\n", encoding="utf-8")
    print(f"Wrote {len(ids)} driver_id(s) to run -> {ids_path}")

    print()
    print("Next step:")
    print(f"    python run_pipeline.py --driver-id-file {ids_path.relative_to(ROOT)}")
    print("Then reconcile:")
    print(f"    python reconcile_source_comparison.py --manifest {manifest_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
