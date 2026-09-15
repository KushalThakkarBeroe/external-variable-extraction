#!/usr/bin/env python3
"""
Standalone: build a rerun worklist Excel from an existing run_manifest.json,
without re-running the pipeline.

Lists exactly the failed drivers worth another automated attempt: excludes
anything already successful, anything already resolved via a manual drop,
and anything in the manual-upload category (paid/restricted, login-gated,
or connector=manual_yaml) -- see pipeline/rerun_worklist.py for the exact
exclusion rules. Feed the output's driver_id column straight into
--driver-id-file for the actual rerun:

    python generate_rerun_worklist.py
    python run_pipeline.py --driver-id-file output/rerun_worklist_<ts>.xlsx --skip-flagged

(--driver-id-file reads a plain-text one-id-per-line file, not the xlsx
directly -- see the printed hint at the end of this script for a one-liner
to extract the column.)

Examples
--------
    python generate_rerun_worklist.py
    python generate_rerun_worklist.py --manifest output/run_manifest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd                                                     # noqa: E402

from pipeline.config import Config                                       # noqa: E402
from pipeline.rerun_worklist import build_worklist_df, write_worklist_excel  # noqa: E402

log = logging.getLogger("generate_rerun_worklist")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a rerun worklist (automated-source-but-failed drivers) from an "
                    "existing run manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--manifest", default=None,
                        help="Override paths.manifest from config.yaml")
    parser.add_argument("--registry", default=None,
                        help="Override paths.registry from config.yaml")
    parser.add_argument("--out", default=None,
                        help="Output path; defaults to output/rerun_worklist_<timestamp>.xlsx")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s | %(levelname)-8s | %(message)s")

    cfg = Config.load(args.config)
    manifest_path = Path(args.manifest) if args.manifest else cfg.path("paths.manifest", "output/run_manifest.json")
    registry_path = Path(args.registry) if args.registry else cfg.path("paths.registry")
    manual_dir = cfg.path("paths.manual_drop", "input/manual")

    if not manifest_path.exists():
        log.error("Manifest not found at %s -- run the pipeline at least once first", manifest_path)
        return 1
    if not registry_path.exists():
        log.error("Registry not found at %s", registry_path)
        return 1

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = manifest.get("drivers", [])
    if not entries:
        log.warning("Manifest at %s has no driver entries; nothing to build a worklist from", manifest_path)
        return 1

    registry_df = pd.read_csv(registry_path, dtype=str, keep_default_na=False)
    df = build_worklist_df(entries, registry_df, manual_dir)
    if df.empty:
        log.info("Nothing to rerun -- every failed driver is either resolved, already "
                "successful, or manual-upload category.")
        return 0

    if args.out:
        out_path = Path(args.out)
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = cfg.path("paths.curated", "output/monthly") / f"rerun_worklist_{timestamp}.xlsx"

    written = write_worklist_excel(df, out_path)
    if written is None:
        return 1

    id_list_path = written.with_suffix(".txt")
    id_list_path.write_text("\n".join(df["driver_id"]) + "\n", encoding="utf-8")
    log.info("Also wrote a plain driver-id list -> %s", id_list_path)
    print(f"\n{len(df)} driver(s) in the worklist -> {written}")
    print(f"Rerun them with:\n  python run_pipeline.py --driver-id-file {id_list_path} --skip-flagged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
