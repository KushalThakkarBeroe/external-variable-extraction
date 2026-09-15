#!/usr/bin/env python3
"""
Standalone: build the success-flag Excel file from an existing
run_manifest.json, without re-running the pipeline.

Useful right after a run finishes (or for an already-completed run), to
produce output/success_flags.xlsx before enabling --skip-flagged on the
next run_pipeline.py / run_pipeline_parallel.py invocation. Every future
run also writes this file automatically at the end (see
OutputWriter.write_success_flags in pipeline/storage.py) — this script
exists for generating it on demand from a manifest that already exists.

Examples
--------
    python generate_success_flags.py
    python generate_success_flags.py --manifest output/run_manifest.json --threshold 0.5
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.config import Config                                    # noqa: E402
from pipeline.success_flags import build_flags_df, write_flags_excel  # noqa: E402

log = logging.getLogger("generate_success_flags")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the success-flag Excel file from an existing run manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--manifest", default=None,
                        help="Override paths.manifest from config.yaml")
    parser.add_argument("--out", default=None,
                        help="Override paths.success_flag_file from config.yaml")
    parser.add_argument("--threshold", type=float, default=None,
                        help="Override output.success_flag_threshold from config.yaml "
                             "(fraction 0-1, e.g. 0.4 for 40%%)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s | %(levelname)-8s | %(message)s")

    cfg = Config.load(args.config)
    manifest_path = Path(args.manifest) if args.manifest else cfg.path("paths.manifest", "output/run_manifest.json")
    out_path = Path(args.out) if args.out else cfg.path("paths.success_flag_file", "output/success_flags.xlsx")
    threshold = args.threshold if args.threshold is not None else float(cfg.get("output.success_flag_threshold", 0.4))

    if not manifest_path.exists():
        log.error("Manifest not found at %s", manifest_path)
        return 1

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = manifest.get("drivers", [])
    if not entries:
        log.warning("Manifest at %s has no driver entries; nothing to flag", manifest_path)
        return 1

    df = build_flags_df(entries, threshold)
    written = write_flags_excel(df, out_path, threshold)
    if written is None:
        return 1

    flagged = int(df["meets_threshold"].sum())
    log.info("Flag file written: %d of %d drivers meet the %.0f%% threshold -> %s",
             flagged, len(df), threshold * 100, written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
