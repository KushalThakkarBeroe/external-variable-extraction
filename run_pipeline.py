#!/usr/bin/env python3
"""
Commodity driver ingestion pipeline: entry point.

Examples
--------
Validate the registry and show the plan without touching the network:
    python run_pipeline.py --dry-run

Run the two sample commodities:
    python run_pipeline.py

One commodity and region only:
    python run_pipeline.py --commodity Poultry --region Australia

A single driver, with verbose logging, while debugging a connector:
    python run_pipeline.py --driver-id LA_PLP_FX --log-level DEBUG

Scale to the full list by pointing at a bigger registry:
    python run_pipeline.py --registry input/driver_registry_full.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.alternate_sources import (                  # noqa: E402
    commit_alternate_sources, load_rejected_sources, save_rejected_sources,
)
from pipeline.cascade import CascadeOrchestrator          # noqa: E402
from pipeline.config import Config                        # noqa: E402
from pipeline.connectors import CONNECTORS                # noqa: E402
from pipeline.eurostat_repair import (                    # noqa: E402
    commit_eurostat_locator_repairs, load_eurostat_repair_state, save_eurostat_repair_state,
)
from pipeline.fallback_audit import record_fallback_audit  # noqa: E402
from pipeline.http_client import HttpClient               # noqa: E402
from pipeline.llm import LlmHelper                        # noqa: E402
from pipeline.registry import load_registry, primary_drivers, resolve_driver_ids  # noqa: E402
from pipeline.registry_enrichment import enrich_new_drivers  # noqa: E402
from pipeline.registry_validator import log_registry_warnings  # noqa: E402
from pipeline.storage import OutputWriter                 # noqa: E402
from pipeline.success_flags import (                      # noqa: E402
    apply_skip_flagged, flagged_driver_ids, load_flags,
)

log = logging.getLogger("run_pipeline")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cascading commodity driver data collection pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--registry", default=None,
                        help="Override the registry path set in config.yaml")
    parser.add_argument("--commodity", action="append",
                        help="Filter by commodity; repeatable")
    parser.add_argument("--region", action="append",
                        help="Filter by region; repeatable")
    parser.add_argument("--driver-id", action="append",
                        help="Run specific driver ids; repeatable")
    parser.add_argument("--driver-id-file", default=None,
                        help="Path to a text file with one driver_id per line (blank lines and "
                             "'#' comments ignored); merged with --driver-id. For a worklist too "
                             "large for the command line, e.g. output/rerun_worklist_*.xlsx's "
                             "driver_id column")
    parser.add_argument("--resume-deferred", action="store_true",
                        help="Also include drivers deferred by a prior run for a source's rate "
                             "budget (see http.source_limits / paths.rate_limit_state)")
    parser.add_argument("--start-date",
                        help="Override run.start_date, e.g. 2024-07-01")
    parser.add_argument("--end-date",
                        help="Override run.end_date, e.g. 2026-06-30")
    parser.add_argument("--min-history-years", type=float,
                        help="Override the minimum history quality gate. Lower this when "
                             "deliberately pulling a short window, or every series fails the gate")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate the registry and print the plan without fetching")
    parser.add_argument("--no-llm", action="store_true",
                        help="Disable LLM assistance for this run")
    parser.add_argument("--skip-flagged", action="store_true",
                        help="Skip drivers already marked sufficiently complete in the "
                             "success-flag file (see paths.success_flag_file), reusing their "
                             "cached data instead of re-fetching")
    parser.add_argument("--flag-file", default=None,
                        help="Override paths.success_flag_file from config.yaml")
    parser.add_argument("--log-level", default=None,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc)

    # ---- configuration -----------------------------------------------------
    cfg = Config.load(args.config)
    if args.log_level:
        cfg._data.setdefault("logging", {})["level"] = args.log_level  # noqa: SLF001
    if args.no_llm:
        cfg._data.setdefault("llm", {})["enabled"] = False             # noqa: SLF001

    # Window overrides. The history gate must move with the window: pulling a
    # deliberate 2-year slice against an 8-year gate marks every series partial.
    run_cfg = cfg._data.setdefault("run", {})                          # noqa: SLF001
    if args.start_date:
        run_cfg["start_date"] = args.start_date
    if args.end_date:
        run_cfg["end_date"] = args.end_date
    if args.min_history_years is not None:
        run_cfg["min_history_years"] = args.min_history_years
    cfg.setup_logging()
    cfg.ensure_dirs()

    registry_path = Path(args.registry) if args.registry else cfg.path("paths.registry")
    if not registry_path.is_absolute():
        registry_path = cfg.root / registry_path

    log.info("Commodity driver pipeline starting")
    log.info("Registry: %s", registry_path)
    log.info("Window:   %s to %s (minimum %s years of history)",
             cfg.get("run.start_date"), cfg.get("run.end_date") or "today",
             cfg.get("run.min_history_years"))

    # ---- enrichment (optional, before registry load) ----------------------
    llm = LlmHelper(cfg)
    if cfg.get("registry.auto_enrich_new_drivers", True) and llm.enabled:
        seed_path = Path(cfg.get("registry.new_drivers_seed", "input/new_drivers.xlsx"))
        if not seed_path.is_absolute():
            seed_path = cfg.root / seed_path
        rows_added = enrich_new_drivers(cfg, llm, registry_path, seed_path)
        if rows_added > 0:
            log.info("Enriched %d new drivers from seed file", rows_added)
    elif cfg.get("registry.auto_enrich_new_drivers", True) and not llm.enabled:
        log.warning("Registry enrichment configured but LLM is disabled; "
                   "new drivers from seed file will not be enriched this run")

    # ---- registry ----------------------------------------------------------
    driver_ids = resolve_driver_ids(args.driver_id, args.driver_id_file, args.resume_deferred, cfg)
    try:
        specs = load_registry(
            registry_path,
            commodities=args.commodity,
            regions=args.region,
            driver_ids=driver_ids,
        )
    except (FileNotFoundError, ValueError) as exc:
        log.error("Registry could not be loaded: %s", exc)
        return 2

    all_specs = {s.driver_id: s for s in specs}
    queue = primary_drivers(specs)
    log.info("%d primary drivers queued (%d rows including fallbacks)", len(queue), len(specs))

    # Pre-flight: flag any row with an incomplete locator before spending run
    # time on it — the connector would fail on the very first attempt anyway.
    log_registry_warnings(registry_path)

    # Read-only at this point: only used to annotate the dry-run plan and to
    # decide the queue below. The actual skip/reuse work (which touches
    # outputs) happens after the dry-run early-return.
    flags_df = None
    if args.skip_flagged:
        flag_path = Path(args.flag_file) if args.flag_file else cfg.path("paths.success_flag_file")
        flags_df = load_flags(flag_path)
    flagged_ids = flagged_driver_ids(flags_df) if flags_df is not None else set()

    # ---- dry run -----------------------------------------------------------
    if args.dry_run:
        print("\nPLANNED EXTRACTIONS")
        print("-" * 108)
        print(f"{'driver_id':<22} {'commodity':<10} {'region':<16} {'tier':<6}"
              f"{'freq':<9} {'rollup':<8} {'connector':<24} {'hitl'}")
        print("-" * 108)
        for spec in queue:
            skip_marker = "SKIP (flagged)" if spec.driver_id in flagged_ids else (
                'yes' if spec.human_in_loop else '')
            print(f"{spec.driver_id:<22} {spec.commodity:<10} {spec.region:<16} "
                  f"{int(spec.declared_tier):<6}{spec.native_frequency:<9} "
                  f"{spec.rollup_method:<8} {spec.connector:<24} "
                  f"{skip_marker}")
        print("-" * 108)
        if flagged_ids:
            print(f"\n{len(flagged_ids & {s.driver_id for s in queue})} of {len(queue)} would be "
                  f"skipped (reused from cache) under --skip-flagged.")
        missing = [s.driver_id for s in specs
                   if s.credential_keys and not cfg.has_credentials(s.credential_keys)]
        if missing:
            print(f"\nDrivers with unresolved credentials ({len(missing)}): {', '.join(missing)}")
            print("These will cascade to a fallback source or become human-in-the-loop tasks.")
        return 0

    # ---- execution ---------------------------------------------------------
    http = HttpClient(cfg)
    rejected_sources_path = cfg.path("paths.rejected_alternate_sources")
    rejected_sources = load_rejected_sources(rejected_sources_path)
    rejected_eurostat_path = cfg.path("paths.rejected_eurostat_locator_repairs")
    rejected_eurostat_repairs = load_eurostat_repair_state(rejected_eurostat_path)
    orchestrator = CascadeOrchestrator(cfg, http, CONNECTORS, llm,
                                       rejected_sources=rejected_sources,
                                       rejected_eurostat_repairs=rejected_eurostat_repairs)
    writer = OutputWriter(cfg)

    if flags_df is not None:
        queue = apply_skip_flagged(queue, flags_df, orchestrator, writer, cfg.path("paths.interim"))

    checkpoint_every = int(cfg.get("output.checkpoint_every_n_drivers", 20))

    for index, spec in enumerate(queue, start=1):
        log.info("[%d/%d] %s", index, len(queue), spec.driver_id)
        started = time.time()
        try:
            result, monthly = orchestrator.run_driver(spec, all_specs)
        except Exception as exc:  # noqa: BLE001 - one driver must not kill the run
            log.exception("Unhandled error on %s", spec.driver_id)
            from pipeline.models import FetchResult, Outcome
            import pandas as pd
            result = FetchResult(driver_id=spec.driver_id, outcome=Outcome.FAILED_PERMANENT,
                                 tier_attempted=spec.declared_tier,
                                 message=f"Unhandled {type(exc).__name__}: {exc}")
            monthly = pd.DataFrame()
        writer.add(spec, result, monthly, time.time() - started)

        if checkpoint_every > 0 and index % checkpoint_every == 0:
            writer.write_checkpoint(index, len(queue))

    # ---- outputs -----------------------------------------------------------
    commit_alternate_sources(cfg, registry_path, orchestrator.alternate_source_hits)
    save_rejected_sources(rejected_sources_path, rejected_sources,
                          orchestrator.rejected_alternate_source_attempts)
    commit_eurostat_locator_repairs(cfg, registry_path, orchestrator.eurostat_locator_repair_hits)
    save_eurostat_repair_state(rejected_eurostat_path, rejected_eurostat_repairs,
                               orchestrator.rejected_eurostat_repair_attempts)
    # Fallback-audit reads this run's own entries only (what was actually
    # exercised THIS run) -- must happen before merging carries forward
    # driver_ids this run never touched.
    fallback_target_ids = {s.fallback_driver_id for s in all_specs.values() if s.fallback_driver_id}
    record_fallback_audit(writer.manifest_entries, fallback_target_ids,
                          cfg.path("paths.fallback_audit_log", "output/fallback_audit_log.csv"))

    # A scoped run (--driver-id / --driver-id-file) only ever touches a
    # subset of the registry -- merge in every driver_id the existing
    # manifest already knows about but this run didn't touch, so the
    # manifest/success-flags/report below reflect the full picture instead
    # of silently discarding everything outside this run's scope.
    writer.merge_with_existing_manifest()

    writer.finalize_bot_block_flags(http.bot_blocked_hosts, http.attempted_hosts_by_driver)
    writer.write_panel()
    writer.write_hitl(orchestrator.hitl_tasks)
    writer.archive_raw_files(started_at)
    writer.write_manifest(orchestrator.hitl_tasks, orchestrator.tier_suggestions, started_at)
    writer.write_excel_report(orchestrator.hitl_tasks, started_at, http.rate_limiter.deferred_entries(),
                               http.rate_limiter.waited_entries())
    writer.write_success_flags()
    http.rate_limiter.save_state()
    writer.print_summary()

    if orchestrator.hitl_tasks:
        print(f"\n{len(orchestrator.hitl_tasks)} driver(s) need human action. "
              f"See {cfg.path('paths.hitl_tasks')}")

    # Exit 0 when anything was collected, 1 when the run produced nothing at all.
    return 0 if writer.rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
