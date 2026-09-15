#!/usr/bin/env python3
"""
Commodity driver ingestion pipeline: parallel entry point.

Identical to run_pipeline.py in every respect except how the fetch loop is
driven: driver fetches run concurrently across a thread pool instead of one
at a time. run_pipeline.py itself is untouched and remains the proven,
strictly-sequential fallback — this is a separate script, not a replacement.

Safe to run concurrently by construction, not by convention:
  - CascadeOrchestrator's only shared mutable state (hitl_tasks,
    tier_suggestions) is plain list.append(), safe under the GIL.
  - HttpClient's per-host politeness delay and per-URL download cache both
    got a lock so two threads can never race the same host or the same
    cached file (see pipeline/http_client.py).
  - OutputWriter.add() is lock-protected and returns a running completed
    count, so the periodic checkpoint fires off real completions rather
    than queue position, which stops meaning anything once completion
    order and submission order diverge under concurrency.

Examples
--------
Run the F5 batch with the default worker count:
    python run_pipeline_parallel.py

Tune concurrency for this run only:
    python run_pipeline_parallel.py --max-workers 12

Everything else (--dry-run, --commodity, --registry, etc.) works exactly
as it does in run_pipeline.py.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.alternate_sources import (                  # noqa: E402
    commit_alternate_sources, load_rejected_sources, save_rejected_sources,
)
from pipeline.cascade import CascadeOrchestrator          # noqa: E402
from pipeline.config import Config                        # noqa: E402
from pipeline.connectors import CONNECTORS                # noqa: E402
from pipeline.http_client import HttpClient               # noqa: E402
from pipeline.llm import LlmHelper                        # noqa: E402
from pipeline.models import DriverSpec, FetchResult, Outcome  # noqa: E402
from pipeline.registry import load_registry, primary_drivers  # noqa: E402
from pipeline.registry_enrichment import enrich_new_drivers  # noqa: E402
from pipeline.registry_validator import log_registry_warnings  # noqa: E402
from pipeline.storage import OutputWriter                 # noqa: E402
from pipeline.success_flags import (                      # noqa: E402
    apply_skip_flagged, flagged_driver_ids, load_flags,
)

import pandas as pd  # noqa: E402

log = logging.getLogger("run_pipeline_parallel")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cascading commodity driver data collection pipeline (parallel fetch)",
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
    parser.add_argument("--max-workers", type=int, default=None,
                        help="Override parallel.max_workers from config.yaml for this run")
    parser.add_argument("--log-level", default=None,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def _run_driver_safe(orchestrator: CascadeOrchestrator, spec: DriverSpec,
                      all_specs: dict[str, DriverSpec]) -> tuple[DriverSpec, FetchResult, pd.DataFrame, float]:
    """
    Runs inside a worker thread. Mirrors run_pipeline.py's inline try/except
    exactly: one driver's unhandled crash must not take down the pool or
    the run, so it's caught here and turned into the same fallback
    FetchResult run_pipeline.py would have produced.
    """
    started = time.time()
    try:
        result, monthly = orchestrator.run_driver(spec, all_specs)
    except Exception as exc:  # noqa: BLE001 - one driver must not kill the run
        log.exception("Unhandled error on %s", spec.driver_id)
        result = FetchResult(driver_id=spec.driver_id, outcome=Outcome.FAILED_PERMANENT,
                             tier_attempted=spec.declared_tier,
                             message=f"Unhandled {type(exc).__name__}: {exc}")
        monthly = pd.DataFrame()
    return spec, result, monthly, time.time() - started


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc)

    # ---- configuration -----------------------------------------------------
    cfg = Config.load(args.config)
    if args.log_level:
        cfg._data.setdefault("logging", {})["level"] = args.log_level  # noqa: SLF001
    if args.no_llm:
        cfg._data.setdefault("llm", {})["enabled"] = False             # noqa: SLF001

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

    max_workers = args.max_workers if args.max_workers is not None else int(cfg.get("parallel.max_workers", 8))
    if max_workers < 1:
        log.warning("parallel.max_workers was %d; using 1 (fully sequential)", max_workers)
        max_workers = 1

    log.info("Commodity driver pipeline starting (parallel, max_workers=%d)", max_workers)
    log.info("Registry: %s", registry_path)
    log.info("Window:   %s to %s (minimum %s years of history)",
             cfg.get("run.start_date"), cfg.get("run.end_date") or "today",
             cfg.get("run.min_history_years"))

    # ---- enrichment (optional, before registry load) -----------------------
    # Left sequential on purpose: 53 rows of LLM calls is a few minutes, a
    # much smaller share of total runtime than the fetch phase, and keeping
    # it sequential avoids adding a second, independent concurrency surface
    # to reason about in the first version of the parallel runner.
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

    # ---- registry ------------------------------------------------------------
    try:
        specs = load_registry(
            registry_path,
            commodities=args.commodity,
            regions=args.region,
            driver_ids=args.driver_id,
        )
    except (FileNotFoundError, ValueError) as exc:
        log.error("Registry could not be loaded: %s", exc)
        return 2

    all_specs = {s.driver_id: s for s in specs}
    queue = primary_drivers(specs)
    log.info("%d primary drivers queued (%d rows including fallbacks)", len(queue), len(specs))

    log_registry_warnings(registry_path)

    # Read-only at this point: only used to annotate the dry-run plan and to
    # decide the queue below. The actual skip/reuse work (which touches
    # outputs) happens after the dry-run early-return.
    flags_df = None
    if args.skip_flagged:
        flag_path = Path(args.flag_file) if args.flag_file else cfg.path("paths.success_flag_file")
        flags_df = load_flags(flag_path)
    flagged_ids = flagged_driver_ids(flags_df) if flags_df is not None else set()

    # ---- dry run -------------------------------------------------------------
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

    # ---- execution (parallel) -------------------------------------------------
    http = HttpClient(cfg)
    rejected_sources_path = cfg.path("paths.rejected_alternate_sources")
    rejected_sources = load_rejected_sources(rejected_sources_path)
    orchestrator = CascadeOrchestrator(cfg, http, CONNECTORS, llm, rejected_sources=rejected_sources)
    writer = OutputWriter(cfg)

    # total tracks every driver recorded this run (reused + live-fetched) so
    # the "[completed/total]" progress log stays meaningful even though
    # writer.add()'s running count includes drivers reused below, before the
    # live fetch loop starts.
    total = len(queue)
    if flags_df is not None:
        queue = apply_skip_flagged(queue, flags_df, orchestrator, writer, cfg.path("paths.interim"))

    checkpoint_every = int(cfg.get("output.checkpoint_every_n_drivers", 20))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_run_driver_safe, orchestrator, spec, all_specs): spec
            for spec in queue
        }
        for future in as_completed(futures):
            spec, result, monthly, elapsed = future.result()
            completed = writer.add(spec, result, monthly, elapsed)
            log.info("[%d/%d] %s -> %s", completed, total, spec.driver_id, result.outcome)

            if checkpoint_every > 0 and completed % checkpoint_every == 0:
                writer.write_checkpoint(completed, total)

    # ---- outputs ---------------------------------------------------------------
    commit_alternate_sources(cfg, registry_path, orchestrator.alternate_source_hits)
    save_rejected_sources(rejected_sources_path, rejected_sources,
                          orchestrator.rejected_alternate_source_attempts)
    writer.finalize_bot_block_flags(http.bot_blocked_hosts)
    writer.write_panel()
    writer.write_hitl(orchestrator.hitl_tasks)
    writer.write_manifest(orchestrator.hitl_tasks, orchestrator.tier_suggestions, started_at)
    writer.write_excel_report(orchestrator.hitl_tasks, started_at)
    writer.write_success_flags()
    writer.print_summary()

    if orchestrator.hitl_tasks:
        print(f"\n{len(orchestrator.hitl_tasks)} driver(s) need human action. "
              f"See {cfg.path('paths.hitl_tasks')}")

    return 0 if writer.rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
