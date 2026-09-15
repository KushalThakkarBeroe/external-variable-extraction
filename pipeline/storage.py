"""
Output layer.

Four artifacts per run:

  monthly_panel        Long format, one row per driver-month. This is what the
                       forecasting models consume. Every row carries its own
                       provenance so a modeller can filter on quality without
                       going back to the pipeline.

  monthly_wide         Commodity x region matrix, months down and drivers
                       across. Convenient for correlation screens and for the
                       explanatory variable selection framework.

  run_manifest.json    Per-driver outcome: which tier won, how many rows, what
                       quality notes fired, how long it took. This is the
                       operational view when a run of 300 drivers half-works.

  human_in_the_loop_tasks.yaml
                       The exact list of things a person needs to do, with the
                       config key or drop path that resolves each one. Written
                       in YAML so it can be pasted straight back into config.
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import yaml

from .atomic_io import write_atomic
from .models import DriverSpec, FetchResult, utc_now
from .rollup import history_years
from .success_flags import build_flags_df, write_flags_excel

log = logging.getLogger(__name__)


def _format_retry_after(seconds: Any) -> str:
    """Human-readable estimate for the Deferred sheet, e.g. '2h 15m'."""
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return ""
    if total <= 0:
        return "now"
    hours, remainder = divmod(total, 3600)
    minutes = remainder // 60
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m" if minutes else f"{total}s"


class OutputWriter:
    def __init__(self, cfg):
        self.cfg = cfg
        self.curated = cfg.path("paths.curated", "output/monthly")
        self.interim = cfg.path("paths.interim", "output/interim")
        self.curated.mkdir(parents=True, exist_ok=True)
        self.interim.mkdir(parents=True, exist_ok=True)
        self.format = str(cfg.get("output.format", "parquet")).lower()
        self.rows: list[pd.DataFrame] = []
        self.manifest_entries: list[dict[str, Any]] = []
        # Guards rows/manifest_entries and the completion counter below.
        # Only contended when several drivers finish at once (the parallel
        # runner); a single caller never contends on it.
        self._lock = threading.Lock()
        self._completed_count = 0

    # --------------------------------------------------------------- collect

    def add(self, spec: DriverSpec, result: FetchResult, monthly: pd.DataFrame,
            elapsed_seconds: float) -> int:
        """
        Attach provenance to a driver's monthly frame and stage it.

        Returns the running count of drivers recorded so far, so a caller
        can trigger a checkpoint off real completions rather than queue
        position — the two only match up when driver order and completion
        order are the same, which parallel execution doesn't guarantee.
        """
        with self._lock:
            return self._add_locked(spec, result, monthly, elapsed_seconds)

    def _add_locked(self, spec: DriverSpec, result: FetchResult, monthly: pd.DataFrame,
                     elapsed_seconds: float) -> int:
        if not monthly.empty:
            enriched = monthly.copy()
            enriched.insert(0, "driver_id", spec.driver_id)
            enriched.insert(1, "commodity", spec.commodity)
            enriched.insert(2, "region", spec.region)
            enriched.insert(3, "driver_name", spec.driver_name)
            enriched["unit"] = result.unit or spec.unit
            enriched["rollup_method"] = spec.rollup_method
            enriched["native_frequency"] = spec.native_frequency

            # Provenance follows the source that actually served the data,
            # which may be a fallback, while driver_id stays the primary slot.
            served = result.extra.get("serving_spec", spec)

            if self.cfg.get("output.include_quality_columns", True):
                enriched["tier_declared"] = int(spec.declared_tier)
                enriched["tier_used"] = int(result.tier_attempted)
                enriched["frequency_detected"] = result.extra.get("frequency_detected", spec.native_frequency)
                enriched["served_by_driver_id"] = served.driver_id
                enriched["source_name"] = served.source_name
                enriched["source_url"] = result.source_url_used
                enriched["is_proxy"] = served.is_proxy or served.driver_id != spec.driver_id
                enriched["is_substituted"] = served.driver_id != spec.driver_id
                enriched["data_quality_risk"] = spec.data_quality_risk
                enriched["transform_hint"] = spec.transform_hint
                enriched["retrieved_at"] = result.retrieved_at.isoformat(timespec="seconds")

            self.rows.append(enriched)

            # Raw pre-rollup observations, kept for reconciliation.
            result.observations.assign(driver_id=spec.driver_id).to_csv(
                self.interim / f"{spec.driver_id}_observations.csv", index=False
            )

        observed = monthly["value"].notna().sum() if not monthly.empty else 0
        first = monthly.loc[monthly["value"].notna(), "month"].min() if observed else None
        last = monthly.loc[monthly["value"].notna(), "month"].max() if observed else None

        self.manifest_entries.append({
            "driver_id": spec.driver_id,
            "commodity": spec.commodity,
            "region": spec.region,
            "driver": spec.driver_name,
            "outcome": result.outcome,
            "tier_declared": int(spec.declared_tier),
            "tier_used": int(result.tier_attempted),
            "frequency_declared": spec.native_frequency,
            "frequency_detected": result.extra.get("frequency_detected", spec.native_frequency),
            "connector": spec.connector,
            "source": spec.source_name,
            "source_url": result.source_url_used,
            "raw_observations": int(len(result.observations)),
            "monthly_observations": int(observed),
            "coverage_ratio": round(float(result.extra.get("coverage_ratio", 0.0)), 4),
            "history_years": history_years(monthly) if not monthly.empty else 0.0,
            "first_month": first.strftime("%Y-%m") if first is not None and pd.notna(first) else None,
            "last_month": last.strftime("%Y-%m") if last is not None and pd.notna(last) else None,
            # Mirrors cascade.py::_quality_gate()'s own override logic exactly: a
            # populated spec.min_history_years always wins over the global default,
            # so this manifest field agrees with what the gate actually decided
            # instead of silently reverting to the global figure for rows that
            # have a per-driver override (see Workstream 4).
            "meets_min_history": (history_years(monthly) >= (
                float(spec.min_history_years) if spec.min_history_years is not None
                else float(self.cfg.get("run.min_history_years", 8))
            ) if not monthly.empty else False),
            "is_proxy": spec.is_proxy,
            "served_by_driver_id": result.extra.get("served_by_driver_id", spec.driver_id),
            "served_by_source": result.extra.get("serving_spec", spec).source_name,
            "reclassified": result.extra.get("reclassified", False),
            "alternate_source_used": result.extra.get("alternate_source_used", False),
            "eurostat_locator_repaired": result.extra.get("eurostat_locator_repaired", False),
            "manual_drop": result.extra.get("source") == "manual_drop",
            "raw_artifact_path": result.raw_artifact_path,
            "quality_notes": result.extra.get("quality_notes", []),
            "retrieved_at": result.retrieved_at.isoformat(timespec="seconds"),
            "message": result.message,
            "elapsed_seconds": round(elapsed_seconds, 2),
        })

        self._completed_count += 1
        return self._completed_count

    # ----------------------------------------------------------------- write

    def write_panel(self) -> Path | None:
        if not self.rows:
            log.warning("No data collected; skipping panel write")
            return None

        panel = pd.concat(self.rows, ignore_index=True).sort_values(
            ["commodity", "region", "driver_id", "month"]
        )
        base = self.curated / "monthly_panel"

        written = None
        if self.format in ("parquet", "both"):
            # Parquet is preferred for the panel (typed columns, cheap to read
            # in the modelling step) but the run must not fail when no parquet
            # engine is installed. Fall back to CSV and say so.
            try:
                written = base.with_suffix(".parquet")
                panel.to_parquet(written, index=False)
            except ImportError:
                log.warning("No parquet engine available (pip install pyarrow); writing CSV instead")
                written = None
                self.format = "csv" if self.format == "parquet" else "both"
        if self.format in ("csv", "both"):
            csv_path = base.with_suffix(".csv")
            panel.to_csv(csv_path, index=False)
            written = written or csv_path

        log.info("Wrote monthly panel: %d rows, %d drivers -> %s",
                 len(panel), panel["driver_id"].nunique(), written)

        if self.cfg.get("output.write_wide_panel", True):
            self._write_wide(panel)
        return written

    def write_checkpoint(self, drivers_done: int, drivers_total: int) -> Path | None:
        """
        Periodic checkpoint, called every N drivers (registry rows) processed
        during a run (N is config-driven, not hardcoded — see
        output.checkpoint_every_n_drivers).

        Flushes tracker .xlsx files for whatever has been collected so far
        (the same upsert logic write_panel uses at the end) and appends a
        block to a running plain-text log recording exactly how far the run
        got. If the process is interrupted, the trackers already reflect
        everything completed up to the last checkpoint, and the log shows
        precisely where to resume from — instead of losing all progress,
        which is what happens today since every output is otherwise written
        only once, at the very end of the full run.

        Takes a locked snapshot of rows/manifest_entries first (this is the
        one write method called while other drivers may still be mid-`add()`
        in another thread under the parallel runner) and does the actual
        concat/file-writing work outside the lock, so a slow checkpoint
        write never blocks other threads from recording their results.
        """
        with self._lock:
            rows_snapshot = list(self.rows)
            manifest_snapshot = list(self.manifest_entries)

        if rows_snapshot:
            panel = pd.concat(rows_snapshot, ignore_index=True).sort_values(
                ["commodity", "region", "driver_id", "month"]
            )
            self._write_wide(panel)

        log_path = self.cfg.path("paths.checkpoint_log", "output/checkpoint_log.txt")
        log_path.parent.mkdir(parents=True, exist_ok=True)

        summary = self._summarise(manifest_snapshot)
        last_driver_id = manifest_snapshot[-1]["driver_id"] if manifest_snapshot else "(none)"

        lines = [
            "=" * 72,
            f"CHECKPOINT @ {utc_now().isoformat(timespec='seconds')}",
            f"Drivers processed: {drivers_done} / {drivers_total}",
            f"Last driver processed: {last_driver_id}",
        ]
        for key, value in summary.items():
            lines.append(f"  {key:32s} {value}")
        lines.append("")

        addition = "\n".join(lines) + "\n"
        existing_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
        write_atomic(log_path, lambda tmp: tmp.write_text(existing_text + addition, encoding="utf-8"))

        log.info("Checkpoint written: %d/%d drivers -> %s",
                 drivers_done, drivers_total, log_path)
        return log_path

    def _write_wide(self, panel: pd.DataFrame) -> None:
        """Compute wide matrices per commodity x region; write CSV and/or tracker independently."""
        for (commodity, region), group in panel.groupby(["commodity", "region"]):
            wide = group.pivot_table(index="month", columns="driver_id",
                                     values="value", aggfunc="first").sort_index()
            slug = f"{commodity}_{region}".lower().replace(" ", "_")

            if self.cfg.get("output.write_wide_panel", False):
                wide.to_csv(self.curated / f"wide_{slug}.csv")
                log.info("Wrote wide matrix CSV for %s / %s: %d months x %d drivers",
                         commodity, region, len(wide), wide.shape[1])

            if self.cfg.get("output.write_tracker_xlsx", True):
                self._upsert_tracker(slug, commodity, region, wide)

    def _upsert_tracker(self, slug: str, commodity: str, region: str, wide: pd.DataFrame) -> None:
        """
        Maintain one persistent workbook per commodity x region that accumulates
        across runs, rather than being overwritten wholesale like the wide CSV.

        First run for a slug: bootstrap the file with full history.

        Every run after that: this run's values win for any month it covers
        (so a source revising last month's print, or a driver that only just
        cleared the quality gate, is picked up); months the tracker already
        holds that fall outside this run's window are left untouched, so the
        file keeps growing rather than being clipped to whatever window a
        given run happened to request.
        """
        path = self.curated / f"tracker_{slug}.xlsx"
        new = wide.sort_index()

        if not path.exists():
            write_atomic(path, lambda tmp: new.to_excel(tmp, sheet_name="Monthly"))
            log.info("Created tracker for %s / %s: %d month(s) (bootstrap) -> %s",
                     commodity, region, len(new), path.name)
            return

        existing = pd.read_excel(path, sheet_name="Monthly", index_col=0, parse_dates=True)

        added = new.index.difference(existing.index)
        common = new.index.intersection(existing.index)
        revised = [m for m in common
                  if not new.loc[m].equals(existing.loc[m].reindex(new.columns))]

        # New values take priority wherever this run has data; existing rows
        # outside the current window (or existing columns this run does not
        # touch) are carried forward unchanged.
        merged = new.combine_first(existing).sort_index()
        write_atomic(path, lambda tmp: merged.to_excel(tmp, sheet_name="Monthly"))
        log.info("Updated tracker for %s / %s: %d new month(s), %d revised, %d unchanged -> %s",
                 commodity, region, len(added), len(revised),
                 len(common) - len(revised), path.name)

    def archive_raw_files(self, started_at: datetime) -> int:
        """
        Permanent per-run copy of the raw file behind every successful driver
        that resolved through a file-based extraction (see raw_artifact_path,
        stamped by tiers/base.py::_guard() off a DataFrame's .attrs).

        HttpClient's on-disk cache (output/raw/<hash>.<ext>) is keyed purely
        by URL and silently overwritten once its TTL expires or the URL is
        re-fetched again — good for avoiding redundant downloads, but it
        means the exact bytes behind a number computed weeks ago are not
        guaranteed to still be on disk later. This copies them out to a
        location no later run's cache eviction can touch, at the point
        they're known to have produced a real result.

        Best-effort: a source file already gone by the time this runs (e.g.
        its working cache slot was reused by a concurrent driver under the
        parallel runner) is logged and skipped rather than failing the run.
        """
        timestamp = started_at.strftime("%Y%m%d_%H%M%S")
        dest_dir = self.cfg.path("paths.raw_archive", "output/raw_archive") / timestamp
        archived = 0
        for entry in self.manifest_entries:
            if entry.get("outcome") != "success":
                continue
            raw_path = entry.get("raw_artifact_path")
            if not raw_path:
                continue
            source = Path(raw_path)
            if not source.exists():
                log.debug("Raw artifact for %s no longer on disk at %s; skipping archive copy",
                         entry["driver_id"], source)
                continue
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / f"{entry['driver_id']}{source.suffix}"
            try:
                shutil.copy2(source, dest)
                archived += 1
            except OSError as exc:
                log.warning("Could not archive raw file for %s (%s): %s", entry["driver_id"], source, exc)
        if archived:
            log.info("Archived %d raw source file(s) -> %s", archived, dest_dir)
        return archived

    def merge_with_existing_manifest(self) -> int:
        """
        Carries forward every driver_id from the existing run_manifest.json
        that this run did NOT touch, so a scoped run (--driver-id /
        --driver-id-file) produces a manifest, success-flag file and report
        reflecting the full registry picture instead of silently discarding
        every driver outside its scope.

        This run's own entries always win for any driver_id they cover — a
        fresh result supersedes a stale one, never the other way round.
        Call once, right after the fetch loop completes and before any
        write_* method — those all read self.manifest_entries, so mutating
        it here is what makes every downstream output consistent without
        each one needing its own merge logic.

        A driver_id no longer present in the current registry (e.g. a row
        was deleted) is still carried forward as a stale record rather than
        silently dropped — a full, unscoped run naturally supersedes it the
        next time that driver_id IS attempted; this only matters for a
        driver permanently removed from the registry, which a scoped rerun
        workflow does not do.

        Returns the number of carried-forward entries added.
        """
        path = self.cfg.path("paths.manifest", "output/run_manifest.json")
        if not path.exists():
            return 0

        this_run_ids = {e["driver_id"] for e in self.manifest_entries}
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log.warning("Could not read existing manifest at %s to merge (%s); this run's "
                       "outputs will only reflect drivers processed this run", path, exc)
            return 0

        carried_forward = [entry for entry in existing.get("drivers", [])
                           if entry.get("driver_id") not in this_run_ids]
        if carried_forward:
            log.info("Merging %d driver(s) carried forward from the existing manifest "
                    "(not touched this run) with %d driver(s) from this run",
                    len(carried_forward), len(self.manifest_entries))
            self.manifest_entries = carried_forward + self.manifest_entries
        return len(carried_forward)

    def write_manifest(self, hitl_tasks: list, tier_suggestions: list,
                       started_at: datetime) -> Path:
        manifest = {
            "run_started_at": started_at.isoformat(timespec="seconds"),
            "run_finished_at": utc_now().isoformat(timespec="seconds"),
            "window": {
                "start": self.cfg.get("run.start_date"),
                "end": self.cfg.get("run.end_date") or "today",
                "min_history_years": self.cfg.get("run.min_history_years"),
            },
            "summary": self._summarise(),
            "drivers": self.manifest_entries,
            "registry_tier_suggestions": tier_suggestions,
            "human_in_the_loop_count": len(hitl_tasks),
        }
        path = self.cfg.path("paths.manifest", "output/run_manifest.json")
        payload = json.dumps(manifest, indent=2, default=str)
        write_atomic(path, lambda tmp: tmp.write_text(payload, encoding="utf-8"))
        log.info("Wrote run manifest -> %s", path)
        return path

    def write_success_flags(self, threshold: float | None = None) -> Path | None:
        """
        Excel flag file marking which drivers cleared the coverage threshold
        this run, for a future run's --skip-flagged to read (see
        pipeline/success_flags.py). Regenerated fresh from self.manifest_entries
        every time, so a driver reused via --skip-flagged (outcome SKIPPED)
        that still clears the bar keeps propagating forward across runs with
        no special merge logic needed here — call merge_with_existing_manifest()
        beforehand (as both entry-point scripts now do) so a scoped run's
        flags reflect every driver_id, not just the ones it touched.
        """
        if not self.manifest_entries:
            return None
        if threshold is None:
            threshold = float(self.cfg.get("output.success_flag_threshold", 0.4))
        df = build_flags_df(self.manifest_entries, threshold)
        path = self.cfg.path("paths.success_flag_file", "output/success_flags.xlsx")
        return write_flags_excel(df, path, threshold)

    def finalize_bot_block_flags(self, bot_blocked_hosts: set[str] | None = None,
                                  attempted_hosts_by_driver: dict[str, set[str]] | None = None) -> None:
        """
        Tags every manifest entry with whether a host it actually contacted
        was confirmed bot-blocking requests at any point this run (see
        HttpClient.bot_blocked_hosts / attempted_hosts_by_driver in
        pipeline/http_client.py).

        Checks the driver's real attempted hosts (attempted_hosts_by_driver,
        recorded by HttpClient.request() for every call made while resolving
        that driver — including fallback/alternate-source/repair sub-fetches)
        rather than the registry's source_url column. The two are often
        different hosts entirely (e.g. a landing page at comtradeplus.un.org
        vs. the actual API host comtradeapi.un.org a connector calls), which
        previously meant a driver killed by a block was never correctly
        flagged. Falls back to the source_url comparison only when no
        attempted-hosts record exists for a driver (e.g. a reused --skip-
        flagged row that made no live request this run at all).

        Called once, after the fetch loop finishes, before any write_* call —
        the blocked-host set isn't complete until every driver has been
        attempted, so a host a later driver discovers is blocked would be
        missed if this ran earlier (e.g. per-driver, inside add()). Mutates
        manifest_entries in place so write_manifest, write_excel_report and
        _summarise all read the same computed field from one source of truth.
        """
        blocked = bot_blocked_hosts or set()
        by_driver = attempted_hosts_by_driver or {}
        for entry in self.manifest_entries:
            attempted = by_driver.get(entry["driver_id"])
            if attempted:
                entry["likely_bot_blocked"] = bool(attempted & blocked)
            else:
                host = urlparse(entry.get("source_url") or "").netloc
                entry["likely_bot_blocked"] = host in blocked

    def write_hitl(self, tasks: list) -> Path | None:
        if not tasks:
            log.info("No human-in-the-loop tasks this run")
            return None
        path = self.cfg.path("paths.hitl_tasks", "output/human_in_the_loop_tasks.yaml")
        payload = {
            "generated_at": utc_now().isoformat(timespec="seconds"),
            "instructions": (
                "Complete each action, then either add the credential to config.yaml under "
                "'credentials:' or save the series at the drop path with columns obs_date,value. "
                "Re-run the pipeline; resolved tasks disappear automatically."
            ),
            "tasks": [t.to_dict() for t in tasks],
        }
        dumped = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
        write_atomic(path, lambda tmp: tmp.write_text(dumped, encoding="utf-8"))
        log.warning("Wrote %d human-in-the-loop tasks -> %s", len(tasks), path)
        return path

    def _summarise(self, entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        entries = self.manifest_entries if entries is None else entries
        if not entries:
            return {}
        df = pd.DataFrame(entries)
        succeeded = df["outcome"] == "success"
        # A driver reused via --skip-flagged (see pipeline/success_flags.py)
        # is real, already-verified data — it must not be counted as
        # blocked/failed just because it wasn't fetched live this run.
        skipped = df["outcome"] == "skipped"
        # A successful row can still carry quality_notes (short history,
        # staleness, sparse coverage) — real data that just isn't a clean
        # pass. Surfaced as a sub-count alongside "succeeded" rather than as
        # its own outcome, so it reads as a note on a success, not a
        # different, weaker category of result.
        has_notes = df["quality_notes"].apply(bool)
        # Not present yet on a mid-run checkpoint snapshot (finalize_bot_block_flags
        # only runs once, after the fetch loop completes) — default False rather
        # than KeyError so a checkpoint summary doesn't crash.
        if "likely_bot_blocked" not in df.columns:
            df["likely_bot_blocked"] = False
        return {
            "drivers_attempted": int(len(df)),
            "succeeded": int(succeeded.sum()),
            # Scoped to THIS RUN's live successes only — deliberately does
            # not include rows reused via --skip-flagged (outcome=='skipped'
            # can also carry a carried-over quality note). See
            # _success_breakdown()'s "Succeeded, Partial" row for the
            # combined (this-run + reused) count; the two are intentionally
            # different scopes, not a discrepancy.
            "succeeded_with_quality_notes_this_run": int((succeeded & has_notes).sum()),
            "skipped_reused_from_prior_run": int(skipped.sum()),
            "blocked_or_failed": int((~succeeded & ~skipped).sum()),
            "likely_bot_blocked_count": int(df["likely_bot_blocked"].sum()),
            "resolved_by_tier": df.loc[df["monthly_observations"] > 0, "tier_used"]
                                  .value_counts().sort_index().to_dict(),
            # Counts every driver TAGGED is_proxy this run, whether or not it
            # actually succeeded — see served_by_proxy_success below for the
            # successes-only figure. Previously named "served_by_proxy",
            # which read as "successfully served by a proxy" but wasn't.
            "proxy_tagged_attempts": int(df["is_proxy"].sum()),
            "served_by_proxy_success": int((df["is_proxy"] & succeeded).sum()),
            "meeting_min_history": int(df["meets_min_history"].sum()),
            "total_monthly_observations": int(df["monthly_observations"].sum()),
        }

    def _success_breakdown(self, entries: list[dict[str, Any]] | None = None) -> pd.DataFrame:
        """
        Combined pass-rate view: how many drivers have usable data in total —
        this run's live successes plus everything reused via --skip-flagged —
        and, for this run's live successes only, which resolution path
        actually produced each one (direct, registered fallback, plain
        reclassification, alternate-source search, or Eurostat locator
        repair). Deliberately a separate method from _summarise(): that
        plain dict is embedded verbatim in run_manifest.json's "summary" key
        and iterated generically by print_summary(), so it must never change
        shape. This is a purely additive, narrative view built from the same
        already-collected manifest fields.

        Categories are mutually exclusive, checked in an order that matches
        exactly what cascade.py sets on result.extra: alternate-source-search
        also sets reclassified=True (and eurostat locator repair sets it
        too), so both must be excluded before a success is counted under the
        plain "reclassification rescue" bucket, or it would be double-counted.

        Returns a (metric, count, pct_of_total) DataFrame; a section-header
        row carries None in the numeric columns.
        """
        entries = self.manifest_entries if entries is None else entries
        if not entries:
            return pd.DataFrame(columns=["metric", "count", "pct_of_total"])

        df = pd.DataFrame(entries)
        total = len(df)
        succeeded = df["outcome"] == "success"
        reused = df["outcome"] == "skipped"
        failed = ~succeeded & ~reused
        combined = succeeded | reused
        has_notes = df["quality_notes"].apply(bool)

        is_alt = succeeded & df["alternate_source_used"].astype(bool)
        is_eurostat_repair = succeeded & df["eurostat_locator_repaired"].astype(bool) & ~is_alt
        is_reclassified = succeeded & df["reclassified"].astype(bool) & ~is_alt & ~is_eurostat_repair
        is_fallback = (succeeded & (df["served_by_driver_id"] != df["driver_id"])
                      & ~is_alt & ~is_eurostat_repair & ~is_reclassified)
        is_direct = succeeded & ~is_alt & ~is_eurostat_repair & ~is_reclassified & ~is_fallback

        def _count(mask) -> int:
            return int(mask.sum())

        def _pct(n: int) -> float:
            return round(n / total, 4) if total else 0.0

        def _row(metric: str, mask=None) -> dict:
            if mask is None:
                return {"metric": metric, "count": None, "pct_of_total": None}
            n = _count(mask)
            return {"metric": metric, "count": n, "pct_of_total": _pct(n)}

        rows = [
            {"metric": "Total drivers attempted", "count": total, "pct_of_total": 1.0 if total else 0.0},
            _row("Total succeeded (usable data)", combined),
            _row("  by data quality:"),
            _row("    Succeeded, Clean", combined & ~has_notes),
            # Label is an approximation, not a precise category: quality_notes
            # also fires for density (<80% of months populated) and staleness
            # (latest observation older than budget), independent of history
            # length. Short history is the dominant real-world cause today,
            # but a row can land in this bucket for either/both other reasons
            # too -- see quality_notes itself (joined into the "All Drivers"/
            # "Successful" sheets) for the specific reason on any given row.
            _row("    Succeeded, Partial (less than 8 years of history)", combined & has_notes),
            _row("  by source:"),
            _row("    Reused from prior run (already verified)", reused),
            _row("    Succeeded this run — Direct/original source", is_direct),
            _row("    Succeeded this run — Registered fallback", is_fallback),
            _row("    Succeeded this run — Reclassification rescue", is_reclassified),
            _row("    Succeeded this run — Alternate source search", is_alt),
            _row("    Succeeded this run — Eurostat locator repair", is_eurostat_repair),
            _row("Failed / No data", failed),
        ]
        return pd.DataFrame(rows)

    def print_summary(self) -> None:
        """Console summary. The manifest holds the detail."""
        if not self.manifest_entries:
            print("\nNo drivers processed.")
            return
        df = pd.DataFrame(self.manifest_entries)
        print("\n" + "=" * 96)
        print("RUN SUMMARY")
        print("=" * 96)
        display = df[["driver_id", "commodity", "region", "outcome", "tier_declared",
                      "tier_used", "monthly_observations", "history_years", "first_month",
                      "last_month"]]
        print(display.to_string(index=False))
        summary = self._summarise()
        print("-" * 96)
        for key, value in summary.items():
            print(f"  {key:32s} {value}")
        print("-" * 96)
        breakdown = self._success_breakdown()
        for _, row in breakdown.iterrows():
            # A section-header row carries None in these columns, which
            # pandas coerces to NaN (a float) once mixed with real ints in
            # the same column — pd.isna() catches that; `is None` would not.
            if pd.isna(row["count"]):
                print(f"  {row['metric']}")
            else:
                print(f"  {row['metric']:52s} {int(row['count']):>6d}   {row['pct_of_total']:.1%}")
        print("=" * 96)

    def write_excel_report(self, hitl_tasks: list, started_at: datetime,
                           deferred_entries: list[dict] | None = None,
                           waited_entries: list[dict] | None = None) -> Path | None:
        """
        Write a timestamped Excel workbook with multiple sheets summarizing the run.
        Purely presentational — all data already collected in manifest_entries and hitl_tasks.
        """
        if not self.cfg.get("output.write_run_report", True):
            return None

        if not self.manifest_entries:
            log.warning("No drivers processed; skipping Excel report")
            return None

        try:
            import openpyxl
            from openpyxl.styles import PatternFill
        except ImportError:
            log.warning("openpyxl not installed; skipping Excel report (pip install openpyxl)")
            return None

        # Distinct amber fill for rows the run confirmed as bot-blocked
        # (see HttpClient.bot_blocked_hosts / finalize_bot_block_flags) —
        # applied by row position, since to_excel writes rows 2..N in the
        # DataFrame's own order with the header on row 1.
        bot_block_fill = PatternFill(start_color="FFD9A6", end_color="FFD9A6", fill_type="solid")

        def _highlight_bot_blocked(worksheet, sheet_df: pd.DataFrame) -> None:
            if "likely_bot_blocked" not in sheet_df.columns:
                return
            n_cols = sheet_df.shape[1]
            for row_offset, is_blocked in enumerate(sheet_df["likely_bot_blocked"].tolist()):
                if not is_blocked:
                    continue
                excel_row = row_offset + 2  # header is row 1
                for col in range(1, n_cols + 1):
                    worksheet.cell(row=excel_row, column=col).fill = bot_block_fill

        timestamp = started_at.strftime("%Y%m%d_%H%M%S")
        report_path = self.curated / f"run_report_{timestamp}.xlsx"

        df_manifest = pd.DataFrame(self.manifest_entries)

        # Prepare HITL lookup: dict[driver_id] -> required_action + credential_keys + drop_path
        hitl_lookup = {}
        for task in hitl_tasks:
            hitl_lookup[task.driver_id] = {
                "required_action": task.required_action,
                "credential_keys": ", ".join(task.credential_keys) if task.credential_keys else "",
                "drop_file_path": task.suggested_drop_path,
            }

        def _write(tmp_path: Path) -> None:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                # Sheet 1: Summary
                summary = self._summarise()
                summary_df = pd.DataFrame([
                    {"metric": k, "value": v} for k, v in summary.items()
                ])
                summary_df.to_excel(writer, sheet_name="Summary", index=False)

                # Add a tier breakdown sub-table
                tier_breakdown = df_manifest[df_manifest["monthly_observations"] > 0]["tier_used"].value_counts().sort_index()
                tier_df = pd.DataFrame({
                    "tier_used": tier_breakdown.index,
                    "drivers_with_data": tier_breakdown.values,
                })
                tier_df.to_excel(writer, sheet_name="Summary", startrow=len(summary_df) + 3, index=False)

                # Combined success breakdown: total usable data (this run's
                # live successes plus everything reused via --skip-flagged),
                # by data quality and by resolution path. See
                # _success_breakdown()'s docstring for why this is a separate
                # table from summary_df above rather than folded into it.
                breakdown_df = self._success_breakdown()
                breakdown_startrow = len(summary_df) + 3 + len(tier_df) + 3
                breakdown_df.to_excel(writer, sheet_name="Summary", startrow=breakdown_startrow, index=False)

                # Sheet 2: All Drivers (full manifest, quality_notes joined)
                all_drivers = df_manifest.copy()
                all_drivers["quality_notes"] = all_drivers["quality_notes"].apply(
                    lambda x: "; ".join(x) if isinstance(x, list) else ""
                )
                all_drivers.to_excel(writer, sheet_name="All Drivers", index=False)
                _highlight_bot_blocked(writer.sheets["All Drivers"], all_drivers)

                # Sheet 3: Successful (includes drivers reused via --skip-flagged —
                # "skipped" means "already verified good", not "failed")
                successful = df_manifest[df_manifest["outcome"].isin(["success", "skipped"])].copy()
                successful["quality_notes"] = successful["quality_notes"].apply(
                    lambda x: "; ".join(x) if isinstance(x, list) else ""
                )
                successful.to_excel(writer, sheet_name="Successful", index=False)

                # Sheet 4: Failed & HITL (joined with HITL reasons)
                failed = df_manifest[~df_manifest["outcome"].isin(["success", "skipped"])].copy()
                failed["quality_notes"] = failed["quality_notes"].apply(
                    lambda x: "; ".join(x) if isinstance(x, list) else ""
                )
                # Left join with HITL tasks
                failed["required_action"] = failed["driver_id"].map(
                    lambda did: hitl_lookup.get(did, {}).get("required_action", "")
                )
                failed["credential_keys"] = failed["driver_id"].map(
                    lambda did: hitl_lookup.get(did, {}).get("credential_keys", "")
                )
                failed["drop_file_path"] = failed["driver_id"].map(
                    lambda did: hitl_lookup.get(did, {}).get("drop_file_path", "")
                )
                failed.to_excel(writer, sheet_name="Failed & HITL", index=False)
                _highlight_bot_blocked(writer.sheets["Failed & HITL"], failed)

                # Sheet 5: By Commodity (aggregate per commodity, same metrics as Summary)
                by_commodity = []
                for commodity, group in df_manifest.groupby("commodity", sort=False):
                    is_success = group["outcome"].isin(["success", "skipped"])
                    succeeded = is_success.sum()
                    succeeded_with_notes = (is_success & group["quality_notes"].apply(bool)).sum()
                    failed_count = (~is_success).sum()
                    tier_dist = group[group["monthly_observations"] > 0]["tier_used"].value_counts().to_dict()
                    by_commodity.append({
                        "commodity": commodity,
                        "drivers_attempted": len(group),
                        "succeeded": succeeded,
                        # Combined scope (this run's successes + reused
                        # skips), matching _success_breakdown()'s "Succeeded,
                        # Partial" row — deliberately not the narrower
                        # this-run-only figure in the Summary sheet's
                        # succeeded_with_quality_notes_this_run.
                        "succeeded_with_quality_notes_combined": succeeded_with_notes,
                        "failed": failed_count,
                        "tiers_resolved": str(tier_dist) if tier_dist else "",
                        "proxy_tagged_attempts": int(group["is_proxy"].sum()),
                        "served_by_proxy_success": int((group["is_proxy"] & is_success).sum()),
                        "meeting_min_history": int(group["meets_min_history"].sum()),
                        "total_monthly_observations": int(group["monthly_observations"].sum()),
                    })
                by_commodity_df = pd.DataFrame(by_commodity)
                by_commodity_df.to_excel(writer, sheet_name="By Commodity", index=False)

                # Sheet 6: Deferred (Rate-Limited) — drivers this run skipped
                # proactively because a source's configured request budget
                # (http.source_limits) was reached, not because anything
                # failed. Only written when there's something to show.
                if deferred_entries:
                    deferred_df = pd.DataFrame(deferred_entries)
                    if "retry_after_seconds" in deferred_df.columns:
                        deferred_df["retry_after"] = deferred_df["retry_after_seconds"].apply(_format_retry_after)
                    deferred_df.to_excel(writer, sheet_name="Deferred (Rate-Limited)", index=False)

                # Sheet 7: Rate-Limited (Waited) — occasions this run paused
                # a worker thread in place until a host's rate-limit window
                # reset (http.rate_limit_wait_cap_seconds), then retried the
                # same call successfully, instead of deferring it to a later
                # run. Distinct from the Deferred sheet above: nothing here
                # was skipped, it just took longer. See RateLimiter.check().
                if waited_entries:
                    waited_df = pd.DataFrame(waited_entries)
                    if "waited_seconds" in waited_df.columns:
                        waited_df["waited"] = waited_df["waited_seconds"].apply(_format_retry_after)
                    waited_df.to_excel(writer, sheet_name="Rate-Limited (Waited)", index=False)

        try:
            write_atomic(report_path, _write)
            log.info("Wrote Excel report -> %s", report_path)
            return report_path

        except Exception as exc:
            log.error("Failed to write Excel report: %s", exc)
            return None
