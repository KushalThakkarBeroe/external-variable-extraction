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
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import yaml

from .models import DriverSpec, FetchResult, utc_now
from .rollup import history_years
from .success_flags import build_flags_df, write_flags_excel

log = logging.getLogger(__name__)


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
            "meets_min_history": (history_years(monthly) >= float(
                self.cfg.get("run.min_history_years", 8)) if not monthly.empty else False),
            "is_proxy": spec.is_proxy,
            "served_by_driver_id": result.extra.get("served_by_driver_id", spec.driver_id),
            "served_by_source": result.extra.get("serving_spec", spec).source_name,
            "reclassified": result.extra.get("reclassified", False),
            "alternate_source_used": result.extra.get("alternate_source_used", False),
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

        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

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
            new.to_excel(path, sheet_name="Monthly")
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
        merged.to_excel(path, sheet_name="Monthly")
        log.info("Updated tracker for %s / %s: %d new month(s), %d revised, %d unchanged -> %s",
                 commodity, region, len(added), len(revised),
                 len(common) - len(revised), path.name)

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
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        log.info("Wrote run manifest -> %s", path)
        return path

    def write_success_flags(self, threshold: float | None = None) -> Path | None:
        """
        Excel flag file marking which drivers cleared the coverage threshold
        this run, for a future run's --skip-flagged to read (see
        pipeline/success_flags.py). Regenerated fresh from this run's
        manifest_entries every time, so a driver reused via --skip-flagged
        (outcome SKIPPED) that still clears the bar keeps propagating
        forward across runs with no special merge logic needed.
        """
        if not self.manifest_entries:
            return None
        if threshold is None:
            threshold = float(self.cfg.get("output.success_flag_threshold", 0.4))
        df = build_flags_df(self.manifest_entries, threshold)
        path = self.cfg.path("paths.success_flag_file", "output/success_flags.xlsx")
        return write_flags_excel(df, path, threshold)

    def finalize_bot_block_flags(self, bot_blocked_hosts: set[str] | None = None) -> None:
        """
        Tags every manifest entry with whether its source host was confirmed
        bot-blocking requests at any point this run (see HttpClient's
        bot_blocked_hosts in pipeline/http_client.py).

        Called once, after the fetch loop finishes, before any write_* call —
        the blocked-host set isn't complete until every driver has been
        attempted, so a host a later driver discovers is blocked would be
        missed if this ran earlier (e.g. per-driver, inside add()). Mutates
        manifest_entries in place so write_manifest, write_excel_report and
        _summarise all read the same computed field from one source of truth.
        """
        blocked = bot_blocked_hosts or set()
        for entry in self.manifest_entries:
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
        path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
                        encoding="utf-8")
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
            "succeeded_with_quality_notes": int((succeeded & has_notes).sum()),
            "skipped_reused_from_prior_run": int(skipped.sum()),
            "blocked_or_failed": int((~succeeded & ~skipped).sum()),
            "likely_bot_blocked_count": int(df["likely_bot_blocked"].sum()),
            "resolved_by_tier": df.loc[df["monthly_observations"] > 0, "tier_used"]
                                  .value_counts().sort_index().to_dict(),
            "served_by_proxy": int(df["is_proxy"].sum()),
            "meeting_min_history": int(df["meets_min_history"].sum()),
            "total_monthly_observations": int(df["monthly_observations"].sum()),
        }

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
        print("=" * 96)

    def write_excel_report(self, hitl_tasks: list, started_at: datetime) -> Path | None:
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

        try:
            with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
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
                        "succeeded_with_quality_notes": succeeded_with_notes,
                        "failed": failed_count,
                        "tiers_resolved": str(tier_dist) if tier_dist else "",
                        "proxies": int(group["is_proxy"].sum()),
                        "meeting_min_history": int(group["meets_min_history"].sum()),
                        "total_monthly_observations": int(group["monthly_observations"].sum()),
                    })
                by_commodity_df = pd.DataFrame(by_commodity)
                by_commodity_df.to_excel(writer, sheet_name="By Commodity", index=False)

            log.info("Wrote Excel report -> %s", report_path)
            return report_path

        except Exception as exc:
            log.error("Failed to write Excel report: %s", exc)
            return None
