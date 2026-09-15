"""
The cascade orchestrator.

For each driver it walks an escalation ladder and stops at the first rung that
produces usable data:

  1. Declared tier          The registry's best guess, tried first because it is
                            usually right and is the cheapest path.
  2. Remaining tiers        In ascending order of difficulty. A source declared
                            Tier 2 might expose a quiet JSON endpoint (Tier 1),
                            or the workbook link might have moved behind a
                            paginated archive (Tier 3).
  3. LLM re-classification  All tiers failed. Re-read the landing page, work out
                            what the access pattern actually is, and retry once
                            against that verdict. The suggestion is written to
                            the manifest for an analyst to fold into the
                            registry; it is never applied silently.
  4. Registered fallback    The registry's declared alternative source, run
                            through the same cascade. This is how a proxy takes
                            over when a primary series dies.
  5. Alternate-source search  Still nothing? Ask the LLM for a genuinely
                            different publisher (not just a different way to
                            read a source already tried), verify it through
                            the exact same live-fetch + quality-gate bar as
                            every tier above, and — only if it actually
                            works — commit it into the registry as a new
                            fallback so a future run finds it directly
                            instead of searching again. A rejected proposal
                            is remembered too (output/rejected_alternate_sources.yaml),
                            so the same dead end is never re-proposed.
  6. Human in the loop      A task is recorded with the exact action needed and
                            the exact config key or drop path that will resolve
                            it. The run continues.

Quality gates applied to whatever comes back:
  - minimum history in years
  - share of months that are complete rather than sparse
  - staleness against the registry's expected update lag

A result that clears every gate is returned immediately. One that doesn't but
still carries real monthly data is held as a floor — real data beats no
data — while the ladder keeps looking for a cleaner source at a later tier.
Either way the outcome is reported as SUCCESS; a gate failure only ever shows
up as a `quality_notes` annotation (short history, staleness, sparse
coverage), never as a separate outcome category, so "thin" data is never
mistaken for something having gone wrong.

A result that produces zero usable monthly observations (raw bytes came
back, but rolled up to nothing after the run-window clip) is a different
case entirely: it is never held as a floor and never reported as SUCCESS,
because there is nothing there for a floor to protect. It keeps escalating
through reclassification, the registered fallback, and finally a
human-in-the-loop task, exactly as if nothing had been found at all.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import pandas as pd

from . import eurostat_repair
from .models import DriverSpec, FetchResult, Frequency, HitlTask, Outcome, Tier, utc_now
from .registry_enrichment import _normalize_access_mode, _normalize_connector
from .rollup import history_years, to_monthly
from .tiers.base import manual_file
from .tiers.tier1_direct import Tier1Direct
from .tiers.tier2_files import Tier2EmbeddedFile
from .tiers.tier3_multipage import Tier3MultiPage
from .tiers.tier4_gated import Tier4Gated

log = logging.getLogger(__name__)


class CascadeOrchestrator:
    def __init__(self, cfg, http, connectors, llm=None,
                rejected_sources: Optional[dict[str, list[dict]]] = None,
                rejected_eurostat_repairs: Optional[dict[str, list[dict]]] = None):
        self.cfg = cfg
        self.http = http
        self.llm = llm
        self.handlers: dict[Tier, object] = {
            Tier.DIRECT: Tier1Direct(cfg, http, connectors, llm),
            Tier.EMBEDDED_FILE: Tier2EmbeddedFile(cfg, http, connectors, llm),
            Tier.MULTIPAGE: Tier3MultiPage(cfg, http, connectors, llm),
            Tier.GATED: Tier4Gated(cfg, http, connectors, llm),
        }
        self.hitl_tasks: list[HitlTask] = []
        self.tier_suggestions: list[dict] = []
        # Verified alternate sources found this run, committed to the
        # registry once at the end (see pipeline/alternate_sources.py) —
        # never mid-run, since this can be appended to from a worker thread
        # under the parallel runner and a shared CSV needs one writer.
        self.alternate_source_hits: list[dict] = []
        # Rejected alternate-source proposals this run, persisted the same
        # way so a dead-end candidate is never re-proposed on a later run.
        self.rejected_alternate_source_attempts: list[dict] = []
        # Rejections from PRIOR runs, loaded once by the caller before the
        # fetch loop and keyed by driver_id for O(1) lookup here.
        self.rejected_sources: dict[str, list[dict]] = rejected_sources or {}
        # Verified Eurostat locator repairs this run, committed to the
        # registry once at the end (see pipeline/eurostat_repair.py) — same
        # single-writer, end-of-run discipline as alternate_source_hits.
        self.eurostat_locator_repair_hits: list[dict] = []
        # Rejected repair attempts this run, persisted so a dead-end
        # dataset/filter guess is never re-tried on a later run.
        self.rejected_eurostat_repair_attempts: list[dict] = []
        # Rejections from PRIOR runs, loaded once by the caller, keyed by
        # driver_id — separate from rejected_sources since the record shape
        # (dataset/filters vs source_url) differs and the two escalation
        # steps' attempt-cap budgets must not interfere with each other.
        self.rejected_eurostat_repairs: dict[str, list[dict]] = rejected_eurostat_repairs or {}

    # ------------------------------------------------------------------ entry

    def run_driver(self, spec: DriverSpec, all_specs: dict[str, DriverSpec],
                   depth: int = 0) -> tuple[FetchResult, pd.DataFrame]:
        """
        Resolve one driver. Returns the winning FetchResult and its monthly frame.

        `depth` guards against a registry with a fallback cycle.
        """
        log.info("=" * 78)
        log.info("Driver %s | %s / %s | declared %s",
                 spec.driver_id, spec.commodity, spec.driver_name, spec.declared_tier.label)

        if depth == 0:
            # Only the top-level call establishes attribution — a nested
            # fallback/alternate-source/repair sub-fetch under this same
            # call tree is deliberately left attributed to this driver_id
            # too, since only the top-level driver gets a manifest entry.
            self.http.set_current_driver(spec.driver_id)

        # A manually supplied file always wins, for EVERY driver regardless
        # of access_mode — checked before any tier is even attempted, no
        # network call needed. Previously this only happened inside
        # Tier4Gated.fetch(), reached only when access_mode was
        # api_key/login/paid_or_restricted — an open_api/html_table/
        # file_download driver's manually-dropped file (exactly what its own
        # HITL task told the analyst to create) was never even checked.
        manual_result = self._check_manual_drop(spec)
        if manual_result is not None:
            return manual_result

        blocked_host = self._blocked_host(spec)
        if blocked_host:
            log.warning("Skipping %s: host %s is on cascade.known_blocked_hosts", spec.driver_id, blocked_host)
            self._record_hitl(spec, reason_override=f"Known-blocked host ({blocked_host}); skipped without retrying")
            return FetchResult(
                driver_id=spec.driver_id,
                outcome=Outcome.FAILED_PERMANENT,
                tier_attempted=spec.declared_tier,
                message=f"Host {blocked_host} is on the known-blocked list; skipped without a network call",
                source_url_used=spec.source_url,
            ), pd.DataFrame()

        # Tracks connectors that have already failed with a credential/paywall
        # block this driver. Calling the same connector again from a
        # different tier would raise the identical PermissionError — the key
        # is still missing, no tier changes that — so it's skipped rather
        # than burning another attempt and another identical log line.
        blocked_connectors: set[str] = set()

        # Holds the best thin result found so far (real data, but short
        # history / sparse / stale) while the ladder keeps looking for
        # something fuller. Both this and a clean pass are reported as
        # SUCCESS — quality_notes carries the "thin" signal, not the
        # outcome — but a clean pass is still preferred when one exists.
        best: Optional[FetchResult] = None
        best_monthly = pd.DataFrame()

        # Set when a tier attempt fails with a structurally-diagnosed bad
        # Eurostat dataset code or filter (see connectors.EurostatLocatorError
        # / tiers.base._guard). Local, not instance state, since this method
        # runs concurrently across drivers under run_pipeline_parallel.py.
        eurostat_repair_hint: Optional[dict] = None

        # The most recent failing FetchResult from the main ladder, kept so
        # a HITL task (if we end up recording one) can show the real reason
        # a live attempt failed — a bad locator, a bot-block, a genuine 404
        # — instead of guessing from static registry fields alone (see
        # _record_hitl). A driver that ran with a real, working credential
        # and still failed must never be told it's missing one.
        last_failure_result: Optional[FetchResult] = None

        for tier in self._ladder(spec):
            handler = self.handlers[tier]
            if not handler.can_handle(spec):
                log.debug("Tier %d cannot handle %s; skipping", int(tier), spec.driver_id)
                continue
            if spec.connector and spec.connector in blocked_connectors:
                log.debug("Tier %d skipped for %s: connector '%s' already blocked_credentials this driver",
                         int(tier), spec.driver_id, spec.connector)
                continue

            attempts = int(self.cfg.get("cascade.max_tier_attempts", 2))
            for attempt in range(1, attempts + 1):
                result = handler.fetch(spec)
                log.info("  Tier %d attempt %d -> %s (%s)",
                         int(tier), attempt, result.outcome, result.message[:120])

                if not result.ok:
                    last_failure_result = result
                    if result.outcome in (Outcome.BLOCKED_PAID, Outcome.BLOCKED_CREDENTIALS):
                        if spec.connector:
                            blocked_connectors.add(spec.connector)
                        break                       # retrying will not find a key
                    if result.outcome == Outcome.FAILED_PERMANENT:
                        if result.extra.get("eurostat_repair"):
                            eurostat_repair_hint = result.extra["eurostat_repair"]
                        break                       # escalate rather than retry
                    continue                        # retryable: try this tier again

                monthly, detected_frequency = self._rollup(spec, result)
                passed, notes, coverage_ratio = self._quality_gate(spec, monthly, detected_frequency)
                result.extra["quality_notes"] = notes
                result.extra["frequency_detected"] = detected_frequency
                result.extra["coverage_ratio"] = coverage_ratio
                result.extra.setdefault("serving_spec", spec)
                # Both a clean pass and a thin-but-real result are reported as
                # SUCCESS — the difference is carried in quality_notes, not a
                # separate outcome — but a clean pass still wins outright,
                # while a thin one is held as a floor in case a later tier
                # does better.
                result.outcome = Outcome.SUCCESS

                if passed:
                    log.info("  ACCEPTED at tier %d: %d months, %.1f years of history",
                             int(tier), len(monthly), history_years(monthly))
                    return result, monthly

                log.warning("  Tier %d result carries quality notes, still searching: %s",
                           int(tier), "; ".join(notes))
                # Optional, off by default: an analyst can opt a specific run
                # into hard-failing a result that's too sparse to hold as a
                # floor, forcing escalation (fallback/alternate-source/HITL)
                # instead of accepting thin data. Never the default behavior —
                # flipping this globally would push today's accepted partial
                # successes into HITL, a regression for anyone not opting in.
                hard_fail_threshold = self.cfg.get("rollup.hard_fail_below_coverage_ratio")
                too_sparse = (hard_fail_threshold is not None
                             and coverage_ratio < float(hard_fail_threshold))
                if best is None and not too_sparse:
                    best, best_monthly = result, monthly
                break

        # ---- every tier exhausted without a clean pass ----

        # A "best" floor is only worth keeping if it actually has usable
        # monthly data. A tier can come back with real raw observations that
        # still roll up (after the run-window clip) to zero populated
        # months — bytes in, nothing usable out. That must not count as a
        # floor: it must not block escalation, and must never be handed back
        # as a fabricated SUCCESS (see the module docstring).
        #
        # "Usable" also has a floor on volume, not just non-zero: a driver
        # with only a handful of populated months (well under min_history_years'
        # 8-year bar too, so it was already going to be flagged "Partial") is
        # too thin to be a meaningful regressor. Below run.min_months_for_success
        # it must not be reported as a success at all — it has to keep
        # escalating (fallback / alternate-source / HITL) instead of settling
        # for a floor that thin. This is a separate, stricter bar than
        # min_history_years above: that one only ever adds a quality_notes
        # annotation, it never blocks acceptance.
        min_months = self._min_months_for_success()
        best_populated_months = (
            int(best_monthly["value"].notna().sum())
            if best is not None and not best_monthly.empty else 0
        )
        best_too_thin = 0 < best_populated_months < min_months
        best_has_data = (
            best is not None and not best_monthly.empty
            and best_populated_months >= min_months
        )

        # Step 2.5: Eurostat-specific locator repair. Cheaper and more
        # targeted than a full landing-page re-read (step 3 below) — an API
        # locator problem (wrong dataset code or filter) can't be fixed by
        # re-reading HTML, so this fires first when the failure was
        # structurally diagnosed as exactly that. Only ever touches Eurostat
        # drivers; every other connector's escalation path is unaffected.
        if not best_has_data and eurostat_repair_hint and self.cfg.get(
                "cascade.allow_eurostat_locator_repair", True):
            repaired = self._repair_eurostat_locator(spec, eurostat_repair_hint)
            if repaired is not None:
                return repaired

        # Step 3: ask the LLM what this source actually is, then retry once.
        if not best_has_data and self.llm and depth == 0:
            retried = self._retry_after_reclassification(spec)
            if retried is not None:
                return retried

        # Step 4: registered fallback source.
        if not best_has_data and spec.fallback_driver_id and depth < 3:
            fallback = all_specs.get(spec.fallback_driver_id)
            if fallback:
                log.warning("Falling back from %s to %s", spec.driver_id, fallback.driver_id)
                fb_result, fb_monthly = self.run_driver(fallback, all_specs, depth + 1)
                if fb_result.ok:
                    # The panel keeps the PRIMARY driver_id as the regressor slot
                    # so downstream models see a stable column, but provenance
                    # must point at the source that actually served the data.
                    fb_result.extra["served_by_driver_id"] = fb_result.extra.get(
                        "served_by_driver_id", fallback.driver_id
                    )
                    fb_result.extra["serving_spec"] = fb_result.extra.get(
                        "serving_spec", fallback
                    )
                    return fb_result, fb_monthly

        # Step 4.5: search for a genuinely different source entirely — not
        # just another way to read something already tried. Only reached
        # when nothing above, including any registered fallback, produced
        # real data. Restricted to depth == 0 (the primary driver only) so a
        # long already-failed fallback chain doesn't multiply into several
        # searches; see CascadeOrchestrator._search_alternate_source.
        if not best_has_data and self.llm and depth == 0:
            alternate = self._search_alternate_source(spec, all_specs)
            if alternate is not None:
                return alternate

        # Step 5: hand to a human — unless a thin-but-real result was already held as a floor.
        if not best_has_data:
            message = "All tiers exhausted; recorded as a human-in-the-loop task"
            if best_too_thin:
                message = (f"All tiers exhausted (best attempt returned only {best_populated_months} "
                          f"populated month(s), below the {min_months}-month minimum); recorded as "
                          "a human-in-the-loop task")
                self._record_hitl(spec, last_failure=last_failure_result,
                                  insufficient_months=(best_populated_months, min_months))
            else:
                if best is not None:
                    message = ("All tiers exhausted (best attempt returned 0 usable months after "
                              "rollup); recorded as a human-in-the-loop task")
                self._record_hitl(spec, last_failure=last_failure_result)
            return FetchResult(
                driver_id=spec.driver_id,
                outcome=Outcome.BLOCKED_CREDENTIALS if spec.human_in_loop else Outcome.FAILED_PERMANENT,
                tier_attempted=spec.declared_tier,
                message=message,
                source_url_used=spec.source_url,
            ), pd.DataFrame()

        log.info("Returning best-effort result for %s (see quality_notes)", spec.driver_id)
        return best, best_monthly

    # ------------------------------------------------------- cache reuse

    def reuse_cached_result(self, spec: DriverSpec, raw_observations: pd.DataFrame,
                            retrieved_at: datetime, source_url: str,
                            tier_used: Tier, message: str = "") -> tuple[FetchResult, pd.DataFrame]:
        """
        Reconstruct a driver's result from previously-cached raw observations
        (output/interim/<driver_id>_observations.csv) instead of making a
        live fetch. Used by the --skip-flagged path in both entry scripts so
        a driver already known to clear the success-flag threshold is not
        re-fetched this run, but still rolls through the exact same
        aggregation/window-clip logic a live fetch would use and still lands
        in this run's outputs with full provenance.

        Runs the same rollup + quality gate as a live fetch, so a reused
        driver's monthly data is clipped to this run's window exactly like a
        fresh one would be. Returns an empty monthly frame if the cached
        observations no longer produce anything usable after rollup (e.g.
        the run window moved since the data was cached) — the caller should
        treat that as "cannot reuse" and fetch live instead.
        """
        result = FetchResult(
            driver_id=spec.driver_id,
            outcome=Outcome.SKIPPED,
            tier_attempted=tier_used,
            observations=raw_observations,
            source_url_used=source_url or spec.source_url,
            unit=spec.unit,
            message=message or "Reused from a prior run; already flagged as sufficiently complete",
            retrieved_at=retrieved_at,
        )
        monthly, detected_frequency = self._rollup(spec, result)
        if monthly.empty or monthly["value"].notna().sum() == 0:
            return result, pd.DataFrame()

        _passed, notes, coverage_ratio = self._quality_gate(spec, monthly, detected_frequency)
        result.extra["quality_notes"] = notes
        result.extra["frequency_detected"] = detected_frequency
        result.extra["coverage_ratio"] = coverage_ratio
        return result, monthly

    # ------------------------------------------------------ alternate source

    def _walk_fallback_chain(self, spec: DriverSpec,
                             all_specs: dict[str, DriverSpec]) -> tuple[list[dict], DriverSpec]:
        """
        Walks a driver's fallback_driver_id chain to its end.

        Returns (already_tried, tail_spec):
          already_tried - one {source_url, source_name, connector} dict per
            link in the chain (including the primary itself), for the
            alternate-source search's exclusion list.
          tail_spec - the last spec in the chain (the one with no further
            fallback_driver_id), i.e. where a new discovery should attach so
            an existing human-configured fallback is never overwritten.

        Guards against a cyclic chain with a seen-ids set.
        """
        already_tried = [{"source_url": spec.source_url, "source_name": spec.source_name,
                          "connector": spec.connector}]
        tail = spec
        seen_ids = {spec.driver_id}
        fb_id = spec.fallback_driver_id
        while fb_id and fb_id not in seen_ids:
            seen_ids.add(fb_id)
            fb_spec = all_specs.get(fb_id)
            if not fb_spec:
                break
            already_tried.append({"source_url": fb_spec.source_url, "source_name": fb_spec.source_name,
                                  "connector": fb_spec.connector})
            tail = fb_spec
            fb_id = fb_spec.fallback_driver_id
        return already_tried, tail

    def _search_alternate_source(self, spec: DriverSpec,
                                 all_specs: dict[str, DriverSpec]) -> Optional[tuple[FetchResult, pd.DataFrame]]:
        """
        Last resort before HITL: ask the LLM for a genuinely different
        publisher for this driver's data, verify it through the exact same
        live-fetch + quality-gate bar as every other tier, and only use it
        if that verification actually passes — never trusted on the LLM's
        word alone.

        A successful hit is recorded in self.alternate_source_hits for a
        one-time, end-of-run, single-writer commit into driver_registry.csv
        (see pipeline/alternate_sources.py) rather than written here,
        because this can run inside a worker thread under the parallel
        runner and a shared CSV file needs one sequential writer, not many
        concurrent ones. A rejected proposal is recorded in
        self.rejected_alternate_source_attempts so it is never re-proposed
        on a future run.
        """
        if not self.cfg.get("cascade.allow_alternate_source_search", True):
            return None

        already_tried, chain_tail = self._walk_fallback_chain(spec, all_specs)

        prior_rejections = self.rejected_sources.get(spec.driver_id, [])
        max_attempts = int(self.cfg.get("cascade.alternate_source_max_attempts_per_driver", 3))
        if len(prior_rejections) >= max_attempts:
            log.info("Alternate-source search skipped for %s: already rejected %d time(s) (max %d)",
                     spec.driver_id, len(prior_rejections), max_attempts)
            return None
        already_tried = already_tried + [
            {"source_url": r.get("source_url", ""), "source_name": r.get("source_name", ""),
             "connector": r.get("connector", "")}
            for r in prior_rejections
        ]

        proposal = self.llm.propose_source(spec.commodity, spec.driver_name, spec.region,
                                           spec.unit, already_tried=already_tried)

        def _reject(reason: str, proposed: Optional[dict] = None) -> None:
            self.rejected_alternate_source_attempts.append({
                "driver_id": spec.driver_id,
                "source_url": (proposed or {}).get("source_url", ""),
                "source_name": (proposed or {}).get("source_name", ""),
                "connector": (proposed or {}).get("connector", ""),
                "reason": reason,
                "rejected_at": utc_now().isoformat(timespec="seconds"),
            })
            log.warning("Alternate-source proposal rejected for %s: %s", spec.driver_id, reason)

        if not proposal:
            _reject("no_proposal")
            return None

        confidence = float(proposal.get("confidence", 0))
        min_confidence = float(self.cfg.get("cascade.alternate_source_min_confidence", 0.4))
        if confidence < min_confidence:
            _reject("low_confidence", proposal)
            return None

        proposed_url = str(proposal.get("source_url", "")).strip()
        proposed_domain = urlparse(proposed_url).netloc.lower()
        if not proposed_url or not proposed_domain:
            _reject("no_url", proposal)
            return None
        tried_domains = {urlparse(t["source_url"]).netloc.lower()
                         for t in already_tried if t.get("source_url")}
        if proposed_domain in tried_domains:
            _reject("same_domain_as_tried", proposal)
            return None

        connector = _normalize_connector(proposal.get("connector", ""))
        declared_tier_raw = proposal.get("declared_tier", int(spec.declared_tier))
        try:
            declared_tier = Tier(int(declared_tier_raw))
        except (TypeError, ValueError):
            declared_tier = spec.declared_tier
        access_mode = _normalize_access_mode(proposal.get("access_mode", ""), int(declared_tier))

        revised = DriverSpec(**{**spec.__dict__})
        revised.source_url = proposed_url
        revised.source_name = str(proposal.get("source_name", "")).strip() or spec.source_name
        revised.connector = connector
        revised.access_mode = access_mode
        revised.endpoint_or_locator = str(proposal.get("endpoint_or_locator", "")).strip()
        revised.declared_tier = declared_tier
        revised.native_frequency = str(proposal.get("native_frequency", "")).strip() or spec.native_frequency
        revised.rollup_method = str(proposal.get("rollup_method", "")).strip().lower() or spec.rollup_method
        # unit is deliberately NOT taken from the proposal — never trust an
        # LLM-guessed unit; keep the primary driver's own, known-correct one.
        revised.fallback_driver_id = None
        # Marks this as a substitute source in the panel/manifest, matching
        # the is_proxy=Y the committed registry row gets in
        # pipeline/alternate_sources.py — consistent provenance whether or
        # not the run's HITL/commit step has landed yet.
        revised.is_proxy = True

        handler = self.handlers[declared_tier]
        result = handler.fetch(revised)
        if not result.ok:
            _reject(f"fetch_failed:{result.outcome}", proposal)
            return None

        monthly, detected_frequency = self._rollup(revised, result)
        if monthly.empty or monthly["value"].notna().sum() < self._min_months_for_success():
            _reject("quality_gate_failed:insufficient_months", proposal)
            return None

        _passed, notes, coverage_ratio = self._quality_gate(revised, monthly, detected_frequency)
        result.outcome = Outcome.SUCCESS
        result.extra["quality_notes"] = notes
        result.extra["frequency_detected"] = detected_frequency
        result.extra["coverage_ratio"] = coverage_ratio
        result.extra["reclassified"] = True
        result.extra["alternate_source_used"] = True
        # Provenance must point at the source that actually served the data
        # (revised), not the primary's original, now-dead one — same
        # convention Step 4's registered-fallback path already uses.
        result.extra["served_by_driver_id"] = spec.driver_id
        result.extra["serving_spec"] = revised

        self.alternate_source_hits.append({
            "driver_id": spec.driver_id,
            "chain_tail_id": chain_tail.driver_id,
            "commodity": spec.commodity,
            "driver_name": spec.driver_name,
            "region": spec.region,
            "unit": spec.unit,
            "history_from": spec.history_from or "",
            "update_lag_days": spec.update_lag_days,
            "source_name": revised.source_name,
            "source_url": revised.source_url,
            "connector": revised.connector,
            "access_mode": revised.access_mode,
            "endpoint_or_locator": revised.endpoint_or_locator,
            "declared_tier": int(revised.declared_tier),
            "native_frequency": revised.native_frequency,
            "rollup_method": revised.rollup_method,
            "confidence": confidence,
            "reasoning": proposal.get("reasoning", ""),
            "verified_at": utc_now().isoformat(timespec="seconds"),
        })
        log.info("Alternate source rescued %s via %s (%s, confidence %.2f)",
                 spec.driver_id, revised.source_url, revised.connector, confidence)
        return result, monthly

    # ------------------------------------------------------------- internals

    def _min_months_for_success(self) -> int:
        return int(self.cfg.get("run.min_months_for_success", 24))

    def _ladder(self, spec: DriverSpec) -> list[Tier]:
        """Declared tier first, then the remaining tiers in configured order."""
        order = [Tier(t) for t in self.cfg.get("cascade.escalation_order", [1, 2, 3, 4])]
        if self.cfg.get("cascade.attempt_declared_tier_first", True):
            ladder = [spec.declared_tier] + [t for t in order if t != spec.declared_tier]
        else:
            ladder = order
        return ladder

    def _rollup(self, spec: DriverSpec, result: FetchResult) -> tuple[pd.DataFrame, str]:
        """Apply the registry's aggregation rule and clip to the run window. Returns (monthly, detected_frequency)."""
        monthly, detected_frequency = to_monthly(
            result.observations,
            method=spec.rollup_method,
            frequency=spec.native_frequency,
            min_coverage_ratio=float(self.cfg.get("rollup.min_coverage_ratio", 0.6)),
            max_forward_fill_months=int(self.cfg.get("rollup.max_forward_fill_months", 0)),
        )
        if monthly.empty:
            return monthly, detected_frequency

        start = pd.Timestamp(self.cfg.get("run.start_date", "2014-01-01"))
        end = pd.Timestamp(self.cfg.get("run.end_date") or utc_now().date())
        clipped = monthly[(monthly["month"] >= start.normalize().replace(day=1)) &
                          (monthly["month"] <= end)].reset_index(drop=True)
        return clipped, detected_frequency

    def _quality_gate(self, spec: DriverSpec, monthly: pd.DataFrame,
                      detected_frequency: str) -> tuple[bool, list[str], float]:
        """
        Decide whether a monthly series is fit for modelling.

        Deliberately explicit rather than a single score: an analyst reading the
        manifest should see exactly which condition a series failed. Five checks
        run: history, density, completeness, staleness, and frequency. The first
        four fail the gate on any shortfall; frequency only fails it on a heavy
        mismatch (2+ tiers apart on the Daily/Weekly/Monthly/Quarterly/Annual
        ladder, e.g. declared Monthly but actually Annual) — a one-tier gap is
        common rollup noise and stays informational only.

        Returns (passed, notes, coverage_ratio). coverage_ratio is the density
        check's raw fraction (share of months in the rolled-up window that
        carry a value) — persisted by the caller so downstream reporting (the
        success-flag file) never has to recompute it from scratch.
        """
        notes: list[str] = []
        info: list[str] = []
        if monthly.empty:
            return False, ["no monthly observations after rollup"], 0.0

        # 1. History length. Beroe's ST/MT/LT horizons need a real back series.
        # A driver may override the global minimum (spec.min_history_years,
        # registry column of the same name) when its source is legitimately
        # shorter-lived by nature rather than broken — blank/unset behaves
        # exactly as before.
        required = (float(spec.min_history_years) if spec.min_history_years is not None
                   else float(self.cfg.get("run.min_history_years", 8)))
        years = history_years(monthly)
        if years < required:
            notes.append(f"history {years:.1f}y is below the {required:.0f}y minimum")

        # 2. Density. A panel of half-empty months breaks lag construction.
        observed = monthly["value"].notna().mean()
        if observed < 0.8:
            notes.append(f"only {observed:.0%} of months carry a value")

        # 3. Completeness of the months that do have values.
        complete = monthly.loc[monthly["value"].notna(), "is_complete"].mean() if observed else 0
        if complete < 0.7:
            notes.append(f"only {complete:.0%} of populated months meet the coverage threshold")

        # 4. Staleness against the source's own publication lag, with slack.
        latest = monthly.loc[monthly["value"].notna(), "month"].max()
        if pd.notna(latest):
            stale_days = (pd.Timestamp.today() - latest).days
            budget = spec.update_lag_days + 62      # publication lag plus two months
            if stale_days > budget:
                notes.append(f"latest observation {latest:%Y-%m} is {stale_days}d old "
                             f"against a {budget}d budget")

        # 5. Frequency mismatch. A one-tier gap (e.g. Weekly data rolling up
        # looking like Monthly) is ordinary rollup noise and stays
        # informational only, same as before. A gap of two tiers or more
        # (e.g. declared Monthly but the data is actually Annual) is a heavy
        # inconsistency — the series isn't the cadence the registry thinks it
        # is, which can quietly distort a model built assuming monthly
        # granularity, so it now fails the gate like the other four checks.
        if detected_frequency and detected_frequency.strip().lower() != spec.native_frequency.strip().lower():
            mismatch = (f"declared native_frequency='{spec.native_frequency}' vs "
                       f"detected cadence='{detected_frequency}' from observation spacing")
            gap = self._frequency_gap_tiers(spec.native_frequency, detected_frequency)
            if gap is not None and gap >= 2:
                notes.append(f"heavy frequency inconsistency: {mismatch}")
            else:
                info.append(f"[info] {mismatch}")

        return (len(notes) == 0), notes + info, float(observed)

    @staticmethod
    def _frequency_gap_tiers(declared: str, detected: str) -> Optional[int]:
        """
        Distance in tiers between two frequencies on the Daily -> Weekly ->
        Monthly -> Quarterly -> Annual ladder. None when either side isn't on
        that ladder (Event, or an unrecognised value) — a gap can't be
        meaningfully measured then, so it's left informational by the caller.
        """
        ladder = [Frequency.DAILY, Frequency.WEEKLY, Frequency.MONTHLY,
                 Frequency.QUARTERLY, Frequency.ANNUAL]
        try:
            d = ladder.index(str(declared).strip().title())
            t = ladder.index(str(detected).strip().title())
        except ValueError:
            return None
        return abs(d - t)

    def _repair_eurostat_locator(self, spec: DriverSpec,
                                 hint: dict) -> Optional[tuple[FetchResult, pd.DataFrame]]:
        """
        Eurostat-specific self-repair: the failure was structurally diagnosed
        (see connectors.EurostatLocatorError) as a bad dataset code or a bad/
        insufficient filter — not a network fault or a different kind of
        problem. Resolve a correction against Eurostat's own live catalogue
        (bad dataset) or dimension metadata (bad filters), verify it with a
        real fetch through the same quality-gate bar as every other tier, and
        only use it if that verification actually passes.

        A successful hit is recorded in self.eurostat_locator_repair_hits for
        a one-time, end-of-run, single-writer commit into driver_registry.csv
        (see pipeline/eurostat_repair.py), for the same reason
        _search_alternate_source defers its registry write — this can run
        inside a worker thread under the parallel runner. A rejected attempt
        is recorded in self.rejected_eurostat_repair_attempts so it is never
        retried past the attempt cap on a future run.
        """
        max_attempts = int(self.cfg.get("cascade.eurostat_locator_repair_max_attempts_per_driver", 3))
        prior_attempts = self.rejected_eurostat_repairs.get(spec.driver_id, [])
        if len(prior_attempts) >= max_attempts:
            log.info("Eurostat locator repair skipped for %s: already rejected %d time(s) (max %d)",
                     spec.driver_id, len(prior_attempts), max_attempts)
            return None

        def _reject(reason: str, extra: Optional[dict] = None) -> None:
            self.rejected_eurostat_repair_attempts.append({
                "driver_id": spec.driver_id,
                "dataset": hint.get("dataset", ""),
                "filters": hint.get("filters", {}),
                "reason": reason,
                "rejected_at": utc_now().isoformat(timespec="seconds"),
                **(extra or {}),
            })
            log.warning("Eurostat locator repair rejected for %s: %s", spec.driver_id, reason)

        reason = hint.get("reason")
        if reason == "bad_dataset":
            resolution = eurostat_repair.resolve_dataset_code(self.http, self.llm, spec, self.cfg)
        elif reason in ("bad_filters", "insufficient_filters"):
            resolution = eurostat_repair.resolve_filters(self.http, self.llm, spec, hint, self.cfg)
        else:
            return None

        if not resolution:
            _reject("no_resolution")
            return None

        min_confidence = float(self.cfg.get("cascade.eurostat_repair_min_confidence", 0.5))
        if float(resolution.get("confidence", 0)) < min_confidence:
            _reject("low_confidence", {"proposed": resolution})
            return None

        old_locator = spec.endpoint_or_locator
        new_locator = resolution["locator"]

        revised = DriverSpec(**{**spec.__dict__})
        revised.endpoint_or_locator = new_locator
        # unit is deliberately NOT touched — same invariant
        # _search_alternate_source enforces: the resolver selects a
        # coordinate (dataset code or filter value), never a value or unit.

        handler = self.handlers[Tier.DIRECT]   # Eurostat is a keyless Tier-1 open-API connector
        result = handler.fetch(revised)
        if not result.ok:
            _reject(f"fetch_failed:{result.outcome}", {"proposed": resolution})
            return None

        monthly, detected_frequency = self._rollup(revised, result)
        if monthly.empty or monthly["value"].notna().sum() < self._min_months_for_success():
            _reject("quality_gate_failed:insufficient_months", {"proposed": resolution})
            return None

        _passed, notes, coverage_ratio = self._quality_gate(revised, monthly, detected_frequency)
        result.outcome = Outcome.SUCCESS
        result.extra["quality_notes"] = notes
        result.extra["frequency_detected"] = detected_frequency
        result.extra["coverage_ratio"] = coverage_ratio
        result.extra["reclassified"] = True
        result.extra["eurostat_locator_repaired"] = True
        result.extra["serving_spec"] = revised

        self.eurostat_locator_repair_hits.append({
            "driver_id": spec.driver_id,
            "old_locator": old_locator,
            "new_locator": new_locator,
            "reason": reason,
            "confidence": float(resolution.get("confidence", 0)),
            "reasoning": resolution.get("reasoning", ""),
            "repaired_at": utc_now().isoformat(timespec="seconds"),
        })
        log.info("Eurostat locator repair rescued %s: %r -> %r (confidence %.2f)",
                 spec.driver_id, old_locator, new_locator, float(resolution.get("confidence", 0)))
        return result, monthly

    def _retry_after_reclassification(self, spec: DriverSpec):
        """Re-read the landing page, then retry once against the LLM's verdict."""
        try:
            html = self.http.get_text(spec.source_url)
        except Exception as exc:  # noqa: BLE001
            log.warning("Cannot re-read %s for re-classification: %s", spec.source_url, exc)
            return None

        verdict = self.llm.reclassify_tier(spec, html)
        if not verdict or float(verdict.get("confidence", 0)) < 0.6:
            return None

        self.tier_suggestions.append({
            "driver_id": spec.driver_id,
            "declared_tier": int(spec.declared_tier),
            "suggested_tier": verdict.get("tier"),
            "suggested_access_mode": verdict.get("access_mode"),
            "suggested_url": verdict.get("suggested_url", ""),
            "reasoning": verdict.get("reasoning", ""),
            "action": "review and update input/driver_registry.csv",
        })

        suggested_tier = Tier(int(verdict.get("tier", spec.declared_tier)))
        if suggested_tier == spec.declared_tier and not verdict.get("suggested_url"):
            return None

        # Retry against a copy of the spec so the registry stays authoritative.
        revised = DriverSpec(**{**spec.__dict__})
        revised.declared_tier = suggested_tier
        revised.access_mode = verdict.get("access_mode", spec.access_mode)
        if verdict.get("suggested_url"):
            revised.source_url = verdict["suggested_url"]

        handler = self.handlers[suggested_tier]
        result = handler.fetch(revised)
        if not result.ok:
            return None

        monthly, detected_frequency = self._rollup(revised, result)
        # Same guard as the main ladder: real raw observations can still roll
        # up to zero populated months after the run-window clip. That is not
        # a rescue — fall through so the caller keeps escalating (fallback,
        # then HITL) instead of accepting a hollow "success". Same bar as
        # the main ladder's floor: fewer than min_months_for_success populated
        # months isn't a rescue either, just a smaller version of the same
        # problem.
        if monthly.empty or monthly["value"].notna().sum() < self._min_months_for_success():
            return None

        _passed, notes, coverage_ratio = self._quality_gate(revised, monthly, detected_frequency)
        result.outcome = Outcome.SUCCESS
        result.extra["quality_notes"] = notes
        result.extra["frequency_detected"] = detected_frequency
        result.extra["coverage_ratio"] = coverage_ratio
        result.extra["reclassified"] = True
        log.info("Re-classification rescued %s at tier %d", spec.driver_id, int(suggested_tier))
        return result, monthly

    def _check_manual_drop(self, spec: DriverSpec) -> Optional[tuple[FetchResult, pd.DataFrame]]:
        """
        Checks input/manual/<driver_id>.* for every driver, not just gated
        ones (see tiers/base.py::manual_file — Tier4Gated.can_handle() used
        to be the only path that ever reached this check, which gated it
        behind access_mode in (api_key, login, paid_or_restricted)).

        Runs the manual file through the exact same extraction, rollup and
        quality-gate bar as a live fetch, so a manually-dropped file's
        result carries full provenance and is clipped to this run's window
        exactly like anything else. Returns None (falls through to the
        normal tier ladder) if no file is dropped, the file doesn't parse,
        or it rolls up to zero usable months — never a silent partial
        result.
        """
        manual = manual_file(self.cfg, spec)
        if manual is None:
            return None

        handler = self.handlers[Tier.EMBEDDED_FILE]
        result = handler._guard(spec, handler.extract_from_file, manual, spec)
        if not result.ok:
            log.warning("Manual drop %s for %s did not parse cleanly (%s); falling through to "
                       "the normal tier ladder", manual.name, spec.driver_id, result.message)
            return None

        result.extra["source"] = "manual_drop"
        result.raw_artifact_path = str(manual)
        log.info("%s satisfied by manual drop %s", spec.driver_id, manual.name)

        monthly, detected_frequency = self._rollup(spec, result)
        if monthly.empty or monthly["value"].notna().sum() < self._min_months_for_success():
            log.warning("Manual drop %s for %s rolled up to fewer than the required %d populated "
                       "months (run window may not cover it, or the file is genuinely short); "
                       "falling through to the normal tier ladder",
                       manual.name, spec.driver_id, self._min_months_for_success())
            return None

        _passed, notes, coverage_ratio = self._quality_gate(spec, monthly, detected_frequency)
        result.outcome = Outcome.SUCCESS
        result.extra["quality_notes"] = notes
        result.extra["frequency_detected"] = detected_frequency
        result.extra["coverage_ratio"] = coverage_ratio
        return result, monthly

    def _blocked_host(self, spec: DriverSpec) -> Optional[str]:
        """
        Host-level circuit breaker, config-driven (cascade.known_blocked_hosts).

        For a source that's confirmed to reject every request (e.g. a 403
        bot-block that headers alone don't clear), retrying it every single
        run wastes time and risks the block escalating further. This is a
        deliberate, human-curated list — nothing is added to it
        automatically.
        """
        blocked = self.cfg.get("cascade.known_blocked_hosts", []) or []
        if not blocked:
            return None
        host = urlparse(spec.source_url).netloc.lower()
        for entry in blocked:
            entry = str(entry).lower().strip()
            if entry and (host == entry or host.endswith(f".{entry}")):
                return entry
        return None

    def _record_hitl(self, spec: DriverSpec, reason_override: Optional[str] = None,
                     last_failure: Optional[FetchResult] = None,
                     insufficient_months: Optional[tuple[int, int]] = None) -> None:
        """
        Write a precise, actionable task rather than a vague failure.

        Prefers the real outcome of the last live tier attempt (last_failure,
        threaded through from run_driver()'s ladder loop) over guessing a
        reason from static registry fields (credential_keys/access_mode)
        alone. The two used to always agree in practice, but they can
        diverge: a driver with a configured, working credential can still
        fail for an unrelated technical reason (a bad locator, a bot-block,
        a genuine 404 from a renamed series) — the old static-only logic
        told the analyst to go get a key they already had, instead of
        showing them the failure that actually happened. Static inference is
        kept only as a fallback for the rare case no live attempt was made
        at all (last_failure is None).
        """
        drop_path = self.cfg.path("paths.manual_drop", "input/manual") / f"{spec.driver_id}.csv"

        if insufficient_months is not None:
            populated, minimum = insufficient_months
            reason = (f"Real data was found ({populated} populated month(s) after rollup), but that "
                      f"is below the {minimum}-month minimum (run.min_months_for_success) to count "
                      "as a usable success — too short a span to be a meaningful regressor")
            action = (f"Inspect {spec.source_url} manually for a fuller history (a different page, "
                      f"archive, or section may carry more back data). If {populated} month(s) is "
                      f"genuinely all that exists, either lower run.min_months_for_success for this "
                      f"driver or save the series as {drop_path.name} with columns obs_date,value to "
                      "accept it deliberately")
        elif reason_override:
            reason = reason_override
            action = (f"Remove the entry from cascade.known_blocked_hosts in config.yaml once "
                      f"{spec.source_name or spec.source_url} is reachable again, or inspect "
                      f"{spec.source_url} manually and save the series as {drop_path.name} with "
                      "columns obs_date,value")
        elif last_failure is not None and last_failure.outcome == Outcome.BLOCKED_PAID:
            # The connector's own PermissionError message already names the
            # specific licence/provider — same substance as the old static
            # branch, just sourced from what actually happened.
            reason = "Commercial subscription required"
            action = (f"Confirm licence with {spec.source_name}, then either add the API key to "
                      f"config.yaml or export the series and save it as {drop_path.name} with "
                      "columns obs_date,value")
        elif last_failure is not None and last_failure.outcome == Outcome.BLOCKED_CREDENTIALS:
            # Covers free-API-key, portal-login, AND manual-only-by-design
            # connectors (mla_market_info/manual_yaml) in one branch, since
            # the message raised at the actual blocking site already says
            # precisely which of those it is — more reliable than re-
            # deriving it from spec.access_mode.
            reason = last_failure.message or "Credentials required but not configured"
            action = (f"See the reason above for exactly what's needed. If it's a config key, set "
                      f"it in config.yaml; otherwise save the series as {drop_path.name} with "
                      "columns obs_date,value")
        elif last_failure is not None and last_failure.outcome in (
                Outcome.FAILED_PERMANENT, Outcome.FAILED_RETRYABLE):
            # A real attempt was made (with real credentials, where needed)
            # and failed for a diagnosable technical reason — surface it
            # directly rather than falling back to a credential-shaped
            # guess. This is the fix for the confirmed mislabeling: ~40 of
            # 44 drivers previously shown "Free API key required" here had
            # actually run with a working key and failed for an unrelated
            # reason (bad locator, bot-block, renamed series).
            reason = f"Automated extraction failed: {last_failure.message}"
            action = (f"Inspect {spec.source_url} manually against the failure above. If the data "
                      f"is retrievable, correct the tier/locator in the registry; otherwise save "
                      f"the series as {drop_path.name} with columns obs_date,value")
        elif spec.access_mode == "paid_or_restricted":
            reason = "Commercial subscription required"
            action = (f"Confirm licence with {spec.source_name}, then either add the API key to "
                      f"config.yaml or export the series and save it as {drop_path.name} with "
                      "columns obs_date,value")
        elif spec.access_mode == "login":
            reason = "Portal login required"
            action = (f"Register at {spec.source_url}, then set "
                      f"{' and '.join(spec.credential_keys)} in config.yaml under credentials")
        elif spec.credential_keys:
            reason = "Free API key required but not configured"
            action = (f"Request a free key at {spec.source_url} and set "
                      f"credentials.{spec.credential_key} in config.yaml")
        else:
            reason = "All automated extraction tiers failed"
            action = (f"Inspect {spec.source_url} manually. If the data is retrievable, correct the "
                      f"tier and locator in the registry; otherwise save the series as "
                      f"{drop_path.name} with columns obs_date,value")

        self.hitl_tasks.append(HitlTask(
            driver_id=spec.driver_id,
            commodity=spec.commodity,
            region=spec.region,
            driver_name=spec.driver_name,
            reason=reason,
            source_url=spec.source_url,
            required_action=action,
            credential_keys=spec.credential_keys,
            suggested_drop_path=str(drop_path),
        ))
        log.warning("HITL task recorded for %s: %s", spec.driver_id, reason)
