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

from .models import DriverSpec, FetchResult, HitlTask, Outcome, Tier, utc_now
from .registry_enrichment import _normalize_access_mode, _normalize_connector
from .rollup import history_years, to_monthly
from .tiers.tier1_direct import Tier1Direct
from .tiers.tier2_files import Tier2EmbeddedFile
from .tiers.tier3_multipage import Tier3MultiPage
from .tiers.tier4_gated import Tier4Gated

log = logging.getLogger(__name__)


class CascadeOrchestrator:
    def __init__(self, cfg, http, connectors, llm=None,
                rejected_sources: Optional[dict[str, list[dict]]] = None):
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
                    if result.outcome in (Outcome.BLOCKED_PAID, Outcome.BLOCKED_CREDENTIALS):
                        if spec.connector:
                            blocked_connectors.add(spec.connector)
                        break                       # retrying will not find a key
                    if result.outcome == Outcome.FAILED_PERMANENT:
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
                if best is None:
                    best, best_monthly = result, monthly
                break

        # ---- every tier exhausted without a clean pass ----

        # A "best" floor is only worth keeping if it actually has usable
        # monthly data. A tier can come back with real raw observations that
        # still roll up (after the run-window clip) to zero populated
        # months — bytes in, nothing usable out. That must not count as a
        # floor: it must not block escalation, and must never be handed back
        # as a fabricated SUCCESS (see the module docstring).
        best_has_data = (
            best is not None and not best_monthly.empty
            and best_monthly["value"].notna().sum() > 0
        )

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
            self._record_hitl(spec)
            message = "All tiers exhausted; recorded as a human-in-the-loop task"
            if best is not None:
                message = ("All tiers exhausted (best attempt returned 0 usable months after "
                          "rollup); recorded as a human-in-the-loop task")
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
        if monthly.empty or monthly["value"].notna().sum() == 0:
            _reject("quality_gate_failed:no_usable_months", proposal)
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
        manifest should see exactly which condition a series failed. Also surfaces
        when detected frequency (from data spacing) differs from declared frequency.

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
        required = float(self.cfg.get("run.min_history_years", 8))
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

        # 5. Frequency mismatch (informational, does not fail the gate).
        if detected_frequency and detected_frequency.strip().lower() != spec.native_frequency.strip().lower():
            info.append(f"[info] declared native_frequency='{spec.native_frequency}' vs "
                       f"detected cadence='{detected_frequency}' from observation spacing")

        return (len(notes) == 0), notes + info, float(observed)

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
        # then HITL) instead of accepting a hollow "success".
        if monthly.empty or monthly["value"].notna().sum() == 0:
            return None

        _passed, notes, coverage_ratio = self._quality_gate(revised, monthly, detected_frequency)
        result.outcome = Outcome.SUCCESS
        result.extra["quality_notes"] = notes
        result.extra["frequency_detected"] = detected_frequency
        result.extra["coverage_ratio"] = coverage_ratio
        result.extra["reclassified"] = True
        log.info("Re-classification rescued %s at tier %d", spec.driver_id, int(suggested_tier))
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

    def _record_hitl(self, spec: DriverSpec, reason_override: Optional[str] = None) -> None:
        """Write a precise, actionable task rather than a vague failure."""
        drop_path = self.cfg.path("paths.manual_drop", "input/manual") / f"{spec.driver_id}.csv"

        if reason_override:
            reason = reason_override
            action = (f"Remove the entry from cascade.known_blocked_hosts in config.yaml once "
                      f"{spec.source_name or spec.source_url} is reachable again, or inspect "
                      f"{spec.source_url} manually and save the series as {drop_path.name} with "
                      "columns obs_date,value")
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
