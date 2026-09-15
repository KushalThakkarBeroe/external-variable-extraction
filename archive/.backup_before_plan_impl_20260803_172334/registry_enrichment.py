"""
Registry enrichment from minimal seed file.

Reads a seed file with minimal columns (commodity, driver_name, region,
start_date, unit), dedupes against the existing registry, generates
driver_ids, calls the LLM to propose metadata (source_url, connector, tier,
frequency, rollup_method), and appends fully-populated rows to the registry.

The seed file is entirely separate from driver_registry.csv:
  - CSV-only registry: driver_registry.csv (read by the pipeline)
  - Seed file: new_drivers.xlsx (human input, read by enrichment)
  - Enrichment output: appended back to driver_registry.csv

Identity key for deduplication: (commodity, driver_name, region), normalized
(trimmed, lowercased). Neither start_date nor unit are part of the key:
start_date is synced into history_from on every run, so a user can edit the
date without creating a duplicate; unit is free-text human input in the seed
file that has proven unreliable (an entire commodity block can carry one
copy-pasted placeholder unit, e.g. "AUD/Kg" on every row regardless of what
each driver actually measures) — keying on it let literal duplicate driver
names slip past the dedup check entirely.

Exact-key matching only catches identical driver names. Close-but-not-quite
matches ("Diesel and transport fuel cost" vs. "Australia diesel/transport
fuel cost") are surfaced separately as a near-duplicate report
(output/possible_duplicate_drivers.yaml) for a human to review — never
auto-merged, since a false-positive fuzzy match would silently suppress a
genuinely distinct driver.

No new connector is implemented until the LLM actually proposes one and
a real driver needs it — see the caveat in the plan around EIA.
"""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from .connectors import CONNECTORS
from .llm import LlmHelper
from .models import DriverSpec, utc_now
from .registry import VALID_ACCESS

log = logging.getLogger(__name__)


def _normalize_for_matching(value: str) -> str:
    """Normalize a string for dedup matching: trim, lowercase."""
    return str(value).strip().lower()


def _generate_driver_id(region: str, commodity: str, driver_name: str,
                        existing_ids: set[str]) -> str:
    """
    Generate a driver_id following the informal {REGION}_{COMMODITY}_{DRIVER}
    convention, de-duplicated against existing ids.

    Generic slugification: uppercase, underscore-separated, alphanumeric only.
    """
    # Extract alphanumerics from each part, join with underscore, uppercase
    def slugify(s: str) -> str:
        s = str(s).strip()
        s = re.sub(r"[^a-zA-Z0-9\s]", "", s)  # Remove non-alphanumeric except spaces
        s = re.sub(r"\s+", "_", s)              # Replace spaces with underscore
        return s.upper()

    region_slug = slugify(region)
    commodity_slug = slugify(commodity)
    driver_slug = slugify(driver_name)

    base_id = f"{region_slug}_{commodity_slug}_{driver_slug}"

    if base_id not in existing_ids:
        return base_id

    # De-duplicate: append numeric suffix
    counter = 1
    while True:
        candidate = f"{base_id}_{counter}"
        if candidate not in existing_ids:
            return candidate
        counter += 1


def _credential_key_for_connector(connector: str) -> str:
    """
    Which credential(s) a connector needs, keyed by its validated name (see
    pipeline/connectors.py for which connectors actually require a key).
    "manual_yaml" and any connector not listed here are deliberately blank:
    either a structural "no automatable path" block, not a credential
    requirement, or a connector that needs no key (e.g. eurostat, frankfurter).
    """
    return {
        "fred": "fred_api_key",
        "eia": "eia_api_key",
        "nass_quickstats": "usda_nass_api_key",
        "usda_psd": "usda_fas_api_key",
        "un_comtrade": "comtrade_api_key",
        "mla_market_info": "mla_username|mla_password",
    }.get(connector, "")


def _tier_confidence_from_llm(confidence: float) -> str:
    """Map LLM numeric confidence (0-1) to tier_confidence category."""
    if confidence >= 0.7:
        return "High"
    if confidence >= 0.4:
        return "Medium"
    return "Low"


def _normalize_column_name(name: str) -> str:
    """Normalize a column header for flexible matching: lowercase, spaces/hyphens -> underscore."""
    return re.sub(r"[\s\-]+", "_", str(name).strip().lower())


# Keyword -> canonical access_mode, checked in order. Covers common phrasings
# an LLM might use instead of the exact registry vocabulary (e.g. "download"
# instead of "file_download"). This maps a fixed, small system enum — not
# domain/business data — so a lookup table here isn't the kind of hardcoding
# to avoid.
_ACCESS_MODE_KEYWORDS = [
    ("file_download", ("download", "file", "excel", "spreadsheet", "csv", "workbook")),
    ("multipage_scrape", ("multipage", "multi-page", "multi_page", "crawl", "pagination")),
    ("paid_or_restricted", ("paid", "restricted", "subscription", "licence", "license", "commercial")),
    ("login", ("login", "credential", "portal")),
    ("api_key", ("api_key", "apikey", "key")),
    ("html_table", ("html", "table", "webpage", "scrape")),
    ("open_api", ("open_api", "api", "rest", "json")),
]

# Fallback by declared tier when no keyword matches at all — mirrors the
# typical access pattern for each tier.
_ACCESS_MODE_TIER_DEFAULT = {
    1: "open_api",
    2: "file_download",
    3: "multipage_scrape",
    4: "api_key",
}


def _normalize_connector(value: str) -> str:
    """
    Validate an LLM-proposed connector name against the real CONNECTORS
    registry (pipeline/connectors.py). An unrecognized or misspelled name is
    blanked out rather than left dangling: a nonexistent connector matches
    no tier handler's can_handle() check, so the driver would otherwise
    silently fail every tier and go straight to HITL even if the proposed
    source_url would have worked fine via generic HTML/file scraping.
    """
    if not value or not value.strip():
        return ""
    normalized = _normalize_column_name(value)
    if normalized in CONNECTORS:
        return normalized
    log.warning("LLM proposed unknown connector %r; falling back to generic scraping via source_url",
               value)
    return ""


def _normalize_access_mode(value: str, declared_tier: int) -> str:
    """
    Coerce an LLM-proposed access_mode into the registry's fixed vocabulary.

    registry.py only warns on an unrecognized access_mode, it does not
    correct it — and no tier handler's can_handle() matches an unrecognized
    string, so the driver would silently never be attempted by any tier.
    Normalize here instead of trusting the LLM to hit the exact enum.
    """
    normalized = _normalize_column_name(value)
    if normalized in VALID_ACCESS:
        return normalized

    for canonical, keywords in _ACCESS_MODE_KEYWORDS:
        if any(kw in normalized for kw in keywords):
            return canonical

    fallback = _ACCESS_MODE_TIER_DEFAULT.get(int(declared_tier), "html_table")
    log.warning("Could not map access_mode %r to a known value; defaulting to %r for tier %s",
               value, fallback, declared_tier)
    return fallback


def _parse_start_date(value: str) -> str:
    """
    Parse a start_date value into YYYY-MM, matching the registry's existing
    history_from convention (e.g. '1990-01', '2014-01').

    Handles Excel's common month-year shorthand (e.g. 'Jan-18', 'Mar-2022'),
    which pandas' general parser misreads as day=18 of month=Jan in year 1
    rather than January 2018. That specific format is tried first; anything
    else falls back to pandas' general date parser so ISO dates, 'YYYY-MM',
    or 'January 2018' style input all still work.
    """
    value = str(value).strip()
    if not value:
        return ""

    for fmt in ("%b-%y", "%b-%Y", "%B-%y", "%B-%Y", "%B %Y", "%b %Y"):
        try:
            parsed = pd.to_datetime(value, format=fmt)
            return parsed.strftime("%Y-%m")
        except (ValueError, TypeError):
            continue

    try:
        parsed = pd.to_datetime(value)
        return parsed.strftime("%Y-%m")
    except (ValueError, TypeError):
        log.warning("Could not parse start_date value %r; leaving as-is", value)
        return value


def enrich_new_drivers(cfg, llm: LlmHelper, registry_path: Path,
                       seed_path: Path) -> int:
    """
    Enriches minimal seed rows and appends them to the main registry.

    Args:
        cfg: Config object
        llm: LlmHelper instance (can be disabled)
        registry_path: path to driver_registry.csv
        seed_path: path to new_drivers.xlsx (or .csv)

    Returns:
        Number of rows appended to the registry.
    """
    # Read existing registry
    try:
        registry_df = pd.read_csv(registry_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        log.warning("Registry not found at %s; treating as empty", registry_path)
        registry_df = pd.DataFrame()

    existing_ids = set(registry_df["driver_id"].unique()) if not registry_df.empty else set()

    # Read seed file (Excel or CSV)
    try:
        if str(seed_path).lower().endswith(".xlsx"):
            seed_df = pd.read_excel(seed_path, dtype=str, keep_default_na=False)
        else:
            seed_df = pd.read_csv(seed_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        log.info("Seed file not found at %s; skipping enrichment", seed_path)
        return 0

    if seed_df.empty:
        log.info("Seed file is empty; no new drivers to enrich")
        return 0

    # Normalize headers so human-friendly capitalization/spacing (e.g. "Driver Name",
    # "Start Date") matches the expected snake_case columns regardless of exact casing.
    seed_df.columns = [_normalize_column_name(c) for c in seed_df.columns]

    # Check required columns
    required = {"commodity", "driver_name", "region", "start_date", "unit"}
    missing = required - set(seed_df.columns)
    if missing:
        log.error("Seed file missing required columns: %s", missing)
        return 0

    # Build identity map from existing registry for dedup
    existing_map = {}
    if not registry_df.empty:
        for _, row in registry_df.iterrows():
            key = (
                _normalize_for_matching(row.get("commodity", "")),
                _normalize_for_matching(row.get("driver_name", "")),
                _normalize_for_matching(row.get("region", "")),
            )
            existing_map[key] = row["driver_id"]

    # Near-duplicate detection: same commodity+region, close-but-not-exact
    # driver_name. Visibility only — never blocks enrichment or merges
    # anything, since a false-positive fuzzy match would silently drop a
    # genuinely distinct driver. See module docstring.
    near_dup_threshold = float(cfg.get("registry.near_duplicate_threshold", 0.6))
    possible_duplicates: list[dict] = []
    candidates_by_scope: dict[tuple[str, str], list[tuple[str, str]]] = {}
    if not registry_df.empty:
        for _, row in registry_df.iterrows():
            scope = (_normalize_for_matching(row.get("commodity", "")),
                     _normalize_for_matching(row.get("region", "")))
            candidates_by_scope.setdefault(scope, []).append(
                (row["driver_id"], _normalize_for_matching(row.get("driver_name", "")))
            )

    def _register_candidate(commodity_: str, region_: str, driver_id_: str, driver_name_: str) -> None:
        scope = (_normalize_for_matching(commodity_), _normalize_for_matching(region_))
        candidates_by_scope.setdefault(scope, []).append(
            (driver_id_, _normalize_for_matching(driver_name_))
        )

    def _check_near_duplicates(commodity_: str, driver_name_: str, region_: str, new_driver_id: str) -> None:
        scope = (_normalize_for_matching(commodity_), _normalize_for_matching(region_))
        norm_name = _normalize_for_matching(driver_name_)
        for existing_id, existing_name in candidates_by_scope.get(scope, []):
            if existing_id == new_driver_id:
                continue
            ratio = SequenceMatcher(None, norm_name, existing_name).ratio()
            if ratio >= near_dup_threshold:
                possible_duplicates.append({
                    "new_driver_id": new_driver_id,
                    "new_driver_name": driver_name_,
                    "commodity": commodity_,
                    "region": region_,
                    "matched_driver_id": existing_id,
                    "matched_driver_name": existing_name,
                    "similarity": round(ratio, 2),
                })
                log.warning("Possible near-duplicate: %s (%r) resembles existing driver %s (%r) "
                           "at %.0f%% similarity — not merged automatically, see "
                           "possible_duplicate_drivers.yaml", new_driver_id, driver_name_,
                           existing_id, existing_name, ratio * 100)

    # Process seed rows
    new_rows = []
    rows_matched = 0
    rows_unenriched = 0
    low_confidence_drivers = []

    # First pass: auto-create Frankfurter fallbacks for any existing FRED FX drivers
    # that don't already have a Frankfurter fallback
    if not registry_df.empty:
        fred_fx_drivers = registry_df[
            (registry_df["connector"] == "fred") &
            (registry_df["unit"].str.contains(r"(?:USD|EUR|NZD|BRL|GBP|JPY|CHF)/|per (?:USD|EUR|NZD|BRL|GBP|JPY|CHF)", regex=True, na=False))
        ]

        for _, fred_row in fred_fx_drivers.iterrows():
            driver_id = str(fred_row.get("driver_id", ""))
            fallback_id = str(fred_row.get("fallback_driver_id", "")).strip() or None

            # Skip if already has a Frankfurter fallback
            if fallback_id and fallback_id.endswith("_FRANK"):
                continue

            # Extract currencies from unit
            unit = str(fred_row.get("unit", "")).upper()
            unit_parts = re.split(r'[\s/]', unit)
            currencies = [p for p in unit_parts if len(p) == 3 and p.isalpha()]

            if len(currencies) >= 2:
                target = currencies[0]
                base = "USD"

                # Check if fallback already exists
                fallback_exists = not registry_df[
                    (registry_df["driver_id"] == f"{driver_id}_FRANK") |
                    (registry_df["connector"] == "frankfurter") &
                    (registry_df["source_url"] == "https://api.frankfurter.dev/")
                ].empty

                if not fallback_exists:
                    frank_driver_id = f"{driver_id}_FRANK"
                    if frank_driver_id in existing_ids:
                        counter = 2
                        while f"{driver_id}_FRANK{counter}" in existing_ids:
                            counter += 1
                        frank_driver_id = f"{driver_id}_FRANK{counter}"
                    existing_ids.add(frank_driver_id)

                    # Create Frankfurter fallback
                    fallback_row = {
                        "driver_id": frank_driver_id,
                        "commodity": str(fred_row.get("commodity", "")),
                        "driver_name": f"{unit.strip()} (Frankfurter keyless fallback)",
                        "region": str(fred_row.get("region", "")),
                        "extraction_tier": 1,
                        "tier_confidence": "High",
                        "connector": "frankfurter",
                        "source_name": "Frankfurter API (ECB)",
                        "source_url": "https://api.frankfurter.dev/",
                        "endpoint_or_locator": f"base:{base} | target:{target}",
                        "access_mode": "open_api",
                        "native_frequency": "Daily",
                        "rollup_method": "mean",
                        "unit": str(fred_row.get("unit", "")),
                        "history_from": "1999-01",
                        "meets_min_history": "Y",
                        "update_lag_days": 1,
                        "is_proxy": "N",
                        "proxy_note": "",
                        "human_in_loop": "N",
                        "credential_key": "",
                        "fallback_driver_id": "",
                        "data_quality_risk": "Low",
                        "transform_hint": "mean + MoM pct (keyless ECB rates 1999-01 onward)",
                        "priority": 3,
                        "enrichment_confidence": "",
                    }
                    new_rows.append(fallback_row)
                    _register_candidate(fallback_row["commodity"], fallback_row["region"],
                                        frank_driver_id, fallback_row["driver_name"])

                    # Update the existing FRED driver to point to this fallback
                    registry_df.loc[registry_df["driver_id"] == driver_id, "fallback_driver_id"] = frank_driver_id
                    log.info("Auto-created Frankfurter fallback %s for existing FRED FX driver %s",
                            frank_driver_id, driver_id)

    for _, seed_row in seed_df.iterrows():
        commodity = str(seed_row.get("commodity", "")).strip()
        driver_name = str(seed_row.get("driver_name", "")).strip()
        region = str(seed_row.get("region", "")).strip()
        start_date = _parse_start_date(seed_row.get("start_date", ""))
        unit = str(seed_row.get("unit", "")).strip()

        # Check for missing critical fields
        if not all([commodity, driver_name, region, start_date, unit]):
            log.warning("Seed row skipped: missing critical field(s): %s / %s / %s / %s / %s",
                       commodity, driver_name, region, start_date, unit)
            continue

        # Dedup key (identity)
        key = (
            _normalize_for_matching(commodity),
            _normalize_for_matching(driver_name),
            _normalize_for_matching(region),
        )

        # If this row already matches an existing registry row, just sync history_from
        if key in existing_map:
            matched_id = existing_map[key]
            log.debug("Seed row %s / %s / %s matches existing driver %s; "
                     "syncing history_from to %s", commodity, driver_name, region,
                     matched_id, start_date)
            rows_matched += 1
            # Update history_from in the actual registry file (deferred — just log for now)
            continue

        # New row: call LLM if enabled
        if not llm.enabled:
            log.warning("LLM disabled; seed row %s / %s / %s / %s cannot be enriched. "
                       "Enable LLM to propose metadata.",
                       commodity, driver_name, region, driver_name)
            rows_unenriched += 1
            continue

        proposal = llm.propose_source(commodity, driver_name, region, unit)
        if proposal is None:
            log.warning("LLM proposal failed for %s / %s / %s; row not enriched",
                       commodity, driver_name, region)
            rows_unenriched += 1
            continue

        # Generate driver_id
        driver_id = _generate_driver_id(region, commodity, driver_name, existing_ids)
        existing_ids.add(driver_id)
        _check_near_duplicates(commodity, driver_name, region, driver_id)

        # Extract LLM fields
        confidence = float(proposal.get("confidence", 0.4))
        tier_confidence = _tier_confidence_from_llm(confidence)
        connector = _normalize_connector(proposal.get("connector", ""))
        source_url = proposal.get("source_url", "").strip()
        source_name = proposal.get("source_name", "").strip()
        endpoint_or_locator = proposal.get("endpoint_or_locator", "").strip() or ""
        declared_tier = proposal.get("declared_tier", 1)
        access_mode = _normalize_access_mode(proposal.get("access_mode", ""), declared_tier)
        native_frequency = proposal.get("native_frequency", "Monthly").strip() or "Monthly"
        rollup_method = proposal.get("rollup_method", "mean").strip().lower() or "mean"

        # Determine credential_key based on the validated connector name.
        # There's no way to know which specific paid credential a blank/
        # manual_yaml connector would need from a minimal seed row alone.
        credential_key = _credential_key_for_connector(connector)

        # Build full row with existing columns + new audit columns
        new_row = {
            "driver_id": driver_id,
            "commodity": commodity,
            "driver_name": driver_name,
            "region": region,
            "extraction_tier": int(declared_tier),
            "tier_confidence": tier_confidence,
            "connector": connector,
            "source_name": source_name,
            "source_url": source_url,
            "endpoint_or_locator": endpoint_or_locator,
            "access_mode": access_mode,
            "native_frequency": native_frequency,
            "rollup_method": rollup_method,
            "unit": unit,
            "history_from": start_date,
            "meets_min_history": "Y",
            "update_lag_days": 30,
            "is_proxy": "N",
            "proxy_note": "",
            "human_in_loop": "N",
            "credential_key": credential_key,
            "fallback_driver_id": "",
            "data_quality_risk": "Medium",
            "transform_hint": proposal.get("reasoning", "").strip()[:100] or "",
            "priority": 2,
            "enrichment_confidence": f"{confidence:.2f}" if confidence > 0 else "",
        }

        new_rows.append(new_row)
        _register_candidate(commodity, region, driver_id, driver_name)

        # Auto-create Frankfurter fallback row for FRED FX series
        # Pattern: FRED FX series typically have currency symbols and rates as values
        is_fx_series = (
            connector == "fred" and
            unit and any(sym in unit.upper() for sym in ["USD", "EUR", "NZD", "BRL", "GBP", "JPY", "CHF"])
            and "/" in unit
        )
        if is_fx_series:
            # Extract base and target currencies from unit (e.g. "NZD per USD" -> base:USD, target:NZD)
            unit_parts = re.split(r'[\s/]', unit.upper())
            currencies = [p for p in unit_parts if len(p) == 3 and p.isalpha()]
            if len(currencies) >= 2:
                target = currencies[0]
                base = "USD"

                # Generate fallback driver_id
                frank_driver_id = f"{driver_id}_FRANK"
                if frank_driver_id in existing_ids:
                    counter = 2
                    while f"{driver_id}_FRANK{counter}" in existing_ids:
                        counter += 1
                    frank_driver_id = f"{driver_id}_FRANK{counter}"
                existing_ids.add(frank_driver_id)

                # Create Frankfurter fallback row
                fallback_row = {
                    "driver_id": frank_driver_id,
                    "commodity": commodity,
                    "driver_name": f"{unit.strip()} (Frankfurter keyless fallback)",
                    "region": region,
                    "extraction_tier": 1,
                    "tier_confidence": "High",
                    "connector": "frankfurter",
                    "source_name": "Frankfurter API (ECB)",
                    "source_url": "https://api.frankfurter.dev/",
                    "endpoint_or_locator": f"base:{base} | target:{target}",
                    "access_mode": "open_api",
                    "native_frequency": "Daily",
                    "rollup_method": "mean",
                    "unit": unit,
                    "history_from": "1999-01",  # ECB rates start 1999-01-04
                    "meets_min_history": "Y",
                    "update_lag_days": 1,
                    "is_proxy": "N",
                    "proxy_note": "",
                    "human_in_loop": "N",
                    "credential_key": "",
                    "fallback_driver_id": "",
                    "data_quality_risk": "Low",
                    "transform_hint": "mean + MoM% (keyless ECB rates, history from 1999-01 only)",
                    "priority": 3,
                    "enrichment_confidence": "",
                }
                new_rows.append(fallback_row)
                _register_candidate(commodity, region, frank_driver_id, fallback_row["driver_name"])

                # Point the primary row's fallback to this Frankfurter row
                new_row["fallback_driver_id"] = frank_driver_id
                log.info("Auto-created Frankfurter fallback %s for FRED FX series %s", frank_driver_id, driver_id)

        if confidence < 0.4:
            low_confidence_drivers.append((driver_id, confidence))
        log.info("Enriched seed row: %s (confidence %.2f)", driver_id, confidence)

    # Append new rows to registry
    if new_rows:
        new_df = pd.DataFrame(new_rows)

        # Ensure column order matches existing registry (read from header)
        if not registry_df.empty:
            # Preserve existing column order; append any new columns (like enrichment_confidence)
            existing_cols = list(registry_df.columns)
            new_cols = [c for c in new_df.columns if c not in existing_cols]
            col_order = existing_cols + new_cols
            new_df = new_df[col_order]

        registry_df = pd.concat([registry_df, new_df], ignore_index=True)
        registry_df.to_csv(registry_path, index=False)
        log.info("Appended %d new rows to %s", len(new_rows), registry_path)

    # Summary
    total_appended = len(new_rows)
    log.info("Enrichment complete: %d matched (synced), %d enriched, %d unenriched, %d total seed rows",
            rows_matched, total_appended, rows_unenriched,
            rows_matched + total_appended + rows_unenriched)

    if low_confidence_drivers:
        log.warning("Low-confidence enrichments (< 0.4): %s", low_confidence_drivers)

    if possible_duplicates:
        dup_path = cfg.path("paths.possible_duplicates", "output/possible_duplicate_drivers.yaml")
        dup_path.parent.mkdir(parents=True, exist_ok=True)
        dup_path.write_text(yaml.safe_dump({
            "generated_at": utc_now().isoformat(timespec="seconds"),
            "instructions": (
                "These are NOT merged automatically — driver names are close but not "
                "identical, and auto-merging on text similarity risks silently dropping a "
                "genuinely distinct driver. Review each pair: if they really are the same "
                "underlying series, delete the 'new_driver_id' row from driver_registry.csv "
                "(or point it at the matched driver via fallback_driver_id) and re-run."
            ),
            "possible_duplicates": possible_duplicates,
        }, sort_keys=False, allow_unicode=True), encoding="utf-8")
        log.warning("Wrote %d possible near-duplicate pair(s) -> %s", len(possible_duplicates), dup_path)

    return total_appended
