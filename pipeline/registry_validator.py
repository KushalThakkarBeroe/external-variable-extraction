"""
Pre-flight registry check.

Scans driver_registry.csv for rows whose connector requires a structured
locator (the same key:value contract documented in llm.py's propose_source
prompt and enforced by DriverSpec.locator_part) but is missing the required
piece. This is exactly the class of bug that otherwise surfaces only after
the fetch cascade burns through every tier for that driver: the connector
itself is fine, the registry row is incomplete. Catching it here costs a
CSV read, not 90 minutes of a wasted run.

Read-only: it only reports, never edits the registry.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# connector -> required pipe-delimited locator prefixes, e.g. "series:<name>".
# Connectors not listed either take no structured locator (a bare code, like
# eia), have no strictly-required part (aip_tgp's sheet/cities both default),
# or — like bcb_sgs — have their own tolerant fallback parser in addition to
# the colon format, so a strict check here would false-positive on a row
# that actually works fine.
_REQUIRED_LOCATOR_PARTS: dict[str, list[str]] = {
    "frankfurter": ["target"],
    "worldbank_pinksheet": ["series"],
    "ibge_sidra": ["table"],
    "eurostat": ["dataset"],
    "fred": ["series_id"],
    "usda_psd": ["commodity", "attribute"],
}

# Connectors that instead expect semicolon-separated "key=value" filters
# directly in the locator (not the pipe/colon format above).
_REQUIRED_FILTER_CONNECTORS = {"nass_quickstats", "un_comtrade"}

# For connectors using the pipe/colon "key:value" locator format, the exact
# set of prefixes the connector itself reads (see connectors.py). A locator
# fragment using any OTHER key is not an error the connector will ever
# raise — it's silently ignored and the connector falls back to its own
# default, so the row fails downstream (or worse, "succeeds" against the
# wrong series) with no clue the locator was even read. This is exactly
# the bug class that hid AU_PLT_TRADE_FB's "commodityCode="/"attribute="
# (instead of "commodity:"/"attribute:") until a live fetch burned an
# attempt to discover it.
_RECOGNIZED_LOCATOR_KEYS: dict[str, set[str]] = {
    "frankfurter": {"target", "base"},
    "worldbank_pinksheet": {"series", "sheet", "file"},
    "ibge_sidra": {"table", "variable"},
    "eurostat": {"dataset", "filters"},
    "fred": {"series_id"},
    "usda_psd": {"commodity", "attribute", "country"},
    "bis_fx": {"currency", "freq", "ref_area"},
}

# Same idea for filter-style ("key=value") connectors, but ONLY where the
# connector reads a fixed, closed set of keys. nass_quickstats is
# deliberately excluded: every key in its locator is passed straight
# through as a live NASS API query parameter (see connectors.py), so there
# is no fixed vocabulary to check against — an unrecognized key there is
# the analyst's intentional NASS filter, not a typo the connector drops.
_RECOGNIZED_FILTER_KEYS: dict[str, set[str]] = {
    "un_comtrade": {"reporterCode", "flowCode", "cmdCode", "partnerCode"},
}

# Locator parts expected to be a real numeric source code, where a
# descriptive word ("Palm Oil", "Ending Stocks", "Europe") is a strong sign
# the row was never resolved against the source's own catalogue. Warning
# only — never auto-"fixed"; every correction still requires a live lookup
# (see Workstream 3's non-regression rule).
_NUMERIC_LOCATOR_PARTS: dict[str, list[str]] = {
    "ibge_sidra": ["table", "variable"],
    "usda_psd": ["commodity", "attribute"],
}
_NUMERIC_FILTER_KEYS: dict[str, list[str]] = {
    "un_comtrade": ["cmdCode"],   # reporterCode legitimately allows the literal "all"
}
_NUMERIC_EXCEPTIONS = {"all", "total", "world"}


def _has_locator_part(locator: str, prefix: str) -> bool:
    return any(
        chunk.strip().lower().startswith(f"{prefix.lower()}:")
        for chunk in locator.split("|")
    )


def _locator_parts(locator: str) -> dict[str, str]:
    """Parses a pipe-delimited 'key:value' locator into {key.lower(): value}."""
    parts: dict[str, str] = {}
    for chunk in locator.split("|"):
        chunk = chunk.strip()
        if ":" in chunk:
            key, _, value = chunk.partition(":")
            parts[key.strip().lower()] = value.strip()
    return parts


def _filter_parts(locator: str) -> dict[str, str]:
    """
    Parses a ';'/'|'/'&'-delimited 'key=value' locator — the same tolerant
    separators the filter-style connectors themselves accept (see
    connectors.py's un_comtrade/nass_quickstats parsing).
    """
    parts: dict[str, str] = {}
    for chunk in locator.replace("|", ";").replace("&", ";").split(";"):
        if "=" in chunk:
            key, _, value = chunk.partition("=")
            parts[key.strip()] = value.strip()
    return parts


def _looks_non_numeric(value: str) -> bool:
    """
    Heuristic only, warning-grade: several connectors legitimately accept a
    comma-separated list of numeric codes in one field (e.g. Comtrade
    cmdCode='0201,0202' for two related HS codes), so each comma-separated
    piece is checked individually rather than the whole string at once.
    """
    v = value.strip()
    if not v or v.lower() in _NUMERIC_EXCEPTIONS:
        return False
    pieces = [p.strip() for p in v.split(",") if p.strip()]
    if not pieces:
        return False
    return not all(p.isdigit() or p.lower() in _NUMERIC_EXCEPTIONS for p in pieces)


def validate_registry(registry_path: Path) -> list[str]:
    """Returns one human-readable warning per malformed row; empty if clean."""
    warnings: list[str] = []
    try:
        df = pd.read_csv(registry_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        return warnings

    for _, row in df.iterrows():
        connector = str(row.get("connector", "")).strip()
        locator = str(row.get("endpoint_or_locator", "")).strip()
        driver_id = row.get("driver_id", "?")

        if connector in _REQUIRED_LOCATOR_PARTS:
            missing = [p for p in _REQUIRED_LOCATOR_PARTS[connector] if not _has_locator_part(locator, p)]
            if missing:
                warnings.append(
                    f"{driver_id}: connector '{connector}' is missing "
                    f"{'/'.join(p + ':' for p in missing)} in the locator (got: {locator!r})"
                )
        elif connector in _REQUIRED_FILTER_CONNECTORS and "=" not in locator:
            warnings.append(
                f"{driver_id}: connector '{connector}' expects 'key=value' filters "
                f"in the locator, found none (got: {locator!r})"
            )

        recognized = _RECOGNIZED_LOCATOR_KEYS.get(connector)
        if recognized is not None and locator:
            parts = _locator_parts(locator)
            unrecognized = sorted(set(parts) - recognized)
            if unrecognized:
                warnings.append(
                    f"{driver_id}: connector '{connector}' does not read locator key(s) "
                    f"{unrecognized} — silently ignored; the connector falls back to its own "
                    f"default instead (got: {locator!r})"
                )
            for part_name in _NUMERIC_LOCATOR_PARTS.get(connector, []):
                value = parts.get(part_name)
                if value and _looks_non_numeric(value):
                    warnings.append(
                        f"{driver_id}: connector '{connector}' {part_name}:{value!r} looks like "
                        f"descriptive text rather than a real source code — verify against the "
                        f"source's own catalogue before trusting this row"
                    )

        recognized_filter = _RECOGNIZED_FILTER_KEYS.get(connector)
        if recognized_filter is not None and locator:
            fparts = _filter_parts(locator)
            unrecognized_filter = sorted(set(fparts) - recognized_filter)
            if unrecognized_filter:
                warnings.append(
                    f"{driver_id}: connector '{connector}' does not read filter key(s) "
                    f"{unrecognized_filter} — silently ignored; the connector falls back to its "
                    f"own default instead (got: {locator!r})"
                )
            for key_name in _NUMERIC_FILTER_KEYS.get(connector, []):
                value = fparts.get(key_name)
                if value and _looks_non_numeric(value):
                    warnings.append(
                        f"{driver_id}: connector '{connector}' {key_name}={value!r} looks like "
                        f"descriptive text rather than a real source code — verify against the "
                        f"source's own catalogue before trusting this row"
                    )

    return warnings


def log_registry_warnings(registry_path: Path) -> int:
    """Logs every warning found and returns the count, for a run-start summary line."""
    warnings = validate_registry(registry_path)
    for warning in warnings:
        log.warning("[registry-check] %s", warning)
    if warnings:
        log.warning("[registry-check] %d row(s) with an incomplete locator — see warnings above; "
                   "these will fail immediately on their declared tier", len(warnings))
    return len(warnings)
