"""
Persistence for the LLM-discovered alternate-source escalation step (see
CascadeOrchestrator._search_alternate_source in pipeline/cascade.py).

Two things get written back to disk once per run, both AFTER the fetch loop
finishes and only ever from the single main thread — never mid-run from a
worker thread, since driver_registry.csv is a shared file the parallel
runner cannot safely write to concurrently:

  1. Verified alternate sources -> new rows appended to driver_registry.csv,
     wired in as the fallback at the tail of the driver's existing fallback
     chain. Never overwrites an existing (human-configured) fallback_driver_id.

  2. Rejected proposals -> output/rejected_alternate_sources.yaml, which
     ACCUMULATES across runs (read-merge-write, unlike the other run
     artifacts that get overwritten fresh each time) so a candidate that
     already failed once is never silently re-proposed and re-fetched on
     every subsequent run.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .atomic_io import write_atomic
from .models import utc_now
from .registry_enrichment import _credential_key_for_connector, _tier_confidence_from_llm

log = logging.getLogger(__name__)


# --------------------------------------------------------------- registry commit

def commit_alternate_sources(cfg, registry_path: Path, hits: list[dict[str, Any]]) -> int:
    """
    Appends one new registry row per verified alternate-source hit, wired in
    as the fallback_driver_id at the tail of the primary driver's existing
    fallback chain. Skips (with a warning, never an overwrite) any hit whose
    chain tail already has a fallback_driver_id set, or whose chain tail no
    longer exists in the registry.
    """
    if not hits:
        return 0

    try:
        registry_df = pd.read_csv(registry_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        log.warning("Registry not found at %s; cannot commit alternate sources", registry_path)
        return 0

    existing_ids = set(registry_df["driver_id"].unique())
    new_rows: list[dict[str, Any]] = []

    for hit in hits:
        tail_id = hit["chain_tail_id"]
        tail_mask = registry_df["driver_id"] == tail_id
        if not tail_mask.any():
            log.warning("Alternate-source commit skipped for %s: chain tail %s no longer in registry",
                       hit["driver_id"], tail_id)
            continue

        existing_fallback = str(registry_df.loc[tail_mask, "fallback_driver_id"].iloc[0]).strip()
        if existing_fallback:
            log.warning("Alternate-source commit skipped for %s: chain tail %s already has "
                       "fallback_driver_id=%s — not overwriting", hit["driver_id"], tail_id,
                       existing_fallback)
            continue

        alt_id = f"{tail_id}_ALT"
        if alt_id in existing_ids:
            counter = 2
            while f"{tail_id}_ALT{counter}" in existing_ids:
                counter += 1
            alt_id = f"{tail_id}_ALT{counter}"
        existing_ids.add(alt_id)

        confidence = float(hit.get("confidence", 0.4))
        new_rows.append({
            "driver_id": alt_id,
            "commodity": hit["commodity"],
            "driver_name": f"{hit['driver_name']} (auto-discovered alternate)",
            "region": hit["region"],
            "extraction_tier": int(hit["declared_tier"]),
            "tier_confidence": _tier_confidence_from_llm(confidence),
            "connector": hit["connector"],
            "source_name": hit["source_name"],
            "source_url": hit["source_url"],
            "endpoint_or_locator": hit["endpoint_or_locator"],
            "access_mode": hit["access_mode"],
            "native_frequency": hit["native_frequency"],
            "rollup_method": hit["rollup_method"],
            "unit": hit["unit"],
            "history_from": hit.get("history_from") or "",
            "meets_min_history": "Y",
            "update_lag_days": hit.get("update_lag_days", 30),
            "is_proxy": "Y",
            "proxy_note": (f"Auto-discovered alternate source (LLM-proposed, verified via live "
                          f"fetch on {hit.get('verified_at', '')}; confidence {confidence:.2f}). "
                          f"Reasoning: {str(hit.get('reasoning', ''))[:150]}"),
            "human_in_loop": "N",
            "credential_key": _credential_key_for_connector(hit["connector"]),
            "fallback_driver_id": "",
            "data_quality_risk": "Medium",
            "transform_hint": "",
            "priority": 3,
            "enrichment_confidence": f"{confidence:.2f}",
        })
        registry_df.loc[tail_mask, "fallback_driver_id"] = alt_id
        log.info("Committed alternate source %s as fallback for %s (chain tail %s)",
                 alt_id, hit["driver_id"], tail_id)

    if not new_rows:
        return 0

    new_df = pd.DataFrame(new_rows)
    if not registry_df.empty:
        # Preserve existing column order; append any new columns. reindex
        # (not plain [col_order] indexing) so a registry column that these
        # hit dicts don't happen to set (e.g. a schema column added after
        # this dict-building code was written, like min_history_years) is
        # filled blank instead of raising KeyError — same fix already
        # applied to registry_enrichment.py's identical pattern, which this
        # one was missed alongside at the time.
        existing_cols = list(registry_df.columns)
        new_cols = [c for c in new_df.columns if c not in existing_cols]
        col_order = existing_cols + new_cols
        new_df = new_df.reindex(columns=col_order, fill_value="")

    registry_df = pd.concat([registry_df, new_df], ignore_index=True)
    write_atomic(registry_path, lambda tmp: registry_df.to_csv(tmp, index=False))
    log.info("Appended %d auto-discovered alternate source row(s) to %s", len(new_rows), registry_path)
    return len(new_rows)


# ---------------------------------------------------------- rejected-attempt log

def load_rejected_sources(path: Path) -> dict[str, list[dict[str, Any]]]:
    """
    Reads the accumulated rejected-alternate-source log, grouped by
    driver_id, so the cascade can look up prior rejections for a specific
    driver in O(1) at the point it needs them, without re-scanning a flat
    list per driver. Returns {} if the file doesn't exist yet or can't be read.
    """
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read rejected-alternate-sources file %s (%s); treating as empty",
                   path, exc)
        return {}

    by_driver: dict[str, list[dict[str, Any]]] = {}
    for entry in payload.get("rejected_attempts", []) or []:
        driver_id = entry.get("driver_id")
        if not driver_id:
            continue
        by_driver.setdefault(driver_id, []).append(entry)
    return by_driver


def save_rejected_sources(path: Path, existing: dict[str, list[dict[str, Any]]],
                          new_attempts: list[dict[str, Any]]) -> int:
    """
    Merges this run's newly-rejected attempts into whatever was already
    recorded and writes the union back. Deduplicates by (driver_id,
    source_url): a repeatedly-rejected candidate updates its rejected_at
    timestamp in place rather than growing the file with identical entries
    forever — this also keeps the attempt-cap count meaningful (distinct
    candidates tried, not repeat log lines for the same one).

    Returns the number of genuinely new attempts recorded this call. Safe to
    call with an empty `new_attempts` list (no-op, file untouched).
    """
    if not new_attempts:
        return 0

    merged: dict[str, dict[str, dict[str, Any]]] = {}  # driver_id -> source_url -> entry
    for driver_id, entries in existing.items():
        for entry in entries:
            merged.setdefault(driver_id, {})[entry.get("source_url", "")] = entry

    for attempt in new_attempts:
        driver_id = attempt.get("driver_id")
        if not driver_id:
            continue
        merged.setdefault(driver_id, {})[attempt.get("source_url", "")] = attempt

    flat = [entry for entries in merged.values() for entry in entries.values()]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({
        "generated_at": utc_now().isoformat(timespec="seconds"),
        "instructions": (
            "Accumulated log of alternate sources the LLM proposed for a failed driver that did "
            "not pass verification (low confidence, same-domain-as-tried, fetch failure, or "
            "quality-gate failure). Read at the start of each run so the same rejected candidate "
            "is never re-proposed; see cascade.alternate_source_max_attempts_per_driver for the "
            "cap on how many rejections before a driver stops being retried altogether."
        ),
        "rejected_attempts": flat,
    }, sort_keys=False, allow_unicode=True), encoding="utf-8")
    log.info("Wrote %d rejected alternate-source attempt(s) (%d new this run) -> %s",
             len(flat), len(new_attempts), path)
    return len(new_attempts)
