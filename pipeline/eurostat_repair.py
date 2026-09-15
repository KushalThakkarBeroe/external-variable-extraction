"""
Eurostat locator self-repair.

Resolves a bad Eurostat locator (see connectors.EurostatLocatorError) against
Eurostat's own live metadata — never a guess. Two repair paths, matching the
two ways connectors.eurostat() can fail on a bad locator:

  resolve_dataset_code() - the dataset code itself doesn't exist (404).
    Shortlists real candidate datasets from Eurostat's own catalogue table of
    contents by fuzzy title match, then has the LLM SELECT one of those real
    candidates (never invent a new code) — same "coordinates not values"
    discipline as llm.choose_table.

  resolve_filters() - the dataset exists but a filter dimension/category is
    wrong or missing (400/413/unresolved-dimensions). Reads the dataset's own
    real dimension/category metadata, then has the LLM SELECT real category
    codes from that menu (never invent a filter value).

Neither function ever returns a locator whose pieces weren't verified against
live Eurostat metadata; CascadeOrchestrator._repair_eurostat_locator then
verifies the result with a real fetch through the same quality gate as every
other tier before trusting it.

Persistence mirrors pipeline/alternate_sources.py: a verified repair is
committed into driver_registry.csv (update-in-place — this corrects the same
driver's own locator, it does not add a fallback row) once at the end of the
run, main thread only. A rejected attempt accumulates in a separate YAML file
(different record shape than rejected_alternate_sources.yaml, and a distinct
attempt-cap budget) so a dead-end guess is never silently retried forever.
"""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml

from . import connectors
from .atomic_io import write_atomic
from .models import utc_now

log = logging.getLogger(__name__)

TOC_URL = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en"


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


# ------------------------------------------------------------- dataset repair

def _fetch_toc(http) -> pd.DataFrame:
    """
    Fetches and parses Eurostat's full catalogue table of contents (tab-
    separated, ~12k rows), filtered to real queryable datasets. Relies on
    HttpClient's existing on-disk cache (http.cache_ttl_hours) so repeated
    calls within/across runs don't re-download the ~2MB file every time.
    """
    text = http.get_text(TOC_URL)
    rows = [line.split("\t") for line in text.splitlines() if line.strip()]
    if not rows:
        return pd.DataFrame(columns=["title", "code", "type"])
    header = [h.strip().strip('"') for h in rows[0]]
    data = [[c.strip().strip('"') for c in r] for r in rows[1:] if len(r) == len(header)]
    df = pd.DataFrame(data, columns=header)
    if "type" in df.columns:
        df = df[df["type"] == "dataset"]
    return df


def _shortlist_datasets(toc_df: pd.DataFrame, spec, limit: int = 20) -> list[dict]:
    """Top-N Eurostat datasets by fuzzy title match against this driver's identity."""
    if toc_df.empty or "title" not in toc_df.columns or "code" not in toc_df.columns:
        return []
    query = _normalize(f"{spec.commodity} {spec.driver_name} {spec.region}")
    scored = []
    for title, code in zip(toc_df["title"], toc_df["code"]):
        title = str(title).strip()
        code = str(code).strip()
        if not title or not code:
            continue
        score = SequenceMatcher(None, query, _normalize(title)).ratio()
        scored.append({"code": code, "title": title, "score": score})
    scored.sort(key=lambda c: -c["score"])
    return scored[:limit]


def resolve_dataset_code(http, llm, spec, cfg) -> Optional[dict]:
    """
    Returns {"locator": "dataset:<code>", "confidence": float, "reasoning": str}
    or None if nothing could be resolved with adequate confidence.
    """
    try:
        toc_df = _fetch_toc(http)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not fetch Eurostat catalogue TOC: %s", exc)
        return None

    candidates = _shortlist_datasets(toc_df, spec)
    if not candidates:
        return None

    fuzzy_min = float(cfg.get("cascade.eurostat_repair_fuzzy_match_min_score", 0.72))
    top = candidates[0]

    if llm and llm.enabled:
        selection = llm.select_eurostat_dataset(spec, candidates)
        if selection:
            picked = str(selection.get("dataset_code", "")).strip()
            if picked in {c["code"] for c in candidates}:
                return {
                    "locator": f"dataset:{picked}",
                    "confidence": float(selection.get("confidence", 0)),
                    "reasoning": str(selection.get("reasoning", "")),
                }

    # No LLM, or the LLM didn't select a valid candidate: fall back to the
    # deterministic fuzzy match if it clears the confidence floor.
    if top["score"] >= fuzzy_min:
        return {
            "locator": f"dataset:{top['code']}",
            "confidence": top["score"],
            "reasoning": f"Deterministic fuzzy match against Eurostat catalogue title "
                        f"{top['title']!r} (score {top['score']:.2f})",
        }
    return None


# -------------------------------------------------------------- filter repair

def resolve_filters(http, llm, spec, hint: dict, cfg) -> Optional[dict]:
    """
    Returns {"locator": "dataset:<code> | filters:<k=v;...>", "confidence": float,
    "reasoning": str} or None if every required dimension couldn't be pinned
    to a real category.
    """
    dataset = hint.get("dataset")
    dimensions = hint.get("available_dimensions")
    if not dimensions:
        try:
            dimensions = connectors.eurostat_fetch_dimensions(http, dataset)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not fetch Eurostat dimension metadata for %s: %s", dataset, exc)
            return None
    if not dimensions:
        return None

    # Real menu: every non-time dimension with more than one category needs a pin.
    menu: dict[str, dict[str, str]] = {}
    for dim_id, dim in dimensions.items():
        if dim_id == "time":
            continue
        category = (dim or {}).get("category", {}) or {}
        index = category.get("index", {}) or {}
        if len(index) > 1:
            labels = category.get("label", {}) or {}
            menu[dim_id] = {code: labels.get(code, code) for code in index}
    if not menu:
        return None

    # Keep any already-declared filter that is a literal, valid pin for this dataset.
    existing_filters = dict(hint.get("filters") or {})
    resolved: dict[str, str] = {}
    for dim_id, value in existing_filters.items():
        real_codes = ((dimensions.get(dim_id) or {}).get("category", {}) or {}).get("index", {}) or {}
        if dim_id in menu and value in real_codes:
            resolved[dim_id] = value

    still_needed = {dim_id: cats for dim_id, cats in menu.items() if dim_id not in resolved}
    confidence = 0.6 if resolved and not still_needed else 0.0
    reasoning = "Reused existing valid filter(s) from the original locator" if resolved else ""

    if still_needed and llm and llm.enabled:
        selection = llm.select_eurostat_filters(spec, still_needed)
        if selection:
            pins = selection.get("filters") or {}
            if all(dim_id in still_needed and code in still_needed[dim_id]
                   for dim_id, code in pins.items()) and set(pins) == set(still_needed):
                resolved.update(pins)
                still_needed = {}
                confidence = float(selection.get("confidence", 0.5))
                reasoning = str(selection.get("reasoning", ""))

    if still_needed:
        # Deterministic fallback: pick the category whose label shares the
        # most word-tokens with this driver's own region/commodity/unit —
        # every candidate considered is still a real category from the menu,
        # never invented.
        hint_tokens = set(_normalize(f"{spec.region} {spec.commodity} {spec.unit}").split())
        for dim_id, cats in list(still_needed.items()):
            best_code, best_overlap = None, 0
            for code, label in cats.items():
                overlap = len(set(_normalize(str(label)).split()) & hint_tokens)
                if overlap > best_overlap:
                    best_overlap, best_code = overlap, code
            if best_code and best_overlap > 0:
                resolved[dim_id] = best_code
                del still_needed[dim_id]
        if not still_needed and confidence == 0.0:
            confidence = 0.4
            reasoning = "Deterministic label-token-overlap match against region/commodity/unit"

    if still_needed:
        return None  # could not pin every required dimension to a real category

    filter_str = ";".join(f"{k}={v}" for k, v in resolved.items())
    return {
        "locator": f"dataset:{dataset} | filters:{filter_str}",
        "confidence": confidence,
        "reasoning": reasoning,
    }


# --------------------------------------------------------------- registry commit

def commit_eurostat_locator_repairs(cfg, registry_path: Path, hits: list[dict[str, Any]]) -> int:
    """
    Overwrites endpoint_or_locator in-place for each verified repair (this
    corrects the driver's own locator, it does not add a fallback row like
    alternate_sources.commit_alternate_sources does). Appends a dated audit
    note to the existing transform_hint column rather than a new schema
    column, matching the convention registry_enrichment.py already uses for
    LLM reasoning text.
    """
    if not hits:
        return 0

    try:
        registry_df = pd.read_csv(registry_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        log.warning("Registry not found at %s; cannot commit Eurostat locator repairs", registry_path)
        return 0

    committed = 0
    for hit in hits:
        driver_id = hit["driver_id"]
        mask = registry_df["driver_id"] == driver_id
        if not mask.any():
            log.warning("Eurostat locator repair commit skipped for %s: no longer in registry", driver_id)
            continue

        note = (f"[eurostat_repair {utc_now():%Y-%m-%d}] {hit['old_locator']} -> "
                f"{hit['new_locator']} ({hit['reason']}, confidence {hit['confidence']:.2f})")
        registry_df.loc[mask, "endpoint_or_locator"] = hit["new_locator"]
        if "transform_hint" in registry_df.columns:
            registry_df.loc[mask, "transform_hint"] = note[:200]
        committed += 1
        log.info("Committed Eurostat locator repair for %s: %s -> %s",
                 driver_id, hit["old_locator"], hit["new_locator"])

    if committed:
        write_atomic(registry_path, lambda tmp: registry_df.to_csv(tmp, index=False))
        log.info("Committed %d Eurostat locator repair(s) to %s", committed, registry_path)
    return committed


# ---------------------------------------------------------- rejected-attempt log

def load_eurostat_repair_state(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Reads the accumulated rejected-repair log, grouped by driver_id. {} if absent/unreadable."""
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read rejected-eurostat-repairs file %s (%s); treating as empty", path, exc)
        return {}

    by_driver: dict[str, list[dict[str, Any]]] = {}
    for entry in payload.get("rejected_attempts", []) or []:
        driver_id = entry.get("driver_id")
        if not driver_id:
            continue
        by_driver.setdefault(driver_id, []).append(entry)
    return by_driver


def save_eurostat_repair_state(path: Path, existing: dict[str, list[dict[str, Any]]],
                               new_attempts: list[dict[str, Any]]) -> int:
    """
    Merges new rejections in, deduplicated by (driver_id, dataset, reason) —
    a repeatedly-rejected candidate updates in place rather than growing the
    file forever, keeping the attempt-cap count meaningful.
    """
    if not new_attempts:
        return 0

    merged: dict[str, dict[tuple, dict[str, Any]]] = {}
    for driver_id, entries in existing.items():
        for entry in entries:
            key = (entry.get("dataset", ""), entry.get("reason", ""))
            merged.setdefault(driver_id, {})[key] = entry

    for attempt in new_attempts:
        driver_id = attempt.get("driver_id")
        if not driver_id:
            continue
        key = (attempt.get("dataset", ""), attempt.get("reason", ""))
        merged.setdefault(driver_id, {})[key] = attempt

    flat = [entry for entries in merged.values() for entry in entries.values()]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({
        "generated_at": utc_now().isoformat(timespec="seconds"),
        "instructions": (
            "Accumulated log of Eurostat locator repair attempts that did not pass verification "
            "(no resolution found, low confidence, fetch failure, or quality-gate failure). Read "
            "at the start of each run so the same dead-end guess is never re-tried; see "
            "cascade.eurostat_locator_repair_max_attempts_per_driver for the cap on how many "
            "rejections before a driver stops being retried altogether."
        ),
        "rejected_attempts": flat,
    }, sort_keys=False, allow_unicode=True), encoding="utf-8")
    log.info("Wrote %d rejected Eurostat locator repair attempt(s) (%d new this run) -> %s",
             len(flat), len(new_attempts), path)
    return len(new_attempts)
