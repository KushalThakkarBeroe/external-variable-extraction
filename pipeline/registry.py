"""
Registry loading and validation.

The registry CSV is the pipeline's control plane. Everything about a source
lives there, which is what lets the same code run 14 sample drivers today and
several hundred across 40 commodities later.

Validation is strict on the columns the orchestrator depends on and forgiving
on the descriptive ones, so an analyst filling in the sheet cannot break a run
by leaving a note blank.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .models import DriverSpec, Tier

log = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "driver_id", "commodity", "driver_name", "region", "extraction_tier",
    "connector", "source_name", "source_url", "endpoint_or_locator",
    "access_mode", "native_frequency", "rollup_method", "unit",
]

VALID_ROLLUP = {"mean", "sum", "last", "first", "count", "max", "min", "median"}
VALID_ACCESS = {"open_api", "html_table", "file_download", "multipage_scrape",
                "api_key", "login", "paid_or_restricted"}
VALID_FREQUENCY = {"Daily", "Weekly", "Monthly", "Quarterly", "Annual", "Event"}


def _as_bool(value) -> bool:
    return str(value).strip().upper() in ("Y", "YES", "TRUE", "1")


def _as_int(value, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_optional_float(value) -> Optional[float]:
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def load_registry(path: str | Path, commodities: Optional[list[str]] = None,
                  regions: Optional[list[str]] = None,
                  driver_ids: Optional[list[str]] = None) -> list[DriverSpec]:
    """
    Read the registry and return DriverSpec objects, optionally filtered.

    Fallback rows are always loaded regardless of filters, because the cascade
    needs them available when a primary source fails.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Registry is missing required columns: {missing}")

    duplicates = df["driver_id"][df["driver_id"].duplicated()].tolist()
    if duplicates:
        raise ValueError(f"Duplicate driver_id values in the registry: {duplicates}")

    specs: list[DriverSpec] = []
    for _, row in df.iterrows():
        tier_value = _as_int(row["extraction_tier"], 1)
        if tier_value not in (1, 2, 3, 4):
            log.warning("Row %s has invalid tier '%s'; defaulting to 1",
                        row["driver_id"], row["extraction_tier"])
            tier_value = 1

        rollup = str(row["rollup_method"]).strip().lower()
        if rollup not in VALID_ROLLUP:
            log.warning("Row %s has unknown rollup_method '%s'; defaulting to mean",
                        row["driver_id"], rollup)
            rollup = "mean"

        access = str(row["access_mode"]).strip().lower()
        if access not in VALID_ACCESS:
            log.warning("Row %s has unknown access_mode '%s'", row["driver_id"], access)

        frequency = str(row["native_frequency"]).strip()
        if frequency not in VALID_FREQUENCY:
            log.warning("Row %s has unrecognized native_frequency '%s' (expected one of %s) -- "
                       "Frequency.EXPECTED_PER_MONTH has no entry for it, so rollup.py silently "
                       "falls back to expecting 1 observation/month (the Monthly default) "
                       "regardless of the row's real cadence",
                       row["driver_id"], frequency, sorted(VALID_FREQUENCY))

        specs.append(DriverSpec(
            driver_id=str(row["driver_id"]).strip(),
            commodity=str(row["commodity"]).strip(),
            driver_name=str(row["driver_name"]).strip(),
            region=str(row["region"]).strip(),
            declared_tier=Tier(tier_value),
            connector=str(row["connector"]).strip(),
            source_name=str(row["source_name"]).strip(),
            source_url=str(row["source_url"]).strip(),
            endpoint_or_locator=str(row["endpoint_or_locator"]).strip(),
            access_mode=access,
            native_frequency=str(row["native_frequency"]).strip(),
            rollup_method=rollup,
            unit=str(row["unit"]).strip(),
            tier_confidence=str(row.get("tier_confidence", "Medium")).strip(),
            history_from=str(row.get("history_from", "")).strip() or None,
            meets_min_history=_as_bool(row.get("meets_min_history", "Y")),
            update_lag_days=_as_int(row.get("update_lag_days", 30), 30),
            is_proxy=_as_bool(row.get("is_proxy", "N")),
            proxy_note=str(row.get("proxy_note", "")).strip(),
            human_in_loop=_as_bool(row.get("human_in_loop", "N")),
            credential_key=str(row.get("credential_key", "")).strip() or None,
            fallback_driver_id=str(row.get("fallback_driver_id", "")).strip() or None,
            data_quality_risk=str(row.get("data_quality_risk", "Medium")).strip(),
            transform_hint=str(row.get("transform_hint", "")).strip(),
            priority=_as_int(row.get("priority", 2), 2),
            min_history_years=_as_optional_float(row.get("min_history_years", "")),
        ))

    log.info("Loaded %d driver specifications from %s", len(specs), path)

    if not any([commodities, regions, driver_ids]):
        return specs

    # Apply filters, then re-add any fallback rows the survivors point at.
    def matches(spec: DriverSpec) -> bool:
        if driver_ids and spec.driver_id not in driver_ids:
            return False
        if commodities and spec.commodity.lower() not in [c.lower() for c in commodities]:
            return False
        if regions and spec.region.lower() not in [r.lower() for r in regions]:
            return False
        return True

    selected = {s.driver_id: s for s in specs if matches(s)}
    by_id = {s.driver_id: s for s in specs}
    for spec in list(selected.values()):
        fallback = spec.fallback_driver_id
        while fallback and fallback in by_id and fallback not in selected:
            selected[fallback] = by_id[fallback]
            fallback = by_id[fallback].fallback_driver_id

    log.info("Filter retained %d of %d drivers (fallbacks included)", len(selected), len(specs))
    return list(selected.values())


def resolve_driver_ids(cli_driver_ids: Optional[list[str]], driver_id_file: Optional[str],
                       resume_deferred: bool, cfg) -> Optional[list[str]]:
    """
    Merges the three ways a run can be scoped to a specific driver subset:
    repeatable --driver-id, a bulk --driver-id-file (one id per line, blank
    lines and '#' comments ignored -- for a worklist too large to pass on
    the command line, see storage.py's rerun_worklist_*.xlsx), and
    --resume-deferred (drivers a prior run deferred for a source's rate
    budget, see http_client.RateLimiter/load_deferred_driver_ids).

    Returns None (no filter -- the full registry) when none of the three
    were requested, preserving load_registry()'s existing "None means
    everything" contract; an empty combined list would instead mean "match
    nothing", which is never what a bare, flag-less invocation should do.
    """
    if not cli_driver_ids and not driver_id_file and not resume_deferred:
        return None

    ids: list[str] = list(cli_driver_ids or [])
    if driver_id_file:
        file_path = Path(driver_id_file)
        if not file_path.is_absolute():
            file_path = cfg.root / file_path
        for line in file_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                ids.append(line)
    if resume_deferred:
        from .http_client import load_deferred_driver_ids
        state_path = cfg.path("paths.rate_limit_state", "output/rate_limit_state.json")
        ids.extend(sorted(load_deferred_driver_ids(state_path)))

    seen: set[str] = set()
    return [d for d in ids if not (d in seen or seen.add(d))]


def primary_drivers(specs: list[DriverSpec]) -> list[DriverSpec]:
    """
    Drivers to attempt first: everything that is not itself a fallback target.

    Fallbacks are pulled in on demand by the cascade, so running them as
    primaries too would duplicate work and pollute the panel.
    """
    fallback_ids = {s.fallback_driver_id for s in specs if s.fallback_driver_id}
    ordered = [s for s in specs if s.driver_id not in fallback_ids]
    return sorted(ordered, key=lambda s: (s.priority, s.commodity, s.driver_name))
