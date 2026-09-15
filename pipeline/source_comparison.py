"""
Padma alternate/same-source comparison feature.

Classifies every driver padma flagged with an "Alternate Source" / "Same
Source" / "Alternate Region/Source Suggested" status against the current
registry and the last known run outcome, then prepares and reconciles the
registry mutations needed to actually exercise those sources through the
*existing, unmodified* pipeline.

Deliberately touches nothing in cascade.py / connectors.py / run_pipeline.py:
- "Swap-in" candidates (currently failing) get the alternate source appended
  as a new fallback row at the tail of their existing chain -- the exact
  shape `alternate_sources.commit_alternate_sources()` already uses for
  LLM-discovered alternates. The normal cascade walks into it automatically
  once every declared tier is exhausted; nothing new to run.
- "Comparison" candidates (currently succeeding) get a standalone temporary
  probe row (same commodity/driver_name/region, a synthetic driver_id, not
  wired into anyone's fallback chain) so the alternate source can be
  fetched independently via a normal `run_pipeline.py --driver-id-file` run,
  without disturbing the already-working primary row at all.
Actually running the fetch is just `python run_pipeline.py --driver-id-file
<file>` -- the existing entry point, unmodified. Reconciliation (deciding a
winner, folding the temp probe row's outcome back into the registry) is a
separate, later step once that run has produced fresh results.
"""

from __future__ import annotations

import csv
import json
import re
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import openpyxl

DATE_TAG = "2026-08-07"

# ---------------------------------------------------------------------------
# padma Source Status scope
# ---------------------------------------------------------------------------


def norm(s) -> str:
    return str(s).strip().lower() if s is not None else ""


def is_in_scope(status_text) -> bool:
    """Matches the 151-driver set already agreed with the user this session:
    literal 'alternate source' / 'same source' anywhere in the Source Status
    text, plus the exact 'Alternate Region/Source Suggested' category."""
    s = norm(status_text)
    if not s:
        return False
    if "alternate source" in s or "same source" in s:
        return True
    if s == "alternate region/source suggested":
        return True
    return False


# A bare domain/path with no scheme (e.g. "apps.fas.usda.gov/psdonline/api")
# still counts as a URL -- matches an explicit scheme/www prefix, OR a
# dotted-domain-looking token followed by '/' or end of string.
_DOMAIN_RE = re.compile(
    r"^(https?://|www\.)|^[a-z0-9-]+(\.[a-z0-9-]+)+(/|$)", re.IGNORECASE
)


def looks_like_url(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    # A cell can hold more than one line (e.g. two candidate URLs); only the
    # first token needs to look like a URL for the cell to be usable at all.
    first = t.splitlines()[0].strip()
    if "://" in first:
        return True  # covers a typo'd scheme too, e.g. "ttps://..." missing its leading h
    return bool(_DOMAIN_RE.match(first))


def normalize_url(text: str) -> str:
    # Only the first URL in a multi-line cell is used; a second suggested
    # link (if any) is intentionally not fetched by this pass.
    t = text.strip().splitlines()[0].strip()
    if "://" in t:
        scheme, _, rest = t.partition("://")
        if scheme.lower() not in ("http", "https"):
            scheme = "https"   # fixes a truncated/typo'd scheme, e.g. "ttps://" missing its leading h
        return f"{scheme}://{rest}"
    return "https://" + t


_INDEX_HINTS = ("reports", "listing", "index", "archive", "publications", "database", "databrowser")


def guess_access_mode(url: str) -> tuple[str, int]:
    """(access_mode, extraction_tier). Tier 1 is attempted for every driver
    regardless of declared tier, so a wrong guess here just affects ordering,
    not whether the source gets a real attempt at all."""
    u = url.lower()
    if any(h in u for h in _INDEX_HINTS):
        return "multipage_scrape", 3
    return "html_table", 1


# ---------------------------------------------------------------------------
# Candidate model
# ---------------------------------------------------------------------------


@dataclass
class Candidate:
    driver_id: str
    commodity: str
    driver_name: str
    region: str
    padma_source_status: str
    padma_source_field_used: str   # "G (Alternate Source)" | "F (Current Source)"
    padma_source_text: str
    case: str                      # "swap_in" | "comparison" | "skip"
    reason: str = ""
    access_mode: str = ""
    extraction_tier: int = 0
    alt_source_url: str = ""
    baseline_outcome: str = ""
    baseline_coverage_ratio: float = 0.0
    baseline_monthly_observations: int = 0
    fallback_new_id: str = ""
    comparison_temp_id: str = ""
    # Filled in by reconcile_source_comparison.py once a batch's fetch run has
    # produced fresh results; blank until then. Persisted back into the
    # manifest file so a later combine step never needs to re-derive them.
    decision_tag: str = ""       # e.g. "swapped", "kept_previous", "new_success", "still_failing", "skipped"
    decision_detail: str = ""    # human-readable sentence, same text shown in the color-coded report column
    # padma's Alternate Source text when it wasn't itself a URL but Current
    # Source was used instead (e.g. "PDF downloads", "BIS", "Destatis") --
    # kept for audit even after RESOLVED_OVERRIDES turns it into a real
    # connector+locator below.
    source_hint: str = ""
    resolved_connector: str = ""   # non-blank => a verified connector/locator from RESOLVED_OVERRIDES, not a generic scrape
    resolved_locator: str = ""
    resolved_credential_key: str = ""
    resolved_note: str = ""
    resolved_is_proxy: bool = False


def _dedupe_id(base_id: str, existing_ids: set[str]) -> str:
    if base_id not in existing_ids:
        return base_id
    counter = 2
    while f"{base_id}{counter}" in existing_ids:
        counter += 1
    return f"{base_id}{counter}"


# ---------------------------------------------------------------------------
# Phase 1: build the candidate list
# ---------------------------------------------------------------------------


def load_registry_rows(registry_path: Path) -> tuple[list[str], list[dict]]:
    with open(registry_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    return fieldnames, rows


def _registry_root_index(rows: list[dict]) -> dict[tuple[str, str], dict]:
    referenced = set(r["fallback_driver_id"] for r in rows if r["fallback_driver_id"])
    roots = [r for r in rows if r["driver_id"] not in referenced]
    idx: dict[tuple[str, str], dict] = {}
    for r in roots:
        idx[(norm(r["commodity"]), norm(r["driver_name"]))] = r
    return idx


def load_baseline_report(report_path: Path) -> dict[str, dict]:
    """Keyed by driver_id -> {outcome, coverage_ratio, monthly_observations}."""
    wb = openpyxl.load_workbook(report_path, read_only=True)
    ws = wb["All Drivers"]
    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(header)}
    out = {}
    for r in ws.iter_rows(min_row=2, values_only=True):
        did = r[idx["driver_id"]]
        out[did] = {
            "outcome": r[idx["outcome"]],
            "coverage_ratio": float(r[idx["coverage_ratio"]] or 0.0),
            "monthly_observations": int(r[idx["monthly_observations"]] or 0),
        }
    return out


# padma occasionally splits what the registry already treats as ONE combined
# driver into two rows (e.g. "Annual Supply Data" + "Annual Consumption Data"
# both really are NORTH_AMERICA_WHEAT_DEMANDSUPPLY_ANNUAL_DATA's "Demand/Supply
# - Annual Data"). Confirmed by hand earlier this session -- both padma rows
# recommend the identical alternate source, so there is nothing lost by
# resolving both to the one real registry driver instead of leaving them
# unmatched.
KNOWN_ALIASES: dict[tuple[str, str], tuple[str, str]] = {
    ("wheat", "annual supply data"): ("wheat", "demand/supply - annual data"),
    ("wheat", "annual consumption data"): ("wheat", "demand/supply - annual data"),
    ("wheat", "usda wasde production"): ("wheat", "usda wasde production & ending stocks"),
    ("wheat", "usda wasde ending stocks"): ("wheat", "usda wasde production & ending stocks"),
}

_FORMAT_HINT_KEYWORDS = ("pdf", "xlsx", "excel", "download", "parse data table")


def _looks_like_format_hint(hint: str) -> bool:
    h = hint.lower()
    return any(k in h for k in _FORMAT_HINT_KEYWORDS)


# ---------------------------------------------------------------------------
# Verified overrides for the 44 candidates whose Alternate Source (G) was a
# plain label, not a URL. Every entry below was live-verified this session
# (real HTTP 200 + sane response body/label) before being wired in -- see the
# session's research notes. Keyed by driver_id.
#
# Fields: connector, locator (endpoint_or_locator string), access_mode,
# tier, credential_key (blank if keyless), note, source_url (override,
# only set when different from Current Source itself).
# ---------------------------------------------------------------------------
RESOLVED_OVERRIDES: dict[str, dict] = {
    # --- format-hint rows: Current Source page carries a PDF/XLSX/Excel file ---
    "CHINA_AIR_FREIGHT_CARGO_LOAD_FACTOR_CTL": {
        "access_mode": "file_download", "tier": 2,
        "note": "IATA cargo market analysis page publishes a PDF table each month.",
    },
    "ASIAPACIFIC_AIR_FREIGHT_IATA_ASIAPACIFIC_AIR_CARGO_CTK_INDEX": {
        "access_mode": "file_download", "tier": 2,
        "note": "IATA economic report page; PDF table.",
    },
    "APAC_AIR_FREIGHT_PASSENGER_BELLY_CARGO_CAPACITY": {
        "access_mode": "file_download", "tier": 2,
        "note": "IATA air cargo market analysis page; PDF table.",
    },
    "NORWAY_ATLANTIC_COD_NORWAY_ICELAND_COD_LANDINGS": {
        "access_mode": "file_download", "tier": 2,
        "note": "Norwegian Directorate of Fisheries open-data page links an Excel export.",
    },
    "INDIA_CARBON_STEEL_INDIA_DOMESTIC_IRON_ORE_PRICE_NMDCODISHA": {
        "access_mode": "file_download", "tier": 2,
        "note": "NMDC publishes iron ore price notifications as linked PDFs.",
    },
    "INDIA_NATURAL_GAS_DEMAND_TOTAL_CONSUMPTION": {
        "access_mode": "file_download", "tier": 2,
        "note": "PPAC consumption page; live-verified reachable (200, real content).",
    },
    "INDIA_NATURAL_GAS_IMPORTS_LNG_IMPORTS_INDIA": {
        "access_mode": "file_download", "tier": 2,
        "note": "PPAC prices page links an Excel export.",
    },
    "GLOBAL_GOLD_GEOPOLITICAL_RISK_INDEX": {
        "access_mode": "file_download", "tier": 2,
        "note": "Caldara-Iacoviello GPR page publishes the full series as a linked XLSX.",
    },
    "GLOBAL_PLATINUM_ETF_HOLDINGS": {
        "access_mode": "file_download", "tier": 2,
        "note": "World Platinum Investment Council ETF holdings page links an XLSX.",
    },
    "INDIA_PLATINUM_JEWELRY_DEMAND_INDIACHINA": {
        "access_mode": "file_download", "tier": 2,
        "note": "WPIC supply-demand data page links an XLSX.",
    },
    "NORTH_AMERICA_PLATINUM_GLOBAL_AUTO_CATALYST_DEMAND": {
        "access_mode": "file_download", "tier": 2,
        "note": "WPIC fundamentals supply-demand page links an XLSX.",
    },
    "SOUTH_AFRICA_PLATINUM_SOUTH_AFRICA_MINE_SUPPLY_AMPLATS_IMPALA_SIBANYE": {
        "access_mode": "file_download", "tier": 2,
        "note": "WPIC quarterly platinum data page links an XLSX.",
    },

    # --- "NA"/"Not Required" but Current Source is a real, specific URL ---
    "SOUTH_AFRICA_CEMENT_SOUTH_AFRICA_LIMESTONERAW_MATERIAL_COST": {
        "access_mode": "multipage_scrape", "tier": 3,
        "note": ("Stats SA page reachable but returned a near-empty (212-byte) body on a keyless "
                 "GET -- likely a JS-rendered shell. Low confidence; kept as a genuine attempt "
                 "rather than a silent skip, but flagged."),
    },
    "GLOBAL_CPI_GLOBAL_SUPPLY_CHAIN_PRESSURE_INDEX": {
        "access_mode": "file_download", "tier": 2,
        "note": "NY Fed GSCPI page live-verified (200, 105KB); publishes a linked data file.",
    },
    "BRAZIL_ETHANOL_PHARMACEUTICAL_PRODUCTION_INDEX_BRAZIL": {
        "connector": "ibge_sidra", "locator": "table:8888", "access_mode": "open_api", "tier": 1,
        "note": "Table number 8888 was given directly in padma's Current Source URL; live-verified reachable.",
    },
    "INDIA_NATURAL_GAS_SECTORAL_GAS_CONSUMPTION_FERTILIZER_CGD_POWER_REFINERY_PETROCHEMICAL_INDIA": {
        "access_mode": "multipage_scrape", "tier": 3,
        "note": "PPAC sectoral consumption page; live-verified reachable (200, real content).",
    },
    "EU_GLASS_BOTTLES_EXCHANGE_RATE_VS_USDEUR": {
        "connector": "frankfurter", "locator": "base:USD | target:ZAR", "access_mode": "open_api", "tier": 1,
        "note": ("Relevance column explicitly asked for USD-ZAR, not USD-EUR; 'Not Required' meant "
                 "'same source, just a different currency pair', not 'no change needed'."),
    },

    # --- named institutions matching an already-built, already-verified connector ---
    "EU_ATLANTIC_COD_NOKUSD_EURUSD_EXCHANGE_RATE": {
        "connector": "bis_fx", "locator": "currency:NOK", "access_mode": "open_api", "tier": 1,
        "note": ("Driver covers both NOK/USD and EUR/USD; bis_fx takes one currency per row so NOK "
                 "(the less commonly available of the two via Frankfurter) was chosen as primary."),
    },
    "INDIA_PARACETAMOL_USDINR_USDCNY_EXCHANGE_RATE": {
        "connector": "bis_fx", "locator": "currency:INR", "access_mode": "open_api", "tier": 1,
        "note": "Driver covers both USD/INR and USD/CNY; INR chosen as primary (CNY less reliably free via BIS).",
    },
    "GLOBAL_GLYCERIN_CRUDE_OIL_PRICE_SYNTHETIC_GLYCERIN_ROUTE": {
        "connector": "eia", "locator": "PET.RBRTE.D", "access_mode": "api_key", "tier": 4,
        "credential_key": "eia_api_key",
        "note": "Brent crude EIA series -- matches the same DCOILBRENTEU->PET.RBRTE.D mapping already used elsewhere in this registry.",
    },
    "ASIA_STYRENEBUTADIENE_RUBBER_SBR_UTILITY_NATURAL_GAS": {
        "connector": "eia", "locator": "NG.RNGWHHD.D", "access_mode": "api_key", "tier": 4,
        "credential_key": "eia_api_key", "is_proxy": True,
        "note": "Henry Hub (US) natural gas price as a cross-market proxy for Asia utility gas cost -- same proxy pattern used elsewhere in this registry; not the same regional benchmark.",
    },
    "APAC_PPI_CAPACITY_UTILIZATION_RATE": {
        "connector": "fred", "locator": "series_id:BSCURT02JPQ160S", "access_mode": "api_key", "tier": 4,
        "credential_key": "fred_api_key",
        "note": "Series id read directly from padma's own Current Source URL; live-verified the series exists (public page, 200).",
    },
    "CHINA_OCEAN_FREIGHT_RETAIL_SALES_EUROPECHINA": {
        "connector": "eurostat", "locator": "dataset:ei_isrt_m | filters:geo=EU27_2020", "access_mode": "open_api", "tier": 1,
        "note": ("Current Source (China NBS) is a known bot-blocked host; Eurostat's monthly retail "
                 "trade turnover index is the closest EU-side counterpart to complete this China/EU "
                 "comparison driver -- not live-verified (dataset code from general Eurostat catalogue "
                 "knowledge, not confirmed this session); treat with lower confidence than the others below."),
    },

    # --- Eurostat databrowser links where G names an institution but F already has a working dataset code ---
    "HUNGARY_AVERAGE_WAGES_HUNGARY_CPI_KSH": {
        "connector": "eurostat", "locator": "dataset:prc_hicp_midx | filters:geo=HU", "access_mode": "open_api", "tier": 1,
        "note": "Dataset code read from padma's own Current Source URL; live-verified (200).",
    },
    "HUNGARY_AVERAGE_WAGES_HUNGARY_SECTORAL_EMPLOYMENT_MIX_SHIFT_MANUFACTURING_VS_SERVICES": {
        "connector": "eurostat", "locator": "dataset:lfsa_egan22d | filters:geo=HU", "access_mode": "open_api", "tier": 1,
        "note": "Dataset code read from padma's own Current Source URL.",
    },
    "HUNGARY_AVERAGE_WAGES_HUNGARY_LABOR_FORCE_PARTICIPATION_RATE_KSH": {
        "access_mode": "html_table", "tier": 1,
        "note": "Direct KSH stadat page given by padma; live-verified reachable (200, 67KB real content).",
    },

    # --- Destatis rows: all 7 already cite a Eurostat dataset in Current Source; resolved via
    # live-verified Eurostat COICOP special-aggregate codes (prc_hicp_midx dimension metadata
    # fetched and checked this session) rather than chasing Destatis GENESIS table numbers ---
    "GERMANY_CPI_GERMANY_CORE_SERVICES_INFLATION_DESTATISBUNDESBANK": {
        "connector": "eurostat", "locator": "dataset:prc_hicp_midx | filters:geo=DE;coicop=SERV;unit=I15",
        "access_mode": "open_api", "tier": 1,
        "note": "SERV = 'Services (overall index excluding goods)', confirmed a real COICOP code via live dimension lookup.",
    },
    "GERMANY_CPI_GERMANY_ENERGY_PRICES_FUELELECTRICITY_DESTATISBDEW": {
        "connector": "eurostat", "locator": "dataset:prc_hicp_midx | filters:geo=DE;coicop=NRG;unit=I15",
        "access_mode": "open_api", "tier": 1,
        "note": "NRG = 'Energy', confirmed a real COICOP code via live dimension lookup.",
    },
    "GERMANY_CPI_GERMANY_HOUSINGRENT_COST_INDEX_DESTATIS": {
        "connector": "eurostat", "locator": "dataset:prc_hicp_midx | filters:geo=DE;coicop=CP041;unit=I15",
        "access_mode": "open_api", "tier": 1,
        "note": "CP041 = 'Actual rentals for housing', confirmed via live dimension lookup.",
    },
    "GERMANY_CPI_GERMANY_USED_NEW_VEHICLE_PRICE_INDEX_DESTATIS": {
        "connector": "eurostat", "locator": "dataset:prc_hicp_midx | filters:geo=DE;coicop=CP0711;unit=I15",
        "access_mode": "open_api", "tier": 1,
        "note": "CP0711 = 'Motor cars' (covers both new/used sub-codes 07111/07112), confirmed via live dimension lookup.",
    },
    "GERMANY_CPI_GERMANY_IMPORT_PRICE_INDEX_DESTATIS": {
        "connector": "eurostat", "locator": "dataset:sts_inpi_m | filters:geo=DE",
        "access_mode": "open_api", "tier": 1,
        "note": "Dataset itself IS the import price index (no COICOP sub-filter needed); live-verified (200).",
    },
    "GERMANY_CPI_GERMANY_FOOD_PRICE_INDEX_DESTATIS": {
        "connector": "eurostat", "locator": "dataset:prc_hicp_midx | filters:geo=DE;coicop=FOOD;unit=I15",
        "access_mode": "open_api", "tier": 1,
        "note": "FOOD = 'Food including alcohol and tobacco', confirmed via live dimension lookup.",
    },
    "GERMANY_CPI_GERMANY_WAGE_GROWTH_DESTATIS_VERDIENSTE": {
        "connector": "eurostat", "locator": "dataset:lc_lci_r2_q | filters:geo=DE", "access_mode": "open_api", "tier": 1,
        "note": ("Original lc_lci_r2 code from Current Source is retired (404, live-checked). "
                 "lc_lci_r2_q (quarterly Labour Cost Index) is the live replacement -- verified 200 this session."),
    },

    # --- gold: World Gold Council (Current Source) is more directly on-topic than chasing IMF's SDMX API ---
    "GLOBAL_GOLD_CENTRAL_BANK_GOLD_PURCHASES": {
        "access_mode": "html_table", "tier": 1,
        "note": ("padma suggested IMF, but Current Source (World Gold Council Goldhub) is already the "
                 "more directly on-topic, simpler source for central-bank gold reserves; IMF's SDMX API "
                 "structure for this specific series wasn't confirmed live this session, so kept on the "
                 "source that's already verified relevant rather than guessing an IMF series code."),
    },

    # --- Trademap-family: trademap.org itself needs registration for bulk queries (checked live --
    # homepage loads, but that doesn't confirm free bulk access); every one of these already has
    # Comtrade as Current Source, so routed there instead with a real HS code ---
    "EUROPE_ALUMINUM_TRADE_VOLUME_IMPORTSEXPORTS": {
        "connector": "un_comtrade", "locator": "cmdCode=7601;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key",
        "note": ("padma's 'HSCode 720280' note appears to be misattached from the Ferro-Tungsten row "
                 "(720280 is ferro-tungsten's real HS code, not aluminum's) -- used aluminum's actual "
                 "HS code (7601, unwrought aluminium) instead."),
    },
    "LATIN_AMERICA_ETHANOL_TRADE_INDUSTRIAL_ETHANOL_IMPORTS_LATAM": {
        "connector": "un_comtrade", "locator": "cmdCode=2207;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "HS 2207, ethyl alcohol.",
    },
    "US_ETHANOL_US_ETHANOL_EXPORT_DEMAND": {
        "connector": "un_comtrade", "locator": "cmdCode=2207;flowCode=X;reporterCode=842", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "HS 2207, US as reporter, export flow.",
    },
    "NORTH_AMERICA_FERROTUNGSTEN_TRADE_IMPORTSEXPORTS": {
        "connector": "un_comtrade", "locator": "cmdCode=720280;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "The HS code padma actually gave (720280) -- correctly belongs here.",
    },
    "MEA_GLASS_BOTTLES_TRADE_IMPORTSEXPORTS": {
        "connector": "un_comtrade", "locator": "cmdCode=7010;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "HS 7010, glass containers.",
    },
    "EUROPE_GLYCERIN_TRADE_GLYCERIN_IMPORTSEXPORTS": {
        "connector": "un_comtrade", "locator": "cmdCode=290545;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "HS 2905.45, glycerol.",
    },
    "EUROPE_PROPYLENE_TRADE_IMPORTEXPORT": {
        "connector": "un_comtrade", "locator": "cmdCode=290122;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "HS 2901.22, propylene.",
    },
    "ASIA_STYRENEBUTADIENE_RUBBER_SBR_TRADE_IMPORTEXPORT": {
        "connector": "un_comtrade", "locator": "cmdCode=400219;flowCode=M", "access_mode": "api_key", "tier": 4,
        "credential_key": "comtrade_api_key", "note": "HS 4002.19, styrene-butadiene rubber.",
    },
}

# Genuinely not actionable: padma explicitly says the current source already
# covers it, with no real alternate suggested at all. Left as skip with an
# accurate reason rather than forced through.
NOT_REQUIRED_SKIPS = {
    "MEA_GLASS_BOTTLES_COMPETING_PACKAGING_MATERIAL_PRICES_PET_ALUMINUM_CANS":
        "padma marked 'Not Required' -- current World Bank Pink Sheet source already covers this",
}


def build_candidates(padma_path: Path, registry_path: Path, baseline_report_path: Path) -> list[Candidate]:
    wb = openpyxl.load_workbook(padma_path, read_only=True)
    ws = wb["Driver Source Validation"]
    prows = list(ws.iter_rows(min_row=2, values_only=True))

    _, registry_rows = load_registry_rows(registry_path)
    root_index = _registry_root_index(registry_rows)
    baseline = load_baseline_report(baseline_report_path)

    seen_keys: set[tuple[str, str]] = set()
    seen_registry_ids: set[str] = set()
    candidates: list[Candidate] = []

    for r in prows:
        commodity_p, driver_p, region_p = r[1], r[2], r[3]
        current_source, alt_source = r[5], r[6]
        status = r[9]

        if not is_in_scope(status):
            continue
        key = (norm(commodity_p), norm(driver_p))
        if key in seen_keys:
            continue  # defensive: no dup keys expected at this point, but don't double-process
        seen_keys.add(key)
        key = KNOWN_ALIASES.get(key, key)

        root = root_index.get(key)
        if root is not None and root["driver_id"] in seen_registry_ids:
            candidates.append(Candidate(
                driver_id=root["driver_id"], commodity=root["commodity"], driver_name=root["driver_name"],
                region=root["region"], padma_source_status=str(status), padma_source_field_used="",
                padma_source_text="", case="skip",
                reason=f"padma splits this into multiple rows; already processed as {root['driver_id']}",
            ))
            continue
        if root is not None:
            seen_registry_ids.add(root["driver_id"])
        if root is None:
            candidates.append(Candidate(
                driver_id="", commodity=str(commodity_p), driver_name=str(driver_p),
                region=str(region_p or ""), padma_source_status=str(status),
                padma_source_field_used="", padma_source_text="",
                case="skip", reason="no matching registry driver found",
            ))
            continue

        driver_id = root["driver_id"]

        if driver_id in NOT_REQUIRED_SKIPS:
            candidates.append(Candidate(
                driver_id=driver_id, commodity=root["commodity"], driver_name=root["driver_name"],
                region=root["region"], padma_source_status=str(status), padma_source_field_used="",
                padma_source_text=str(alt_source or ""), case="skip", reason=NOT_REQUIRED_SKIPS[driver_id],
            ))
            continue

        g_text = (alt_source or "").strip()
        f_text = (current_source or "").strip()

        if looks_like_url(g_text):
            source_text, field_used, hint = g_text, "G (Alternate Source)", ""
        elif looks_like_url(f_text):
            # G wasn't a URL (blank, or a plain label/instruction like "PDF downloads",
            # "BIS", "NA") -- fall back to F, keeping G's text as a hint for
            # access_mode/connector matching (see RESOLVED_OVERRIDES) instead of
            # silently discarding it the way a pure-blank-G case would.
            source_text, field_used, hint = f_text, "F (Current Source)", g_text
        else:
            source_text, field_used, hint = "", "NONE", g_text

        if not source_text:
            candidates.append(Candidate(
                driver_id=driver_id, commodity=root["commodity"], driver_name=root["driver_name"],
                region=root["region"], padma_source_status=str(status), padma_source_field_used=field_used,
                padma_source_text=g_text or f_text, case="skip",
                reason=(f"no usable URL in either column (G: {g_text!r}, F: {f_text!r})" if (g_text or f_text)
                        else "no source text in padma (both columns empty)"),
            ))
            continue

        url = normalize_url(source_text)
        access_mode, tier = guess_access_mode(url)
        base = baseline.get(driver_id)
        base_outcome = base["outcome"] if base else "not_in_baseline"
        case = "comparison" if base_outcome == "success" else "swap_in"

        candidate = Candidate(
            driver_id=driver_id, commodity=root["commodity"], driver_name=root["driver_name"],
            region=root["region"], padma_source_status=str(status), padma_source_field_used=field_used,
            padma_source_text=source_text, case=case, access_mode=access_mode, extraction_tier=tier,
            alt_source_url=url, baseline_outcome=base_outcome,
            baseline_coverage_ratio=base["coverage_ratio"] if base else 0.0,
            baseline_monthly_observations=base["monthly_observations"] if base else 0,
            source_hint=hint,
        )

        override = RESOLVED_OVERRIDES.get(driver_id)
        if override:
            candidate.access_mode = override.get("access_mode", candidate.access_mode)
            candidate.extraction_tier = override.get("tier", candidate.extraction_tier)
            candidate.resolved_connector = override.get("connector", "")
            candidate.resolved_locator = override.get("locator", "")
            candidate.resolved_credential_key = override.get("credential_key", "")
            candidate.resolved_note = override.get("note", "")
            candidate.resolved_is_proxy = bool(override.get("is_proxy", False))

        candidates.append(candidate)

    return candidates


# ---------------------------------------------------------------------------
# Phase 1: apply registry mutations for swap-in / comparison candidates
# ---------------------------------------------------------------------------


def _blank_row(fieldnames: list[str]) -> dict:
    return {fn: "" for fn in fieldnames}


def _row_fields_for_candidate(c: Candidate, driver_id: str, driver_name: str, transform_hint: str) -> dict:
    """Shared field-building logic for both the swap-in fallback row and the
    comparison probe row -- verified (RESOLVED_OVERRIDES) candidates get a
    real connector/locator/credential_key instead of a blank connector +
    generic scrape guess."""
    is_verified = bool(c.resolved_connector or c.resolved_locator)
    note = f" Verified: {c.resolved_note}" if c.resolved_note else ""
    return {
        "driver_id": driver_id,
        "commodity": c.commodity,
        "driver_name": driver_name,
        "region": c.region,
        "extraction_tier": str(c.extraction_tier),
        "tier_confidence": "High" if is_verified else "Low",
        "connector": c.resolved_connector,
        "source_name": "",
        "source_url": c.alt_source_url,
        "endpoint_or_locator": c.resolved_locator,
        "access_mode": c.access_mode,
        "native_frequency": "Monthly",
        "rollup_method": "mean",
        "unit": "",
        "history_from": "",
        "meets_min_history": "Y",
        "update_lag_days": "30",
        "is_proxy": "Y" if c.resolved_is_proxy else "N",
        "proxy_note": c.resolved_note if c.resolved_is_proxy else "",
        "human_in_loop": "N",
        "credential_key": c.resolved_credential_key,
        "fallback_driver_id": "",
        "data_quality_risk": "Medium" if is_verified else "High",
        "transform_hint": f"{transform_hint}{note}",
        "priority": "3",
        "enrichment_confidence": "",
    }


def apply_candidate_registry_rows(fieldnames: list[str], rows: list[dict],
                                   candidates: list[Candidate]) -> list[Candidate]:
    """Mutates `rows` in place (appends new rows, sets fallback_driver_id on
    existing rows for swap-in candidates). Returns the same candidates list
    with fallback_new_id / comparison_temp_id filled in."""
    by_id = {r["driver_id"]: r for r in rows}
    existing_ids = set(by_id.keys())

    for c in candidates:
        if c.case == "skip":
            continue

        if c.case == "swap_in":
            tail_id = c.driver_id
            while by_id[tail_id]["fallback_driver_id"]:
                tail_id = by_id[tail_id]["fallback_driver_id"]
            new_id = _dedupe_id(f"{tail_id}_PADMA", existing_ids)
            existing_ids.add(new_id)

            new_row = _blank_row(fieldnames)
            new_row.update(_row_fields_for_candidate(
                c, new_id, f"{c.driver_name} (padma alternate source)",
                f"[padma source-comparison {DATE_TAG}] swap-in candidate for a currently-failing driver; "
                f"source per padma {c.padma_source_field_used}."
                + (f" Hint: {c.source_hint!r}." if c.source_hint else ""),
            ))
            by_id[tail_id]["fallback_driver_id"] = new_id
            rows.append(new_row)
            by_id[new_id] = new_row
            c.fallback_new_id = new_id

        elif c.case == "comparison":
            temp_id = _dedupe_id(f"{c.driver_id}_CMPTEST", existing_ids)
            existing_ids.add(temp_id)

            temp_row = _blank_row(fieldnames)
            temp_row.update(_row_fields_for_candidate(
                c, temp_id, f"{c.driver_name} (padma alt-source comparison probe)",
                f"[padma source-comparison {DATE_TAG}] temporary probe row -- compares against "
                f"{c.driver_id}'s existing source; deleted after reconciliation regardless of outcome. "
                f"Source per padma {c.padma_source_field_used}."
                + (f" Hint: {c.source_hint!r}." if c.source_hint else ""),
            ))
            rows.append(temp_row)
            by_id[temp_id] = temp_row
            c.comparison_temp_id = temp_id

    return candidates


# ---------------------------------------------------------------------------
# manifest I/O
# ---------------------------------------------------------------------------


def write_manifest(candidates: list[Candidate], path: Path) -> None:
    path.write_text(json.dumps([asdict(c) for c in candidates], indent=2), encoding="utf-8")


def read_manifest(path: Path) -> list[Candidate]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [Candidate(**d) for d in data]


def write_registry(registry_path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with open(registry_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def backup_registry(registry_path: Path, tag: str) -> Path:
    ts = datetime.now().strftime("%H%M%S")
    backup_dir = registry_path.parent.parent / f".backup_before_{tag}_{DATE_TAG.replace('-', '')}_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(registry_path, backup_dir / registry_path.name)
    return backup_dir
