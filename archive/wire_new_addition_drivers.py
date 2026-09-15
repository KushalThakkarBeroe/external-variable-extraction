#!/usr/bin/env python3
"""
Wires real, live-verified connector/locator configuration into the 23
"new addition" drivers added earlier this session (previously blank
connector/access_mode -- hence never_attempted). Mirrors the same research
rigor as pipeline/source_comparison.py's RESOLVED_OVERRIDES: every entry
below was either live-checked this session or reuses an already-proven
working pattern elsewhere in this registry.

11 of the 23 get a real configuration (4 high-confidence, 3 medium-confidence
generic-scrape attempts, 4 correctly-built-but-blocked-on-a-missing-API-key).
The other 12 are left as-is with an explanatory transform_hint -- confirmed
via live checks this session to be genuinely not automatable right now
(bot-blocked, unreachable, JS-rendered with no discoverable table/file, or
no source information given at all), not silently skipped.

Example
-------
    python wire_new_addition_drivers.py
"""
from __future__ import annotations

import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.source_comparison import backup_registry, load_registry_rows, write_registry   # noqa: E402

ROOT = Path(__file__).parent
DATE_TAG = "2026-08-12"

# driver_id -> override dict. Only keys present are applied; everything else
# on the row is left as-is.
OVERRIDES: dict[str, dict] = {
    # ---- Tier A: high confidence ----
    "US_WHEAT_US_CORN_PRICES": {
        "connector": "fred", "endpoint_or_locator": "series_id:PMAIZMTUSDM",
        "access_mode": "api_key", "extraction_tier": "4", "credential_key": "fred_api_key",
        "note": "Reuses the exact same FRED series (World Bank global maize price) already "
                "working elsewhere in this registry (AU_PLT_CORN_FB) -- no new research needed.",
    },
    "POLAND_GASOLINE_FINAL_CONSUMPTION_TRANSPORT_SECTOR_ROAD_ENERGY_USE": {
        "connector": "eurostat", "endpoint_or_locator": "dataset:nrg_cb_oilm | filters:geo=PL",
        "access_mode": "open_api", "extraction_tier": "1",
        "note": "Dataset code (nrg_cb_oilm) was already sitting in the source URL padma gave.",
    },
    "INDIA_BLACK_PEPPER_INDIA_PEPPER_PRODUCTION_ESTIMATE": {
        "access_mode": "file_download", "extraction_tier": "2",
        "note": "Direct PDF link, live-verified reachable (200, real ~250KB PDF).",
    },
    "EU_SKIM_MILK_POWDER_WHEAT_PRICE_FEED_COST": {
        "access_mode": "file_download", "extraction_tier": "2",
        "source_url": "https://projectblue.blob.core.windows.net/media/Default/MI%20Reports/D%26A%20Arable/"
                       "Daily%20and%20Weekly%20Price%20Reports/Daily%20Futures%20Settlement%20Prices.xlsx",
        "note": "Found the actual downloadable XLSX linked from the AHDB futures-prices page "
                "(live-verified); source_url updated to the direct file so Tier 2 doesn't need "
                "to rediscover it.",
    },

    # ---- Tier B: medium confidence, worth a real attempt ----
    "INDIA_NATURAL_GAS_INDIA_MCP_DAYAHEAD_ELECTRICITY_PRICES": {
        "access_mode": "html_table", "extraction_tier": "1",
        "note": "IEX India homepage has one real <table> element (live-verified); not confirmed "
                "it's specifically the MCP day-ahead price table, so flagged as medium confidence.",
    },
    "BRAZIL_CORRUGATED_BOARDS_BRAZIL_INDUSTRIAL_ELECTRICITY_PRICES": {
        "access_mode": "multipage_scrape", "extraction_tier": "3",
        "source_url": "https://www.gov.br/aneel/pt-br/assuntos/tarifas",
        "note": "Original source ('www.gov.br') was too vague to attempt at all -- resolved to "
                "ANEEL's actual tariff data portal (Brazil's electricity regulator), live-verified "
                "reachable with real content.",
    },
    "BRAZIL_PULP_BRAZIL_INDUSTRIAL_ELECTRICITY_PRICES": {
        "access_mode": "multipage_scrape", "extraction_tier": "3",
        "source_url": "https://www.gov.br/aneel/pt-br/assuntos/tarifas",
        "note": "Same fix as the Corrugated Boards driver above -- same underlying data need "
                "(Brazil industrial electricity prices), same real source.",
    },

    # ---- Tier C: correctly built, blocked only on the missing comtrade_api_key ----
    "BRAZIL_CORRUGATED_BOARDS_EXPORT_DEMAND": {
        "connector": "un_comtrade", "endpoint_or_locator": "cmdCode=4819;flowCode=X;reporterCode=76",
        "access_mode": "api_key", "extraction_tier": "4", "credential_key": "comtrade_api_key",
        "note": "HS 4819 (cartons/boxes of corrugated paper/paperboard), Brazil (76) as exporter.",
    },
    "BRAZIL_PULP_BRAZIL_PULP_EXPORTS": {
        "connector": "un_comtrade", "endpoint_or_locator": "cmdCode=4703;flowCode=X;reporterCode=76",
        "access_mode": "api_key", "extraction_tier": "4", "credential_key": "comtrade_api_key",
        "note": "HS 4703 (chemical wood pulp, sulphate), Brazil (76) as exporter.",
    },
    "GERMANY_SKIM_MILK_POWDER_GERMANY_SMP_EXPORT_VOLUMES": {
        "connector": "un_comtrade", "endpoint_or_locator": "cmdCode=040210;flowCode=X;reporterCode=276",
        "access_mode": "api_key", "extraction_tier": "4", "credential_key": "comtrade_api_key",
        "note": "HS 0402.10 (skimmed milk powder), Germany (276) as exporter.",
    },
    "GLOBAL_BLACK_PEPPER_INDIA_VIETNAM_BRAZIL_PEPPER_EXPORTS_DATA": {
        "connector": "un_comtrade", "endpoint_or_locator": "cmdCode=090411;flowCode=X;reporterCode=699",
        "access_mode": "api_key", "extraction_tier": "4", "credential_key": "comtrade_api_key",
        "note": ("HS 0904.11 (pepper, neither crushed nor ground). Driver covers India/Vietnam/"
                 "Brazil combined -- Comtrade needs one reporter per row, so India (699) was "
                 "chosen as primary; Vietnam and Brazil are not covered by this single locator. "
                 "padma's suggested source (Trademap) needs registration for bulk data, so routed "
                 "through Comtrade instead, same as the padma-151 batch's Trademap-family rows."),
    },
}

# Genuinely not automatable right now -- confirmed via live checks this
# session, left as-is with an explanation rather than silently blank.
NOT_AUTOMATABLE_NOTES: dict[str, str] = {
    "SPAIN_ELECTRICITY_ROTTERDAM_COAL_FUTURES_BENCHMARK_FOR_EUROPE_COAL_PRICES":
        "investing.com returns HTTP 403 (bot-blocked) on a plain request -- live-verified this session.",
    "GERMANY_SKIM_MILK_POWDER_ENERGY_DUTCH_TTF_NATURAL_GAS_PRICES":
        "Same investing.com page family -- HTTP 403 bot-blocked, live-verified.",
    "INDIA_NATURAL_GAS_INDONESIA_COAL_PRICES_LARGEST_COAL_SUPPLIER_TO_INDIA":
        "coalspot.com is unreachable (connection refused) -- live-verified this session.",
    "AUSTRALIA_CHEESE_AUSTRALIA_CHEESE_PRODUCTION_DAIRY_AUSTRALIA":
        "dairyaustralia.com.au page loads but has zero <table> elements and zero downloadable "
        "file links (JS-rendered dashboard) -- live-verified; matches the same site's earlier "
        "confirmed failure this session for a different driver.",
    "AUSTRALIA_POULTRY_WHEAT_PRICE_FEED_COST":
        "Same dairyaustralia.com.au issue -- zero tables/files found on live check.",
    "INDIA_NATURAL_GAS_INDIA_ELECTRICITY_GENERATIONCONSUMPTION":
        "npp.gov.in/dgrReports loads 1MB+ of HTML but zero tables/files found (JS single-page "
        "app) -- live-verified.",
    "INDIA_NATURAL_GAS_INDIA_COAL_SUBSTITUTE_INVENTORY":
        "Same npp.gov.in issue -- zero tables/files found on live check.",
    "BRAZIL_ORANGE_BRAZIL_FRESH_ORANGE_AND_JUICE_PRODUCTION":
        "padma gave no source at all (both Current Source and Alternate Source blank) -- needs "
        "a source identified from scratch, not just a retrieval method.",
    "BRAZIL_ORANGE_BRAZIL_FRESH_ORANGE_AND_PROCESSING_DEMAND":
        "Same as above -- no source given at all.",
    "GLOBAL_PALM_OIL_PALM_OIL_GLOBAL_STOCKS_TO_USE_RATIO":
        "'Stocks to use ratio' is a derived metric (ending stocks / consumption) -- the usda_psd "
        "connector fetches one raw attribute per row and cannot compute a ratio from two. Needs "
        "a dedicated derived-ratio connector (same limitation flagged earlier this session for "
        "AU_PLT_CORN), not a locator fix.",
    "MALAYSIA_PALM_OIL_PALM_OIL_PRODUCTION":
        "Source is MPOB (Malaysian Palm Oil Board); this same institution was already confirmed "
        "'no automatable access path' for a different Palm Oil driver in this session's pilot run.",
    "NORTH_AMERICA_WHEAT_US_ENDING_STOCKSTOUSE_RATIO":
        "Same derived-ratio limitation as the Palm Oil stocks-to-use driver above.",
}


def main() -> int:
    registry_path = ROOT / "input/driver_registry.csv"
    fieldnames, rows = load_registry_rows(registry_path)
    by_id = {r["driver_id"]: r for r in rows}

    backup_dir = backup_registry(registry_path, "wire_new_addition_drivers")
    print(f"Backed up registry to {backup_dir}")

    applied = 0
    for driver_id, override in OVERRIDES.items():
        row = by_id.get(driver_id)
        if row is None:
            print(f"WARNING: {driver_id} not found in registry, skipping")
            continue
        for field in ("connector", "endpoint_or_locator", "access_mode", "extraction_tier",
                      "credential_key", "source_url"):
            if field in override:
                row[field] = override[field]
        row["human_in_loop"] = "N"
        row["tier_confidence"] = "Medium"
        row["transform_hint"] = f"[new-addition wiring {DATE_TAG}] {override['note']}"
        applied += 1

    flagged = 0
    for driver_id, note in NOT_AUTOMATABLE_NOTES.items():
        row = by_id.get(driver_id)
        if row is None:
            print(f"WARNING: {driver_id} not found in registry, skipping")
            continue
        row["transform_hint"] = f"[new-addition wiring {DATE_TAG}] NOT AUTOMATABLE: {note}"
        flagged += 1

    write_registry(registry_path, fieldnames, rows)
    print(f"Wired real configuration into {applied} driver(s); flagged {flagged} as confirmed "
          f"not-automatable (with reasons) rather than left silently blank.")
    print(f"Total: {applied + flagged} of 23 accounted for.")

    ids_path = ROOT / "output/new_addition_ids_to_run.txt"
    ids_path.write_text("\n".join(OVERRIDES.keys()) + "\n", encoding="utf-8")
    print(f"\nWrote {len(OVERRIDES)} runnable driver_id(s) -> {ids_path}")
    print("Next: python run_pipeline.py --driver-id-file output/new_addition_ids_to_run.txt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
