# Implementation Summary: Minimal-Input Enrichment + Frankfurter Fallbacks + .env Secrets Management

**Date**: 2026-07-27  
**Status**: Complete — Ready for testing

## Overview

Three major enhancements implemented:

1. **Minimal-input driver enrichment** — Auto-generate driver metadata from seed file (commodity, driver_name, region, start_date, unit)
2. **Frankfurter FX fallback** — Automatic keyless ECB exchange-rate fallbacks for FRED FX drivers
3. **Centralized secrets via .env** — All API keys and login credentials now managed via environment variables and `.env` file

---

## Part A: Minimal-Input Registry Enrichment

### New Files
- **`input/new_drivers.xlsx`** — Seed file (5 columns: commodity, driver_name, region, start_date, unit)
  - User populates this; enrichment auto-generates driver metadata
  - Separate from `driver_registry.csv` (which remains the control plane the code reads)

- **`pipeline/registry_enrichment.py`** — New module
  - Entry point: `enrich_new_drivers(cfg, llm, registry_path, seed_path) -> int`
  - Deduplication key: `(commodity, driver_name, region, unit)` normalized for case/whitespace
  - `start_date` syncs to `history_from` per-driver (not part of dedup key, so edits don't create duplicates)
  - Calls new `LlmHelper.propose_source()` job to guess metadata
  - Auto-creates Frankfurter fallback rows for any FRED FX driver (see Part B)
  - Logs low-confidence guesses (< 0.4) for analyst review

### Modified Files
- **`pipeline/llm.py`** — Added 5th LLM job
  - `propose_source(commodity, driver_name, region, unit) -> Optional[dict]`
  - Returns: source_name, source_url, connector, access_mode, endpoint_or_locator, declared_tier, native_frequency, rollup_method, confidence, reasoning
  - Soft guardrail: informs model of available connectors; proposes metadata only (not values, so no hallucination of numbers)

- **`pipeline/connectors.py`** — Updated `_window()` function
  - Now accepts optional `spec` parameter
  - Uses `spec.history_from` (per-driver start date) if present; falls back to global `run.start_date`
  - Updated all 4 call sites: `bcb_sgs`, `fred`, `nass_quickstats`, `un_comtrade` to pass `spec`

- **`run_pipeline.py`** — Wiring enrichment into the pipeline
  - Moved `LlmHelper(cfg)` construction earlier (right after `cfg.ensure_dirs()`)
  - Added enrichment call before `load_registry()`: `enrich_new_drivers(cfg, llm, registry_path, seed_path)`
  - Graceful degradation: if LLM disabled, logs warning and continues (unenriched rows skip to next run)

- **`config/config.yaml`** — New settings
  - Added `registry:` section with `new_drivers_seed: "input/new_drivers.xlsx"` and `auto_enrich_new_drivers: true`

- **`input/driver_registry.csv`** — New column + fallback updates
  - Added `enrichment_confidence` column (empty for analyst-authored rows, raw float for LLM-enriched rows)
  - Updated fallback_driver_id for 3 existing FRED FX drivers to point to Frankfurter counterparts (auto-created on enrichment run)

---

## Part B: Frankfurter FX Fallback (Keyless ECB Exchange Rates)

### New Connector
- **`frankfurter()` in `pipeline/connectors.py`**
  - API: `GET https://api.frankfurter.dev/v1/{start}..{end}?base=USD&symbols=EUR`
  - Locator format: `base:USD | target:EUR`
  - No credentials required (keyless ECB rates)
  - History starts 1999-01-04 (caveat: can't backstop FRED FX before that date)
  - Returns `[obs_date, value]` same as other connectors

### Automatic Fallback Generation
- **Enrichment logic auto-detects FRED FX drivers** (via regex on unit column)
- **For each FRED FX driver**:
  - If no Frankfurter fallback exists, creates one automatically
  - Points primary driver's `fallback_driver_id` to the generated row
  - Applies to both existing (manually-authored) and newly-enriched drivers
  - Completely generic — no hardcoding of specific currencies

### Existing FRED FX Drivers (Now Have Automatic Fallbacks)
1. `LA_PLP_FX_FB` (USD/BRL) → `LA_PLP_FX_FRANK` (auto-created on first enrichment run)
2. `DE_SMP_FXNZD` (USD/NZD) → `DE_SMP_FXNZD_FRANK`
3. `DE_SMP_FXEUR` (USD/EUR) → `DE_SMP_FXEUR_FRANK`

---

## Part C: Centralized Secrets via .env

### New Files
- **`.env.example`** — Safe-to-commit template with all env var names and blank values
- **`.env`** — Real secrets (user-created by copying `.env.example` and filling in real values)
  - Should be added to `.gitignore` (already included)
- **`.gitignore`** — Project-level (first time; was only in `.venv/` before)
  - Includes `.env`, `__pycache__/`, `output/`, etc.

### Modified Files
- **`requirements.txt`** — Added `python-dotenv>=1.0`

- **`pipeline/config.py`** — Enhanced `Config.load()` to load `.env`
  - Calls `load_dotenv(root / ".env")` before parsing YAML (gracefully skips if file missing or dotenv not installed)
  - Existing `_resolve()` function already handles `${ENV:VAR}` placeholders in YAML

- **`config/config.yaml`** — All secrets now reference environment variables
  ```yaml
  llm:
    model: "${ENV:ANTHROPIC_MODEL}"
    api_key: "${ENV:ANTHROPIC_API_KEY}"
  credentials:
    fred_api_key: "${ENV:FRED_API_KEY}"
    eia_api_key: "${ENV:EIA_API_KEY}"
    usda_nass_api_key: "${ENV:USDA_NASS_API_KEY}"
    usda_fas_api_key: "${ENV:USDA_FAS_API_KEY}"
    comtrade_api_key: "${ENV:COMTRADE_API_KEY}"
    mla_username: "${ENV:MLA_USERNAME}"
    mla_password: "${ENV:MLA_PASSWORD}"
    fastmarkets_api_key: "${ENV:FASTMARKETS_API_KEY}"
    euromonitor_api_key: "${ENV:EUROMONITOR_API_KEY}"
  ```

### EIA Connector (Deferred)
- Credential slot reserved (`eia_api_key: "${ENV:EIA_API_KEY}"`)
- Placeholder `eia()` connector skeleton added to `connectors.py` (fully functional but generic for any EIA dataset)
- Will only be used when LLM enrichment or manual registry entry proposes an EIA-backed driver
- Once a real EIA series is identified, update the connector's route/series_id parsing logic

---

## Testing Checklist

### Part A: Enrichment
- [ ] Create `input/new_drivers.xlsx` with 2-3 test rows (commodity, driver_name, region, start_date, unit)
- [ ] Run: `python run_pipeline.py --dry-run`
- [ ] Verify: new rows appended to `driver_registry.csv` with plausible metadata and `enrichment_confidence` populated
- [ ] Re-run: `python run_pipeline.py --dry-run` again — new rows should NOT duplicate (idempotency test)
- [ ] Check manifest for enrichment-confidence and tier-suggestion fields

### Part B: Frankfurter Fallback
- [ ] Blank `fred_api_key` in config (simulate missing FRED key)
- [ ] Run: `python run_pipeline.py --driver-id DE_SMP_FXNZD`
- [ ] Verify: run manifest shows `served_by_driver_id=DE_SMP_FXNZD_FRANK`, monthly panel populated (not HITL task)
- [ ] Check `frequency_detected` on Frankfurter rows (daily ECB data)

### Part C: .env Secrets
- [ ] Copy `.env.example` → `.env`
- [ ] Fill in real values: `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`, `FRED_API_KEY`, etc.
- [ ] Run: `python run_pipeline.py --dry-run`
- [ ] Verify: config resolves all env vars correctly (check log for any `"not set"` warnings)
- [ ] Optional: run full pipeline to confirm API calls use the env-resolved credentials

### End-to-End
- [ ] Full run: `python run_pipeline.py --commodity "Skim Milk Powder"`
- [ ] Verify: tracker xlsx and monthly panel populated, no spurious HITL tasks for enriched drivers
- [ ] Spot-check: `run_manifest.json` for `served_by_driver_id` (shows which fallbacks were used)

---

## Key Design Decisions

1. **Dedup key excludes start_date** — allows analysts to edit fetch windows without creating duplicates
2. **Enrichment is one-shot** — runs before registry load, appends to CSV once
3. **LLM proposes, pipeline verifies** — wrong metadata guesses caught by actual fetch failure (no silent hallucination)
4. **Frankfurter fallback is automatic** — any FRED FX driver (existing or new) gets a Frankfurter fallback auto-created without manual wiring
5. **Secrets never touch git** — `.env` is gitignored, only `.env.example` is committed
6. **Per-driver history_from** — reuses existing `history_from` column for per-driver start dates, no schema change needed

---

## No Changes Needed

- **`cascade.py`** — fallback mechanism already generic; works for Frankfurter rows as-is
- **`registry.py`** — validation already tolerates blank optional columns; enrichment_confidence is optional and ignored
- **Tier handlers** — no changes; connectors are registered in the CONNECTORS dict and dispatched uniformly

---

## Files Created/Modified Summary

| File | Change |
|---|---|
| `input/new_drivers.xlsx` | NEW |
| `pipeline/registry_enrichment.py` | NEW |
| `.env.example` | NEW |
| `.gitignore` | NEW |
| `pipeline/llm.py` | +propose_source() job |
| `pipeline/connectors.py` | +frankfurter(), +eia() (generic skeleton), thread spec through _window(), register in CONNECTORS |
| `run_pipeline.py` | move LlmHelper earlier, add enrich_new_drivers() call |
| `pipeline/config.py` | load .env on Config.load() |
| `requirements.txt` | +python-dotenv |
| `config/config.yaml` | +registry section, llm.model to ${ENV:}, all credentials to ${ENV:} |
| `input/driver_registry.csv` | +enrichment_confidence column, fallback_driver_id refs updated for 3 FX drivers |

---

## Future Enhancements (Out of Scope)

- EIA connector: build route/series parsing once a real EIA driver is proposed by enrichment
- FRED server-side aggregation: add `frequency=m&aggregation_method=avg` to FRED connector (deferred per earlier discussion)
- Tracker pre-filling: document workaround (run enrichment once, copy generated driver_id into pre-filled tracker column header)
- Test suite: none exists yet in this repo
