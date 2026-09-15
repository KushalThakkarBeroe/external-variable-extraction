# Commodity Driver Pipeline — Session Handoff (2026-08-05)

Project: `commodity_driver_pipeline/commodity_driver_pipeline` — 4-tier cascading extraction pipeline, ~40 commodities, `input/driver_registry.csv` (583 rows after this session; 581 before). Entry points: `run_pipeline.py` (sequential), `run_pipeline_parallel.py` (8 workers, use this one). Standing rule: no commodity-specific hardcoding — every fix is structural; only registry *data* is commodity-specific.

## Baseline at start of this session

`output/monthly/run_report_20260804_065530.xlsx` (still intact, untouched, the authoritative pre-session baseline):
**510 attempted, 159 succeeded (105 clean / 54 partial), 351 failed — 31.2%.**

## Scope decision (confirmed with user this session)

Active work targets drivers with a **real automated source that failed** — excludes the "manual-upload category" (`access_mode` in `paid_or_restricted`/`login`, or `connector==manual_yaml`). Deliberately deferred (don't fix an existing failure, so out of scope): **ARMS pre-check, Statistics Sweden pulp connector, Australia ABS corn connector.**

---

## Workstreams completed this session

### Persistence / data-safety
- New `pipeline/atomic_io.py::write_atomic()` — temp-file-then-`os.replace` helper. Applied to every write site: `storage.py` (manifest, HITL yaml, trackers, report, checkpoint log), `success_flags.py`, `alternate_sources.py`, `eurostat_repair.py`, `registry_enrichment.py` (registry CSV writes).
- New `OutputWriter.archive_raw_files()` — permanent per-run copy of raw files behind successful file-based drivers → `output/raw_archive/<timestamp>/`. `raw_artifact_path` threaded through `tiers/base.py::_guard()` via a DataFrame `.attrs` convention, stamped in `tiers/tier2_files.py`.
- New manifest fields: `raw_artifact_path`, `manual_drop`.

### Reporting / diagnosability fixes
- **Bot-block flag fix**: `HttpClient.attempted_hosts_by_driver` (new) tracks the real host(s) each driver actually contacted; `storage.py::finalize_bot_block_flags()` now checks that instead of comparing the registry's human-facing `source_url` (previously a different host entirely, e.g. `comtradeplus.un.org` vs. the real API host `comtradeapi.un.org` — blocked drivers were never correctly flagged).
- **HITL reason mislabeling fix**: `cascade.py::_record_hitl()` now uses the real last-tier failure (`last_failure_result`) instead of inferring from static `credential_keys`/`access_mode`. Confirmed live this session: PSD HITL tasks now correctly say `ValueError: USDA PSD returned no records for commodity X (country)` instead of the old, false "Free API key required."
- Label clarity: `succeeded_with_quality_notes` → `succeeded_with_quality_notes_this_run`; added `served_by_proxy_success` alongside `proxy_tagged_attempts`.

### Host-blocking redesign + rate limiting (highest-value fix)
- `http_client.py`: replaced the flat "one 403 blocks the host forever" set with **corroboration + cooldown** — `http.bot_block_confirmation_threshold` (default 2), `http.bot_block_cooldown_seconds` (900), `http.bot_block_max_cooldown_multiplier` (8). Confirmed live this run: `data.stats.gov.cn` correctly required 2 signals before blocking, then cooled down instead of being blacklisted for the rest of the run.
- New `RateLimiter` class — **proactive** per-host request budgeting, `http.source_limits` config. Real, sourced limits (nothing guessed): **UN Comtrade 500/day, FRED 120/min, Statistics Sweden (SCB) 150/min**. EIA/SIDRA/USDA/Eurostat/World Bank/BIS have no officially published limit — deliberately left unconfigured.
- New `DeferredCallError`, `load_deferred_driver_ids()`, new "Deferred (Rate-Limited)" report sheet.
- 13 passing unit tests: `pipeline/tests/test_http_client_blocking.py`, `test_rate_limiter.py`, `pipeline/tests/_fakes.py` (project's first test suite).

### Locator corrections + pre-flight validator (16 registry rows)
Real connector bug found and fixed: `connectors.py::un_comtrade()` was **silently dropping `partnerCode`** for 9 rows (returning aggregate world trade instead of the intended bilateral figure) — now supported.

New generalized validator in `registry_validator.py`: `_RECOGNIZED_LOCATOR_KEYS`/`_RECOGNIZED_FILTER_KEYS` (catches wrong-key-name locators that silently fall back to a connector's default) + numeric-format checks (catches free-text-instead-of-code). Run against all 581 rows; found/fixed everything below plus 2 new dead-text SIDRA fragments (not fixed, harmless) and confirmed 5 vestigial `freq`/`period` Comtrade keys (harmless, connector already loops monthly regardless).

**Rows fixed** (all with live-verified replacement values, documented in each row's `transform_hint` with a `[ws3 2026-08-05]` tag):
| driver_id | fix |
|---|---|
| `EU_ATLANTIC_COD_EU_WHITEFISH_IMPORT_DEMAND` | Comtrade: `reporterCode=97` (EU), fixed key names — **live-verified success, 48 obs** |
| `EUROPE_ALUMINUM_TRADE_VOLUME_IMPORTSEXPORTS` | Comtrade: `reporterCode=97`, `cmdCode=76`, fixed key names |
| `LATIN_AMERICA_ETHANOL_DEMAND_SUBJECT_TO_DATA_AVAILABILITY` | Comtrade: no LatAm aggregate exists (verified against Comtrade's own reference table) — `reporterCode=all` global proxy |
| `MEA_GLASS_BOTTLES_GLASS_BOTTLES` | Comtrade: no MEA aggregate exists — `reporterCode=all` global proxy, `cmdCode=7010` |
| `MEA_GLASS_BOTTLES_TRADE_IMPORTSEXPORTS` | Comtrade: same MEA-aggregate caveat |
| `LATIN_AMERICA_CORRUGATED_BOARDS_DEMAND_PRODUCER_PRICE_INDEX_PAPER_MANUFACTURING` | SIDRA table 6903, `variable:63`→`10008` (verified live against table's own descriptor) |
| `BRAZIL_ETHANOL_PHARMACEUTICAL_PRODUCTION_INDEX_BRAZIL` | SIDRA table 8888, `variable:'products pharmaceutical'`→`12606` |
| `LATIN_AMERICA_CORRUGATED_BOARDS_CORRUGATED_BOARDS` | SIDRA table **7060 was confirmed live to be Brazil's IPCA (wrong table entirely)** → repointed to table 8888, `variable:12606` |
| `INDIA_HDPE_DEMAND_INDUSTRIAL_PRODUCTION_INDEX_INDIACHINA` | FRED series swap → `PRMNTO01INA657S` |
| `CHINA_LCI_CHINA_CHINA_GDP_GROWTH_RATE` | FRED series swap → `NAEXKP01CNA657S` (annual, not quarterly — no quarterly variant exists under this ID pattern) |
| `MEA_GLASS_BOTTLES_INTEREST_RATE` | FRED series swap → `INTGSTSAM193N` (old series real but discontinued, data ends 2013) |
| `GLOBAL_PPI_GLOBAL_SUPPLY_CHAIN_PRESSURE_INDEX` | **Reclassified off FRED entirely** — GSCPI is an NY Fed publication, not a FRED series (confirmed live). Now `connector=""`, generic Tier-2 file discovery of `newyorkfed.org/research/policy/gscpi`'s `gscpi_data.xlsx` |
| `GLOBAL_OCEAN_FREIGHT_BUNKER_FUEL_PRICE` | Reclassified `eia`→`fred`, series `DHOILNYH` (EIA has no bunker-fuel series; heating-oil proxy is standard practice) — **live-verified success** |
| `US_BENZENE_US_SHALE_GAS_ETHANECRACKING_ECONOMICS` | Reused `NG.RNGWHHD.D` (Henry Hub, already working elsewhere in registry) — **live-verified success** |
| `ASIA_PARACETAMOL_BENZENE_PRICES_ASIA` | **Left unfixed on purpose** — EIA has no benzene series; documented in transform_hint, needs reclassification to a real petrochemical price source, not a locator fix |
| `ASIA_STYRENEBUTADIENE_RUBBER_SBR_ONE_STEP_AHEAD_FEEDSTOCK_BENZENEETHYLENE_PRICES` | Same — EIA has no ethylene series either |

`LATIN_AMERICA_ETHANOL_CRUDE_OIL_GASOLINE_PRICE_RBOB` (EIA) deliberately left untouched — already succeeds via a fallback rescue.

### Eurostat mining (no fix applied)
Mined `output/rejected_eurostat_locator_repairs.yaml` (27 entries) — none have exhausted the 3-attempt repair cap; mechanism is working as designed, will keep retrying automatically on future runs. No tuning bug found.

### Fallback chain audit
- **False alarm corrected**: the "Atlantic Cod ocean-temperature blank-connector fallback" flagged in the old handoff was verified live to already work fine (blank connector + `file_download` routes through Tier 2's generic file discovery) — no fix needed.
- Static-audited all 71 fallback targets via the new validator — only 1 harmless flag (a dead SIDRA `classification:` text fragment).
- New `pipeline/fallback_audit.py::record_fallback_audit()` → `output/fallback_audit_log.csv`, upserted every run for whichever fallback targets actually get exercised. Wired into both entry points.

### USDA PSD locator fixes (7 registry rows)
| driver_id | fix |
|---|---|
| `AU_PLT_TRADE_FB` | Wrong key names (`commodityCode=`/`attribute=`) fixed to `commodity:`/`attribute:` |
| `INDONESIA_PALM_OIL_MALAYSIA_INDONESIA_PRODUCTION` | `commodity:4243000` (Palm Oil — **confirmed live** against USDA's own page titled "Palm Oil") |
| `NORTH_AMERICA_WHEAT_DEMANDSUPPLY_ANNUAL_DATA` | `commodity:0410000`, `attribute:176` |
| `BRAZIL_ORANGE_FLORIDA_BRAZIL_CITRUS_PRODUCTION_FORECASTS` | `commodity:0571000`, `attribute:88` |
| `BRAZIL_ORANGE_BRAZILGLOBAL_RETAIL_JUICE_DEMAND_TREND` | `commodity:0571000`, `attribute:125` |
| `BRAZIL_ORANGE_MEXICOBRAZIL_COMPETING_CITRUS_EXPORTS` | `commodity:0571000`, `attribute:88` — **succeeded this run, but via an alternate-source-search rescue at Tier 1, not via PSD itself** (PSD API is down, see below) |
| `LATIN_AMERICA_ORANGE_DEMAND_ORANGE_PROCESSING_VOLUME` | `commodity:0571000` (attribute was already numeric) |

**Caveat on attribute codes**: PSD's live API has been down (HTTP 500, confirmed still ongoing 2026-08-05) since before this session, so attribute codes (88/125/176) could not be live-verified — they were reused from other rows in this same registry already confirmed working against the real API historically. Flagged as uncertain in each row's transform_hint; verify once the outage clears.

### Quality-gate mining + tunability (12 registry rows)
- **Phase A (mining)**: read the baseline report's real `quality_notes` — confirms the *old handoff's* "28/44/10" figures were actually correct, they just needed to come from the report, not the registry: **28 history-too-short, 43 stale, 15 sparse-density (no tunable knob exists for this), 10 frequency-mismatch (info-only)**.
- **Phase B (applied, clean cases only)**: 9 `min_history_years` overrides for rows that are short-history but NOT also stale (current, just younger sources) — `EU_ATLANTIC_COD_NOKUSD_EURUSD_EXCHANGE_RATE`, `MEA_GLASS_BOTTLES_BRENT_CRUDE_OIL`, `EU_GLASS_BOTTLES_EXCHANGE_RATE_VS_USDEUR`, `GLOBAL_GLYCERIN_CRUDE_OIL_PRICE_SYNTHETIC_GLYCERIN_ROUTE`, `EU_GLYCERIN_EUGERMANY_NATURAL_GAS_PRICE_TTF`, `EU_MAGNESIUM_STEARATE_MACRO_EXCHNAGE_RATE_EURUSD`, `EUROPE_MAGNESIUM_STEARATE_TALLOWVEGETABLE_OIL_FEEDSTOCK_PRICE`, `GLOBAL_PARACETAMOL_CRUDE_OIL_PRICE_PETROCHEMICAL_FEEDSTOCK_CHAIN`, `INDIA_PARACETAMOL_USDINR_USDCNY_EXCHANGE_RATE`. Plus 3 `update_lag_days`/`native_frequency` fixes for genuinely annual/quarterly-cadence rows: `US_WHEAT_LABOR_US_FARM_WAGES`, `HUNGARY_AVERAGE_WAGES_HUNGARY_LABOR_PRODUCTIVITY_GROWTH_KSH` (native_frequency corrected Quarterly→Annual), `USA_PPI_LABOR_COSTS_UNIT_LABOR_COST_INDEX`.
- **Deliberately NOT touched**: the other ~19 short-history rows and ~35 stale Monthly-frequency rows — these are BOTH short AND severely stale (e.g. latest data from 2014-2018), which points at a dead/broken connector, not a legitimate threshold need. Touching `min_history_years`/`update_lag_days` there would mask a real bug. Left as a follow-up item (needs per-row investigation, similar effort to the Eurostat mining).
- Fixed a real bug: `storage.py::_add_locked()`'s `meets_min_history` field wasn't respecting the per-driver `min_history_years` override (now mirrors `cascade.py::_quality_gate()`'s logic exactly).
- Added `VALID_FREQUENCY` load-time validation in `registry.py` (mirrors the existing `rollup_method`/`access_mode` pattern) — 0 violations found on the current registry.

### Rerun exclusion mechanism
- **Manual-drop gap fixed**: moved the manual-file check to a pre-tier-0 step in `cascade.py::run_driver()` (new `_check_manual_drop()`) — previously only reached for gated (`api_key`/`login`/`paid_or_restricted`) drivers via `Tier4Gated`, silently never checked for most of the registry (`open_api`/`html_table`/`file_download`). Shared `manual_file()`/`manual_drop_path()` extracted to `tiers/base.py`.
- New CLI flags on both entry points: `--driver-id-file <path>` (bulk driver-id input, one per line) and `--resume-deferred` (reads WS2's deferred-driver state). New `pipeline/registry.py::resolve_driver_ids()` merges all three input modes.
- New `pipeline/rerun_worklist.py` (`build_worklist_df`, `write_worklist_excel`) + `generate_rerun_worklist.py` — produces exactly "automated source, not manual-upload, currently failing" driver list. **Verified this session: correctly produced 185 drivers.**
- New `OutputWriter.merge_with_existing_manifest()` — a scoped run now carries forward every driver_id from the existing manifest it didn't touch, instead of overwriting wholesale. Verified with a functional test. **Important caveat**: only merges from whatever's already on disk at run time — see "Operational notes" below.

---

## Validation run results (this session)

Ran the 185-driver worklist via `run_pipeline_parallel.py --driver-id-file output/rerun_worklist_ids.txt`. Output: `output/monthly/run_report_<timestamp started ~2026-08-05 10:02 UTC>.xlsx`, `output/run_manifest.json` (185 entries), `output/human_in_the_loop_tasks.yaml` (167 tasks).

**185 attempted → 20 succeeded (10.8%), 165 failed.**

| | Count | % |
|---|---|---|
| Succeeded, Clean | 4 | 2.2% |
| Succeeded, Partial (quality notes) | 16 | 8.6% |
| — Direct/original source | 16 | 8.6% |
| — Alternate source search | 4 | 2.2% |
| Failed / No data | 165 | 89.2% |

**Attribution of the 20 gains**:
- 8 recovered by the Workstream 2 host-blocking fix (Comtrade drivers previously killed by the one-403-blocks-forever bug): `RUSSIA_PLATINUM_RUSSIA_PGM_EXPORT_VOLUMES`, `EUROPE_GLYCERIN_TRADE_GLYCERIN_IMPORTSEXPORTS`, `EUROPE_PROPYLENE_TRADE_IMPORTEXPORT`, `DE_SMP_CNIMPORT`, `CHINA_SKIM_MILK_POWDER_CHINA_DAIRY_IMPORT_VOLUMES`, `EU_SKIM_MILK_POWDER_EXPORTS_EUALGERIAEGYPTINDONESIA`, `AUSTRALIA_POULTRY_AUSTRALIA_PROTEIN_MEAL_SOYBEANCANOLA_MEAL_COST`, `AUSTRALIA_POULTRY_GLOBAL_POULTRY_TRADE_VOLUMES`
- 3 direct Comtrade locator fixes: `EU_ATLANTIC_COD_EU_WHITEFISH_IMPORT_DEMAND`, `EUROPE_ALUMINUM_TRADE_VOLUME_IMPORTSEXPORTS`, `EU_MAGNESIUM_STEARATE_TRADE_EU_IMPORTSEXPORTS`
- 4 FRED/EIA fixes: `CHINA_LCI_CHINA_CHINA_GDP_GROWTH_RATE`, `INDIA_HDPE_DEMAND_INDUSTRIAL_PRODUCTION_INDEX_INDIACHINA`, `GLOBAL_OCEAN_FREIGHT_BUNKER_FUEL_PRICE`, `US_BENZENE_US_SHALE_GAS_ETHANECRACKING_ECONOMICS`
- 5 unrelated wins (live alternate-source search / transient): `AUSTRALIA_POULTRY_AUSTRALIA_SUBSTITUTE_MEAT_PRICES_BEEF_LAMB`, `BRAZIL_ORANGE_MEXICOBRAZIL_COMPETING_CITRUS_EXPORTS`, `EU_ELECTRICITY_EU_GAS_STORAGE`, `INDIA_CARBON_STEEL_INDIA_DOMESTIC_IRON_ORE_PRICE_NMDCODISHA`

**Density check** (coverage_ratio, measured against each driver's own actual data span, not a fixed window): 17 of 20 have ≥40% of months filled — 17 are actually a clean 100%. The 3 below 40%: `CHINA_LCI_CHINA_CHINA_GDP_GROWTH_RATE` (9.1%, genuinely annual FRED series on a monthly grid), `INDIA_HDPE_DEMAND_INDUSTRIAL_PRODUCTION_INDEX_INDIACHINA` (9.2%, same reason), `DE_SMP_CNIMPORT` (33.3%, genuine Comtrade reporting gaps).

**USDA PSD confirmed still down**: `apps.fas.usda.gov/OpenData/api/psd` returns HTTP 500 for every commodity/country/year (confirmed live in this run's log, `output/pipeline_20260805_043249.log`). 10 of 11 `usda_psd`-connector drivers still fail for this reason — external outage, unrelated to this session's locator fixes, which are ready to work once it recovers.

## Combined picture: baseline (159) + this run (20)

**Verified zero duplicate driver_ids between the two sets.**

| Metric | Count | % of 510 |
|---|---|---|
| **Total succeeded** | **179** | **35.1%** (was 31.2%) |
| — Clean | 109 | 21.4% |
| — Partial | 70 | 13.7% |
| — Reused from prior run | 2 | 0.4% |
| — Direct/original source | 139 | 27.3% |
| — Registered fallback | 18 | 3.5% |
| — Reclassification rescue | 9 | 1.8% |
| — Alternate source search | 10 | 2.0% |
| — Eurostat locator repair | 1 | 0.2% |
| — ≥40% coverage of successes | 164 of 179 | 91.6% of successes |
| **Failed / No data** | 331 | 64.9% |

(331 = 165 retried-still-failing + 166 manual-upload-category rows never in scope this round.)

---

## Operational notes for the next session

- **The live `output/run_manifest.json` and `output/success_flags.xlsx` currently only reflect the 185-driver scoped run** (185 entries), not the full 510. The new merge mechanism (`merge_with_existing_manifest()`) only carries forward what's *already on disk* at merge time — and this run started when the live manifest had already been reduced to a single stale entry by an earlier single-driver test, so there was nothing substantial to merge from.
- **The authoritative full baseline** is still `output/monthly/run_report_20260804_065530.xlsx` (untouched, 510/159).
- **The authoritative post-fix scoped result** is the newest `output/monthly/run_report_<timestamp>.xlsx` from the 185-driver run (510/20 subset, i.e. 185/20).
- **To get one single, complete, correctly-merged report covering all 510+ drivers**: run a full pipeline run with no `--driver-id-file`. Because `success_flags.xlsx` is also currently scoped/stale, this will re-fetch everything live rather than reusing cache (slower, but produces one unambiguous report with no merge caveats).
- **Registry grew 581→583 rows** — `registry_enrichment.py`'s existing auto-fallback-creation logic added 2 new EIA fallback rows for FRED drivers (`SPAIN_ELECTRICITY_NATURAL_GAS_PRICE_MARGINAL_GENERATION_FUEL_ALT_EIA`, `ASIA_NATURAL_GAS_ASIA_LNG_DEMAND_JKM_PRICE_ALT_EIA`) during a run this session — pre-existing behavior, not something built this session.
- **Registry backups** (pre-edit snapshots) exist at `.backup_before_ws3_locators_<timestamp>/`, `.backup_before_ws4_tunability_<timestamp>/`, `.backup_before_ws6_locators_<timestamp>/` — each holds `driver_registry.csv` exactly as it was before that workstream's edits.
- 35 total registry rows were modified this session (16 WS3 + 7 PSD + 12 quality-gate), each with a dated `[ws3/ws4/ws6 2026-08-05]` note in its `transform_hint` column for traceability.

## Known remaining gaps / good next-session targets

1. **USDA PSD outage** — purely a waiting game; 10 drivers ready to go the moment `apps.fas.usda.gov` recovers. Worth a quick live check first, since it may have recovered by the next session.
2. **~54 stale/dead-looking drivers from the WS4 mining** (both short-history AND severely stale, e.g. last data point from 2014-2018) — not touched this session, needs per-row investigation (similar effort to the Eurostat mining) to determine dead-connector vs. genuinely-discontinued-source.
3. **`ASIA_PARACETAMOL_BENZENE_PRICES_ASIA` / `ASIA_STYRENEBUTADIENE_RUBBER_SBR_...`** — EIA has no benzene/ethylene series; need reclassification to a real petrochemical price source (ICIS/Platts-style), not an EIA locator fix.
4. **Live-verify the rest of the 68 fallback targets** — only a static/structural audit was done this session (validator-based), not a live `--driver-id` test of each one.
5. **Deferred builds** (only if a concrete need emerges): USDA ARMS pre-check, Statistics Sweden SCB pulp connector, Australia ABS corn connector — none currently fix an actual failing driver.
6. **2 dead `classification:` text fragments** in SIDRA locators (`LA_PLP_WOOD`, `LA_PLP_TISSUE_FB`) and **5 vestigial `freq`/`period` Comtrade locator keys** — both harmless (connector ignores them, no effect on outcome), low-priority registry cleanup.
7. **Manual-upload category (166 drivers)** — deliberately out of scope this round; revisit if the user wants to tackle paid/licensed sources next.

## Full plan file (for deeper implementation history)
`C:\Users\Vivek\.claude\plans\this-is-the-summary-merry-pinwheel.md` — the original detailed plan with file/line-level specifics for every workstream above.
