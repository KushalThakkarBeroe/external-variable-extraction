# Cascading Commodity Driver Data Collection Pipeline

Pulls historical data for every external driver in a commodity x region forecasting
model, harmonises it to monthly, and records exactly what a human still needs to do.

Built and validated against the 14-driver sample (Poultry / Australia, Pulp / Latin
America). Scaling to the full ~40 commodity list means adding rows to the registry,
not writing orchestration code.

---

## 1. What it does

```
driver_registry.csv  ->  cascade orchestrator  ->  monthly panel
   (control plane)         (tier escalation)        (model input)
                                  |
                                  +-> run_manifest.json           (what happened)
                                  +-> human_in_the_loop_tasks.yaml (what you must do)
                                  +-> output/raw/                  (audit trail)
```

Each row of the registry is one commodity x region x driver. The orchestrator resolves
it, escalating through tiers until something works, and falls back to a human task
rather than failing the run.

---

## 2. The four tiers

| Tier | Definition | How the pipeline handles it |
|---|---|---|
| 1 | Data directly on a URL: JSON/CSV API or HTML table | Named connector, or generic table scoring that picks the most time-series-shaped table on the page |
| 2 | Data inside an Excel, CSV or PDF linked from a page | Discovers the file link (never hardcodes it), downloads to the audit cache, locates the series inside multi-banner sheets |
| 3 | Data spread across many pages | Four crawl patterns: period URLs, pagination, index-and-detail, and dashboard-backing JSON endpoints |
| 4 | Login, API key or paid subscription | Uses configured credentials, attempts free signup where a provider documents one, otherwise records a precise human task |

Tier 2 discovers rather than hardcodes for a concrete reason: the World Bank Pink
Sheet sits behind a hashed document path that changes with each monthly edition, and
AIP republishes its terminal gate price workbook under a new dated filename. A pinned
URL would work today and silently break next month.

---

## 3. The cascade

For each driver, in order, stopping at the first rung that yields usable data:

1. **Declared tier** from the registry. Usually right, always cheapest.
2. **Remaining tiers** in ascending order. A source declared Tier 2 sometimes exposes
   a quiet JSON endpoint; a workbook link sometimes moves behind a paginated archive.
3. **LLM re-classification.** All tiers failed, so re-read the landing page and work
   out what the access pattern actually is, then retry once. The suggestion is written
   to the manifest for an analyst to fold into the registry. It is never applied silently.
4. **Registered fallback.** The registry's declared alternative, run through the same
   cascade. This is how a proxy takes over when a primary series dies.
5. **Human in the loop.** A task with the exact action needed and the exact config key
   or drop path that resolves it. The run continues.

### Quality gates

Anything that comes back is tested before acceptance:

- history length against `run.min_history_years` (default 8)
- share of months carrying a value (>= 80%)
- share of populated months meeting the coverage threshold (>= 70%)
- staleness against the source's own publication lag plus two months of slack

A result that fails a gate is downgraded to `partial` and kept as a floor while the
cascade keeps looking. A thin series still beats no series, but it is labelled.

---

## 4. Rollup to monthly

The aggregation method is a modelling decision, so it lives in the registry per driver:

| Method | Used for | Why |
|---|---|---|
| `mean` | prices, indices, exchange rates | A month's economic signal is its average level, not its last print |
| `sum` | trade volumes, chicks placed, production tonnes | Flows accumulate |
| `last` | flock size, capacity, inventories | A stock is a state at month end |
| `count` | outbreak notifications | Months with no events are filled with zero, because absence of an outbreak is information |
| `max` / `min` | shock indicators | Peak spot price within the month |

**Coverage guard.** A month is accepted only when observed points reach
`min_coverage_ratio` of the expected count for that frequency. Without it, a month
where a source published twice would silently become a monthly average sitting
alongside 21-print months in the same regression.

**Weekly straddling month ends.** A week is attributed to the month containing its
end date, matching how USDA and AIP report. Set `split_straddling_weeks=True` to
apportion by day count instead when working with flow variables.

Every output row carries `n_obs`, `coverage_ratio`, `is_complete` and `is_imputed`,
so a modeller can filter on quality without coming back to the pipeline.

---

## 5. Configuration

`config/config.yaml` is the single human touchpoint. Secrets resolve in this order:

1. Literal value in the file (convenient, least secure)
2. `${ENV:VAR_NAME}` placeholder from the environment (recommended)
3. Absent, in which case the source becomes a human-in-the-loop task rather than
   crashing a 300-driver run

```yaml
credentials:
  fred_api_key: "${ENV:FRED_API_KEY}"        # free, self-service
  usda_nass_api_key: "${ENV:USDA_NASS_API_KEY}"
  comtrade_api_key: "${ENV:COMTRADE_API_KEY}"
  mla_username: null                          # free but portal-gated
  mla_password: null
  fastmarkets_api_key: null                   # paid, licence check first
```

Keep the real file out of version control and distribute
`config/credentials.example.yaml`.

---

## 6. Closing the human-in-the-loop

When a driver cannot be automated, the pipeline writes a task naming the fix:

```yaml
tasks:
  - driver_id: AU_PLT_SUBST
    reason: Portal login required
    required_action: Register at https://www.mla.com.au/prices-markets/, then set
      mla_username and mla_password in config.yaml under credentials
    credential_keys_to_fill_in_config: [mla_username, mla_password]
    or_drop_file_here: input/manual/AU_PLT_SUBST.csv
```

Two ways to resolve it, both without touching code:

- add the credential to `config.yaml`, or
- obtain the series offline and save it as `input/manual/<driver_id>.csv` with
  columns `obs_date,value`

The next run picks up the manual file automatically through the Tier 2 extractor and
the task disappears.

---

## 7. Running it

```bash
pip install -r requirements.txt

python run_pipeline.py --dry-run                          # validate registry, show plan
python run_pipeline.py                                    # full sample run
python run_pipeline.py --commodity Poultry --region Australia
python run_pipeline.py --driver-id LA_PLP_FX --log-level DEBUG
python run_pipeline.py --registry input/driver_registry_full.csv   # scale out
python run_pipeline.py --no-llm                           # deterministic paths only
```

Exit code 0 when anything was collected, 1 when the run produced nothing at all.

---

## 8. Outputs

| Artifact | Purpose |
|---|---|
| `output/monthly/monthly_panel.csv` | Long format, one row per driver-month, with full provenance. This is the model input |
| `output/monthly/wide_<commodity>_<region>.csv` | Months down, drivers across. For correlation screens and variable selection |
| `output/interim/<driver_id>_observations.csv` | Pre-rollup observations, for reconciliation |
| `output/run_manifest.json` | Per-driver outcome, tier used, quality notes, timing |
| `output/human_in_the_loop_tasks.yaml` | The exact list of things a person must do |
| `output/raw/` | Every downloaded byte plus a provenance sidecar |

The raw cache matters: when a client questions a number six weeks later, the exact
workbook that produced it is still on disk.

---

## 9. Where the LLM is used, and where it is not

Four narrow jobs where deterministic code is genuinely weak:

1. **Disambiguating tables** when several on a page could be the series
2. **Locating a series** in a workbook whose header layout defeats the parser
3. **Extracting from PDF prose** where no ruled table exists
4. **Re-classifying a tier** when every declared path has failed

Guardrails that matter for a forecasting pipeline:

- For jobs 1 and 2 the model returns **coordinates, never numbers**. Values are then
  read from the actual file, so it cannot hallucinate data.
- For job 3, where it must return values, every row carries the source line it came
  from, and rows whose value does not literally appear in that line are discarded.
- Temperature 0, strict JSON, hard retry cap.
- With `llm.enabled: false` or `--no-llm`, every method returns None and the pipeline
  runs on deterministic paths alone. The validated run in section 11 used `--no-llm`.

---

## 10. Adding drivers

Add a row to `input/driver_registry.csv`. If the publisher is new, add a connector
function to `pipeline/connectors.py` and register it in the `CONNECTORS` dict:

```python
def my_source(spec, http, cfg) -> pd.DataFrame:
    """Return columns [obs_date, value]. Raise PermissionError when credentials block."""
    ...
```

Nothing in the orchestrator changes. Raising `PermissionError` specifically is what
lets the cascade distinguish "needs a key" from "source is broken".

---

## 11. Validated run

Full 14-driver sample, no API keys configured, `--no-llm`:

| Outcome | Count |
|---|---|
| Success | 7 (150-151 months each, 12.4-12.6 years) |
| Partial | 1 |
| Blocked or failed | 6 |

Resolved by tier: 2 at Tier 1, 5 at Tier 2, 1 at Tier 3. Four drivers were served by
a registered fallback rather than their primary source.

Spot checks performed:

- Banco Central SGS returned 3,151 daily USD/BRL quotes; the June 2026 monthly mean
  was recomputed independently from the raw file and matches the panel to six decimals
- AIP terminal gate prices: the current dated workbook was discovered and downloaded,
  yielding 5,882 daily observations across five capital cities; the June 2026 monthly
  mean was recomputed independently and matches to four decimals
- The Pink Sheet workbook link was discovered on the landing page, downloaded, and
  "Maize" located inside a multi-banner sheet; values span 143.9 to 348.2 USD/mt

---

## 12. Known gaps, stated plainly

Three sources raise honest errors and route to the human-in-the-loop file rather than
shipping a guessed endpoint that would rot quietly:

- **WOAH WAHIS** (avian influenza). The public dashboard is Qlik over a private JSON
  API whose path shifts between releases. Confirm the current path, or use the DAFF
  and FAO EMPRES-i fallback row.
- **China customs.** Requires session tokens and per-period form posts. UN Comtrade
  is the better primary route and is already registered.
- **IBGE SIDRA tissue proxy.** Table 8880 returns no values at the aggregate level;
  the classification code for hygiene and personal care needs confirming.

Two data-quality items worth a decision before modelling:

- **Corn for Australian poultry** currently pulls the global maize benchmark, but
  Australian broiler rations are wheat and sorghum led. Flagged as a proxy; add an
  ABARES local feed grain series in phase 2.
- **Pulp capacity** returned only to 2018 from the Statistics Canada cube coordinate,
  so it was accepted as `partial` rather than silently trusted. Confirm the coordinate
  per release and add the IBA Brazil and SCB Sweden legs when scaling out.
