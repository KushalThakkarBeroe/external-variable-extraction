# How to Run This Pipeline

Practical reference for running `run_pipeline.py` in its different modes. Run every command from inside this folder (`external variable extraction/`), since the default config path and every path inside `config.yaml` are relative to it.

## Prerequisites

```bash
pip install -r requirements.txt
```

Credentials resolve from environment variables (`.env`, see `.env.example` for the full list) or literal values in `config/config.yaml`. A missing credential does not stop a run, that driver just fails cleanly and escalates (fallback, AI-proposed alternate source, or a human-in-the-loop task).

## Input file

```
input/driver_registry.csv
```

Required columns (the run refuses to load without these present in the header):
```
driver_id
commodity
driver_name
region
extraction_tier
connector             (may be blank per row -> generic extraction is used)
source_name
source_url
endpoint_or_locator
access_mode
native_frequency
rollup_method
unit
```

Optional columns (sensible defaults applied if left blank):
```
tier_confidence
history_from
meets_min_history
update_lag_days
is_proxy
proxy_note
human_in_loop
credential_key
fallback_driver_id
data_quality_risk
transform_hint
priority
min_history_years
```

## Running it

### Dry run — validate only, no network calls
```bash
python run_pipeline.py --dry-run
```
- Prints the planned extraction table: driver, commodity, region, tier, frequency, rollup method, connector, human-needed flag.
- Does not call any connector, does not write `monthly_panel.csv`, the manifest, or the human-task file.
- Does create the empty output folder skeleton (`output/raw`, `output/interim`, `output/monthly`, `input/manual`), since that setup step runs before the dry-run check, but writes nothing inside them.
- Exits 0.

### Full run — sequential (the default, proven path)
```bash
python run_pipeline.py
```
Fetches every driver one at a time, runs the full cascade per row, writes every output file (see below). Exit code 0 if anything was collected, 1 if the run produced nothing at all.

### Full run — parallel (faster at scale)
```bash
python run_pipeline_parallel.py
python run_pipeline_parallel.py --max-workers 12   # tune concurrency for this run only
```
Identical behavior to the sequential script, but driver fetches run concurrently across a thread pool. Same flags as `run_pipeline.py` otherwise (`--dry-run`, `--commodity`, `--registry`, etc. all work the same way).

### Scope to one commodity or region
```bash
python run_pipeline.py --commodity Poultry --region Australia
```
`--commodity` and `--region` are repeatable to include more than one.

### Run specific drivers only
```bash
python run_pipeline.py --driver-id LA_PLP_FX --driver-id AU_PLT_SUBST
python run_pipeline.py --driver-id-file output/rerun_worklist_2026.txt   # one driver_id per line
```

### Disable AI assistance
```bash
python run_pipeline.py --no-llm
```
Every LLM-assisted method returns nothing; the pipeline runs entirely on deterministic paths. This also skips the auto-enrichment step (see below).

### Point at a different registry or config file

Both `--registry` and `--config` accept either a **relative path** (resolved from wherever the command is run, so it must be typed relative to this folder if run from here) or a **full absolute path** (works no matter what directory the command is run from):

```bash
# relative path, run from inside external variable extraction/
python run_pipeline.py --registry input/driver_registry_full.csv
python run_pipeline.py --config config/config_staging.yaml

# absolute path, works from any directory
python run_pipeline.py --registry "/path/to/Commodity-Price-Forecasting/external variable extraction/input/sample_registry.csv"
```

The folder name `external variable extraction` contains spaces, so any absolute path through it must be wrapped in quotes (as above), or the spaces escaped with a backslash (`external\ variable\ extraction`), otherwise the shell reads it as three separate arguments and the command fails.

### Override the date window or history requirement
```bash
python run_pipeline.py --start-date 2024-07-01 --end-date 2026-06-30
python run_pipeline.py --min-history-years 2   # loosen the quality gate for a deliberately short window
```

### Skip drivers already known-good (reuse cached data)
```bash
python run_pipeline.py --skip-flagged
```
Reuses cached results for drivers already marked sufficiently complete in `output/success_flags.xlsx`, instead of re-fetching them.

### Resume drivers deferred by a source's rate limit
```bash
python run_pipeline.py --resume-deferred
```

### Testing a single driver in isolation, without touching the real registry

```bash
python run_pipeline.py --registry input/my_test_registry.csv --no-llm
```
- Copy just the row(s) to test into a small separate CSV and point `--registry` at it. This fully isolates the experiment from the real registry file.
- **Watch out**: the auto-enrichment step (`registry.auto_enrich_new_drivers`, on by default in `config.yaml`) runs against *whatever* registry file is passed in, and tries to match every row in the shared seed file (`input/new_drivers.xlsx`) against it. Against a tiny test file, that can mean it tries to enrich every unrelated seed row into the small file.
- Fix: add `--no-llm` (enrichment is skipped whenever the AI is disabled), or point `--config` at a copy of `config.yaml` with `registry.auto_enrich_new_drivers: false` set, if the AI needs to stay active for the row being tested but not the unrelated enrichment step.

## Output files

Main output, one row per driver per month:
```
output/monthly/monthly_panel.csv       (also .parquet, controlled by output.format in config.yaml: parquet | csv | both)
```
Columns: `driver_id, commodity, region, driver_name, month, value, n_obs, coverage_ratio, is_complete, is_imputed, unit, rollup_method, native_frequency, tier_declared, tier_used, frequency_detected, served_by_driver_id, source_name, source_url, is_proxy, is_substituted, data_quality_risk, transform_hint, retrieved_at`

Wide format, months down the side, one column per `driver_id` (only written if `output.write_wide_panel: true`):
```
output/monthly/wide_<commodity>_<region>.csv
```

Pre-rollup raw observations, one file per driver:
```
output/interim/<driver_id>_observations.csv
```

Everything else:
```
output/run_manifest.json                    per-driver audit log: tier used, outcome, timing, quality notes
output/human_in_the_loop_tasks.yaml         open tasks needing a human
output/fallback_audit_log.csv               which fallback targets were actually exercised this run, and their outcome
output/rejected_alternate_sources.yaml      AI-proposed sources that failed live verification; kept so they are never re-proposed
output/success_flags.xlsx                   which drivers cleared the success threshold; read by --skip-flagged
output/monthly/run_report_<timestamp>.xlsx  human-readable Excel summary: outcomes, tier breakdown, per-commodity metrics
output/raw/                                 working download cache
output/raw_archive/<run-timestamp>/         permanent copy of the source file behind every success
output/pipeline_<timestamp>.log             full log for that run
```
