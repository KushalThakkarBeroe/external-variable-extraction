"""
Source adapters.

A connector encapsulates everything specific to one publisher: its endpoint
shape, auth style, date format, pagination rule and unit quirks. Tier handlers
call them; the orchestrator never touches them directly.

Contract:
  def connector(spec, http, cfg) -> pd.DataFrame with columns [obs_date, value]
  Raise PermissionError when the blocker is credentials, so the cascade can
  distinguish "needs a key" from "source is broken".

Adding the remaining ~35 commodities means adding functions here and rows to
the registry. No orchestration code changes.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd
import requests

from .models import utc_now

log = logging.getLogger(__name__)


def _window(cfg, spec=None) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Compute the fetch window. Respects per-driver history_from if provided."""
    # Per-driver start date (e.g. from enrichment seed file)
    if spec and spec.history_from:
        start = pd.Timestamp(spec.history_from)
    else:
        start = pd.Timestamp(cfg.get("run.start_date", "2014-01-01"))
    end = pd.Timestamp(cfg.get("run.end_date") or utc_now().date())
    return start, end


def _time_budget(cfg) -> float:
    """
    Wall-clock budget (seconds) for a connector that loops multiple requests
    (e.g. year-by-year). Each individual request already has its own
    timeout+retry budget (http.timeout_seconds/max_retries) — this bounds
    the *total* elapsed time across all iterations, so a source erroring
    consistently across many years/periods can't stall a run for minutes.
    """
    return float(cfg.get("http.max_retrieval_seconds", 180))


# ============================================================================
# TIER 1 - open APIs, no key
# ============================================================================

def bcb_sgs(spec, http, cfg) -> pd.DataFrame:
    """
    Banco Central do Brasil, Sistema Gerenciador de Series Temporais.

    Used for USD/BRL (series 1, daily sale rate). Public, no key.
    Two quirks handled here:
      - dates are dd/MM/yyyy in both request and response
      - since March 2025 a single request is capped at a 10-year window, so we
        chunk the requested span into 9-year slices and concatenate
    """
    series_id = (spec.locator_part("bcdata.sgs") or "").strip()
    if not series_id:
        # Locator written as 'bcdata.sgs.1 (USD sale, daily) | ...'
        token = str(spec.endpoint_or_locator).split("|")[0]
        series_id = "".join(ch for ch in token.split("bcdata.sgs.")[-1] if ch.isdigit())
    if not series_id:
        raise ValueError("No SGS series code found in the registry locator")

    start, end = _window(cfg, spec)
    frames = []
    # Chunk to stay inside the API's 10-year per-request limit. Chunks are built
    # from the requested start rather than with a year-anchored date_range: an
    # anchored frequency snaps the first chunk to 1 January and silently drops
    # every month before it.
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + pd.DateOffset(years=9) - pd.Timedelta(days=1), end)
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        params = {
            "formato": "json",
            "dataInicial": chunk_start.strftime("%d/%m/%Y"),
            "dataFinal": chunk_end.strftime("%d/%m/%Y"),
        }
        payload = http.get_json(url, params=params)
        if payload:
            frames.append(pd.DataFrame(payload))
        chunk_start = chunk_end + pd.Timedelta(days=1)

    if not frames:
        raise ValueError(f"SGS series {series_id} returned no data for the requested window")

    raw = pd.concat(frames, ignore_index=True)
    return pd.DataFrame({
        "obs_date": pd.to_datetime(raw["data"], format="%d/%m/%Y", errors="coerce"),
        "value": pd.to_numeric(raw["valor"].astype(str).str.replace(",", "."), errors="coerce"),
    }).dropna(subset=["obs_date"])


def ibge_sidra(spec, http, cfg) -> pd.DataFrame:
    """
    IBGE SIDRA, Brazil. Public, no key.

    Endpoint shape:
      https://apisidra.ibge.gov.br/values/t/{table}/n1/all/v/{var}/p/all

    Three quirks handled here, all of which silently produced an empty series
    before they were pinned down:
      - row 0 of the response is a column dictionary, not data
      - most tables carry SEVERAL variables (index, seasonally adjusted index,
        MoM change, YoY change) stacked in one response, so the registry must
        name which one is wanted; without a filter the frame mixes units
      - unavailable observations are written as '..' or '-', not blank, so a
        naive numeric coercion yields an all-NaN column that looks like success
    """
    table = spec.locator_part("table")
    if not table:
        raise ValueError("No SIDRA table code in the registry locator")

    # 'variable:63' selects a specific SIDRA variable code; 'all' is the default
    # and is fine for single-variable tables such as the producer price index.
    variable = spec.locator_part("variable") or "all"
    url = f"https://apisidra.ibge.gov.br/values/t/{table}/n1/all/v/{variable}/p/all"
    payload = http.get_json(url)
    if not payload or len(payload) < 2:
        raise ValueError(f"SIDRA table {table} returned no rows")

    raw = pd.DataFrame(payload[1:])          # row 0 is the column dictionary
    if "V" not in raw.columns:
        raise ValueError("SIDRA response has no value column 'V'")

    # SIDRA writes nulls as '..' (not applicable) and '-' (zero or suppressed).
    values = pd.to_numeric(
        raw["V"].astype(str).str.strip().replace({"..": None, "...": None, "-": None, "X": None}),
        errors="coerce",
    )

    # Period column: D3C is standard, but the dimension index shifts by table.
    period_col = next(
        (c for c in ("D3C", "D2C", "D4C", "D1C")
         if c in raw.columns
         and pd.to_datetime(raw[c].astype(str), format="%Y%m", errors="coerce").notna().mean() > 0.5),
        None,
    )
    if period_col is None:
        raise ValueError("Could not identify a monthly period column in the SIDRA response")

    obs_date = pd.to_datetime(raw[period_col].astype(str), format="%Y%m", errors="coerce")

    out = pd.DataFrame({"obs_date": obs_date, "value": values}).dropna(subset=["obs_date"])

    # When several variables came back despite the filter, keep the one with the
    # most populated observations rather than averaging incompatible units.
    label_col = next((c for c in ("D2N", "D3N", "D1N") if c in raw.columns), None)
    if label_col is not None and raw[label_col].nunique() > 1 and out["value"].notna().any():
        out[label_col] = raw[label_col].values
        best = out.dropna(subset=["value"]).groupby(label_col)["value"].count().idxmax()
        log.info("SIDRA table %s returned %d variables; using '%s'",
                 table, raw[label_col].nunique(), best)
        out = out[out[label_col] == best].drop(columns=[label_col])

    if out["value"].notna().sum() == 0:
        raise ValueError(
            f"SIDRA table {table} variable '{variable}' returned no populated observations; "
            "confirm the variable code and any required classification filter"
        )
    return out


class EurostatLocatorError(ValueError):
    """
    Raised by eurostat() when the locator's dataset code or filter dimensions
    do not match Eurostat's real catalogue/metadata — as opposed to a
    transient network fault or a structurally different problem (e.g. no
    'time' dimension at all). Carries enough structure for the cascade to
    attempt a targeted repair instead of just escalating blindly.

    reason:
      "bad_dataset"         - the dataset code itself doesn't exist (404)
      "bad_filters"         - the dataset exists but a filter key/value is not
                               a real dimension/category (400), or a real
                               dimension was left unpinned with >1 category
                               and the connector already has the true
                               dimension/category list in hand (200, no extra
                               network round-trip needed to repair)
      "insufficient_filters" - dataset + filters are individually valid but
                               the response is too large to be one series (413)
    """

    def __init__(self, reason, dataset, filters, status_code, message,
                 available_dimensions=None):
        super().__init__(message)
        self.reason = reason
        self.dataset = dataset
        self.filters = dict(filters or {})
        self.status_code = status_code
        # {dim_id: {category info}} for dims that need a value pinned — only
        # populated for the "bad_filters" 200-OK unresolved-dimensions case,
        # where eurostat() already parsed this out of the response and would
        # otherwise throw it away. None when repair needs a fresh bare fetch.
        self.available_dimensions = available_dimensions


def eurostat_fetch_dimensions(http, dataset: str) -> dict:
    """
    Bare no-filter metadata read for a Eurostat dataset: returns its real
    {dim_id: {"label": ..., "category": {"index": {...}, "label": {...}}}}
    dict, reusing the exact same dissemination endpoint and payload shape
    eurostat() already parses, but with zero filters so the response
    describes ALL of the dataset's real dimensions/categories rather than one
    series. Used only by the locator-repair path in cascade.py to build a
    real filter-value menu — never during a normal fetch, since it discards
    the actual observation values.
    """
    payload = http.get_json(
        f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}",
        params={"format": "JSON", "lang": "EN"},
    )
    return payload.get("dimension") or {}


def eurostat(spec, http, cfg) -> pd.DataFrame:
    """
    Eurostat statistics API. Public, no key.

    Covers the large recurring class of EU agricultural/energy/economic
    series whose landing page is the Eurostat "databrowser" — a JavaScript
    app with no static table or file to scrape. The same data is available,
    keyless, from Eurostat's dissemination API in JSON-stat format.

    Locator: 'dataset:<code>' (the databrowser URL's last path segment, e.g.
    'apro_mk_pobta') optionally followed by '| filters:geo=DE;unit=I15'.
    Every classificatory dimension other than time must be pinned to exactly
    one category via filters — the response otherwise mixes several series
    (regions, units, etc.) together and there would be no way to tell them
    apart.
    """
    dataset = spec.locator_part("dataset")
    if not dataset:
        raise ValueError("No Eurostat dataset code declared in the locator (format: dataset:<code>)")

    filters: dict[str, str] = {}
    filter_str = spec.locator_part("filters") or ""
    for part in filter_str.replace("&", ";").split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            filters[key.strip()] = value.strip()

    try:
        payload = http.get_json(
            f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}",
            params={"format": "JSON", "lang": "EN", **filters},
        )
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 404:
            raise EurostatLocatorError(
                "bad_dataset", dataset, filters, status,
                f"Eurostat dataset {dataset} not found (404) — the dataset code may be "
                "wrong, retired, or renamed",
            ) from exc
        if status == 400:
            raise EurostatLocatorError(
                "bad_filters", dataset, filters, status,
                f"Eurostat dataset {dataset} rejected the filters {filters} (400 Bad Request) "
                "— a filter key is likely not a real dimension on this dataset",
            ) from exc
        if status == 413:
            raise EurostatLocatorError(
                "insufficient_filters", dataset, filters, status,
                f"Eurostat dataset {dataset} response too large for the given filters {filters} "
                "(413) — the filters don't narrow the response to a single series",
            ) from exc
        raise

    dimensions = payload.get("dimension") or {}
    dim_ids = (payload.get("id") or list(dimensions.keys()))
    if "time" not in dimensions:
        raise ValueError(f"Eurostat dataset {dataset} response has no 'time' dimension")

    # Every dimension besides time must resolve to a single category; that
    # single category then contributes a fixed offset of zero to the flat
    # index below, so the time position IS the flat index.
    unresolved = [
        dim_id for dim_id in dim_ids
        if dim_id != "time"
        and len((dimensions.get(dim_id) or {}).get("category", {}).get("index", {})) > 1
    ]
    if unresolved:
        raise EurostatLocatorError(
            "bad_filters", dataset, filters, 200,
            f"Eurostat dataset {dataset} has {len(unresolved)} unfiltered dimension(s) "
            f"({', '.join(unresolved)}) with multiple categories; add filters:<dim>=<code> "
            "for each to select a single series",
            available_dimensions={dim_id: dimensions[dim_id] for dim_id in unresolved},
        )

    time_index = dimensions["time"]["category"]["index"]
    values = payload.get("value") or {}

    rows = []
    for period_label, position in time_index.items():
        raw_value = values.get(str(position), values.get(position))
        if raw_value is None:
            continue
        rows.append({"period": period_label, "value": raw_value})

    if not rows:
        raise ValueError(f"Eurostat dataset {dataset} returned no populated observations")

    raw = pd.DataFrame(rows)
    obs_date = pd.to_datetime(raw["period"], format="%Y-%m", errors="coerce")
    if obs_date.isna().mean() > 0.5:
        obs_date = pd.to_datetime(raw["period"].str.replace(r"-Q(\d)", lambda m: f"-{int(m.group(1)) * 3 - 2:02d}", regex=True), errors="coerce")
    if obs_date.isna().mean() > 0.5:
        obs_date = pd.to_datetime(raw["period"] + "-01-01", format="%Y-%m-%d", errors="coerce")

    return pd.DataFrame({
        "obs_date": obs_date,
        "value": pd.to_numeric(raw["value"], errors="coerce"),
    }).dropna(subset=["obs_date"])


def ccee_pld(spec, http, cfg) -> pd.DataFrame:
    """
    CCEE Brazil settlement price (PLD), weekly by submarket.

    The public price panel is JavaScript-rendered over a JSON feed. Endpoint
    paths at CCEE change with site releases, so this connector deliberately
    fails loudly and lets the cascade fall back to the Pink Sheet energy index
    declared as LA_PLP_ENERGY_FB, rather than silently returning nothing.
    """
    raise NotImplementedError(
        "CCEE PLD JSON feed path must be confirmed against the current price panel release; "
        "cascade will fall back to the configured energy index"
    )


# ============================================================================
# TIER 2 - embedded files
# ============================================================================

def worldbank_pinksheet(spec, http, cfg) -> pd.DataFrame:
    """
    World Bank Pink Sheet, monthly historical workbook.

    Deliberately discovers the download link from the commodity markets landing
    page rather than hardcoding it: the file sits behind a hashed document path
    that changes with each monthly edition.

    Workbook layout: banner rows, then a header row of commodity names, then a
    units row, then data with periods written as '1960M01' in column 0.
    """
    from .tiers.tier2_files import Tier2EmbeddedFile

    series_name = spec.locator_part("series")
    if not series_name:
        raise ValueError("No series name declared for the Pink Sheet lookup")

    extractor = Tier2EmbeddedFile(cfg, http, {}, llm=None)
    file_url = extractor.discover_file_link(
        spec.source_url, filename_hint=spec.locator_part("file") or "CMO-Historical-Data-Monthly.xlsx"
    )
    if not file_url:
        raise ValueError("Could not locate the Pink Sheet monthly workbook on the landing page")

    local = http.download(file_url, suffix=".xlsx")
    return extractor.extract_from_workbook(local, spec)


def aip_tgp(spec, http, cfg) -> pd.DataFrame:
    """
    Australian Institute of Petroleum, historical terminal gate prices.

    Daily wholesale diesel by capital city, published as a periodically renamed
    .xls workbook, so the link is discovered rather than hardcoded. City columns
    are averaged into a single national daily series before monthly rollup,
    which matches how ACCC reports the five-city average.
    """
    from .tiers.tier2_files import Tier2EmbeddedFile
    from pathlib import Path as _Path

    extractor = Tier2EmbeddedFile(cfg, http, {}, llm=None)
    file_url = extractor.discover_file_link(spec.source_url, filename_hint="AIP_TGP_Data")
    if not file_url:
        raise ValueError("Could not locate the AIP terminal gate price workbook")

    # AIP has published both legacy .xls and modern .xlsx over the years and the
    # engines are not interchangeable, so take the real extension rather than a
    # substring test ('.xls' in '...xlsx' is True and silently picks xlrd).
    suffix = _Path(file_url.split("?")[0]).suffix.lower()
    if suffix not in (".xls", ".xlsx", ".xlsm"):
        suffix = ".xlsx"
    local = http.download(file_url, suffix=suffix)
    engine = "xlrd" if suffix == ".xls" else "openpyxl"
    book = pd.read_excel(local, sheet_name=None, header=None, engine=engine)

    # Pick the diesel sheet; AIP separates ULP and diesel into their own tabs.
    sheet_hint = (spec.locator_part("sheet") or "diesel").lower()
    sheet_name = next((s for s in book if sheet_hint.split()[0] in str(s).lower()), list(book)[0])
    grid = book[sheet_name]

    # Header row is the first row containing a recognised capital city name.
    cities = [c.strip().lower() for c in (spec.locator_part("cities") or
                                          "Sydney,Melbourne,Brisbane,Adelaide,Perth").split(",")]
    header_row = None
    for idx in range(min(len(grid), 20)):
        row_text = " ".join(str(v).lower() for v in grid.iloc[idx].tolist())
        if sum(city in row_text for city in cities) >= 2:
            header_row = idx
            break
    if header_row is None:
        raise ValueError("Could not find the city header row in the AIP workbook")

    body = grid.iloc[header_row + 1:].copy()
    body.columns = [str(c).strip() for c in grid.iloc[header_row].tolist()]

    date_col = body.columns[0]
    city_cols = [c for c in body.columns if any(city in str(c).lower() for city in cities)]
    if not city_cols:
        raise ValueError("No capital city price columns found in the AIP workbook")

    prices = body[city_cols].apply(pd.to_numeric, errors="coerce")
    return pd.DataFrame({
        "obs_date": pd.to_datetime(body[date_col], errors="coerce", dayfirst=True, format="mixed"),
        # Row-wise mean across cities: the national wholesale indicator.
        "value": prices.mean(axis=1, skipna=True),
    }).dropna(subset=["obs_date"])


# ============================================================================
# TIER 3 - multi-page / dashboard-backed
# ============================================================================

def woah_wahis(spec, http, cfg) -> pd.DataFrame:
    """
    WOAH WAHIS animal disease event dashboard.

    The public interface is a Qlik dashboard over a private JSON API. Endpoint
    paths shift between WAHIS releases, so rather than pin a path that will
    quietly rot, this connector defers to the generic Tier 3 JSON crawler using
    the endpoint declared in the registry locator. When WAHIS changes shape the
    cascade falls through to the DAFF/FAO fallback driver, which is the
    behaviour we want for a shock indicator that must not go silently stale.
    """
    from .tiers.tier3_multipage import Tier3MultiPage

    crawler = Tier3MultiPage(cfg, http, {}, llm=None)
    return crawler._crawl_json_api(spec)  # noqa: SLF001 - deliberate reuse


def daff_au_outbreak(spec, http, cfg) -> pd.DataFrame:
    """
    Australian DAFF avian influenza status pages, as HPAI fallback.

    Outbreak announcements are narrative pages rather than tables, so the LLM
    extractor is the practical route: it reads page text and returns one dated
    record per confirmed detection. Values are counts, rolled up with 'count'.
    """
    raise NotImplementedError(
        "DAFF outbreak pages require LLM text extraction; enable llm.enabled and supply "
        "an Anthropic API key, or record outbreak dates manually via the HITL path"
    )


def pulp_output_composite(spec, http, cfg) -> pd.DataFrame:
    """
    Composite pulp output: Brazil (IBA), Canada (StatCan), Sweden (SCB).

    Each geography is a separate source with its own tier, which is exactly the
    case the cascade exists for. Statistics Canada is the one open, keyless,
    well-documented member of the set, so it is implemented here and the
    remainder are declared as their own registry rows when scaling out.

    StatCan Web Data Service returns a vector of periods and values as JSON.
    """
    # Table 16-10-0045: manufacturing sales; pulp product vectors sit within it.
    url = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromCubePidCoordAndLatestNPeriods"
    payload = [{
        "productId": 16100045,
        "coordinate": "1.1.0.0.0.0.0.0.0.0",
        "latestN": 180,                      # 15 years of monthly observations
    }]
    data = http.request(url, method="POST", json_body=payload).json()

    records = []
    for block in data:
        for point in block.get("object", {}).get("vectorDataPoint", []):
            records.append({"obs_date": point.get("refPer"), "value": point.get("value")})

    if not records:
        raise ValueError("StatCan returned no data points for the pulp output cube")

    raw = pd.DataFrame(records)
    return pd.DataFrame({
        "obs_date": pd.to_datetime(raw["obs_date"], errors="coerce"),
        "value": pd.to_numeric(raw["value"], errors="coerce"),
    }).dropna(subset=["obs_date"])


def china_customs(spec, http, cfg) -> pd.DataFrame:
    """China GACC monthly import tables, paginated by period. Structure changes often."""
    raise NotImplementedError(
        "China customs portal requires session tokens and per-period form posts; "
        "use the Comtrade connector as the primary route"
    )


# ============================================================================
# TIER 4 - key or login gated
# ============================================================================

def fred(spec, http, cfg) -> pd.DataFrame:
    """
    FRED, Federal Reserve Bank of St Louis. Free API key required.

    Used for fallback series: World Bank maize and soybean meal mirrors, US
    pulpwood PPI, and DEXBZUS as the USD/BRL backstop.
    """
    api_key = cfg.credential("fred_api_key")
    if not api_key:
        raise PermissionError("FRED API key missing. Register free at "
                              "https://fredaccount.stlouisfed.org/apikeys")

    series_id = spec.locator_part("series_id")
    if not series_id:
        raise ValueError("No FRED series_id declared in the registry locator")

    start, end = _window(cfg, spec)
    payload = http.get_json(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": start.strftime("%Y-%m-%d"),
            "observation_end": end.strftime("%Y-%m-%d"),
        },
    )
    raw = pd.DataFrame(payload.get("observations", []))
    if raw.empty:
        raise ValueError(f"FRED returned no observations for {series_id}")

    return pd.DataFrame({
        "obs_date": pd.to_datetime(raw["date"], errors="coerce"),
        # FRED writes '.' for a missing observation.
        "value": pd.to_numeric(raw["value"].replace(".", None), errors="coerce"),
    }).dropna(subset=["obs_date"])


def nass_quickstats(spec, http, cfg) -> pd.DataFrame:
    """
    USDA NASS Quick Stats. Free API key required.

    Broiler placements and eggs set are weekly; production is monthly. The API
    caps a single response at 50,000 records, so the query is issued year by
    year, which also keeps each response small enough to parse comfortably.

    Filters come from the registry locator as 'key=value; key=value'.
    """
    api_key = cfg.credential("usda_nass_api_key")
    if not api_key:
        raise PermissionError("USDA NASS API key missing. Register free at "
                              "https://quickstats.nass.usda.gov/api")

    filters: dict[str, str] = {}
    # Accept ";" (the documented separator), "|" and "&" (URL-query-string
    # style, which enrichment sometimes produces) interchangeably as the
    # filter separator, so a locator like "a=1&b=2" parses into two filters
    # instead of one garbled value.
    for part in str(spec.endpoint_or_locator).replace("|", ";").replace("&", ";").split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            filters[key.strip()] = value.strip()

    start, end = _window(cfg, spec)
    frames = []
    loop_start = time.monotonic()
    budget = _time_budget(cfg)
    for year in range(start.year, end.year + 1):
        if time.monotonic() - loop_start > budget:
            log.warning("NASS year-by-year fetch for %s exceeded %.0fs budget at year %d; "
                       "stopping with %d year(s) collected", spec.driver_id, budget, year, len(frames))
            break
        params = {"key": api_key, "format": "JSON", "year": str(year), **filters}
        try:
            payload = http.get_json("https://quickstats.nass.usda.gov/api/api_GET/", params=params)
        except Exception as exc:  # noqa: BLE001 - a year with no records returns 400
            log.debug("NASS returned no rows for %s: %s", year, exc)
            continue
        rows = payload.get("data", [])
        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        raise ValueError("USDA NASS returned no records for the requested filters")

    raw = pd.concat(frames, ignore_index=True)

    # NASS dates: begin_code is the week or month ordinal within the year, and
    # week_ending carries the actual date for weekly series. NASS's response
    # always includes a week_ending KEY even for non-weekly series — as an
    # empty string, not null/absent — so a plain notna() check is fooled into
    # taking this branch for annual/point-in-time/monthly data too, turning
    # every row into NaT (via pd.to_datetime("")) and silently dropping the
    # entire series. Only a genuinely populated (non-blank) value counts.
    week_ending = raw.get("week_ending")
    has_real_week_ending = (
        week_ending is not None
        and week_ending.astype(str).str.strip().ne("").any()
    )
    if has_real_week_ending:
        obs_date = pd.to_datetime(raw["week_ending"], errors="coerce")
    else:
        # begin_code '00' marks a whole-year aggregate (reference_period_desc
        # 'YEAR'/'MARKETING YEAR') alongside per-month rows sharing the same
        # short_desc — '00' isn't a valid month, so pd.to_datetime correctly
        # coerces just those rows to NaT and dropna() below removes them,
        # leaving the monthly breakdown this pipeline actually wants.
        obs_date = pd.to_datetime(
            raw["year"].astype(str) + "-" + raw.get("begin_code", "01").astype(str).str.zfill(2) + "-01",
            errors="coerce",
        )

    return pd.DataFrame({
        "obs_date": obs_date,
        "value": pd.to_numeric(raw["Value"].astype(str).str.replace(",", ""), errors="coerce"),
    }).dropna(subset=["obs_date"])


def un_comtrade(spec, http, cfg) -> pd.DataFrame:
    """
    UN Comtrade Plus. Free subscription key required.

    Used for global poultry trade (HS 0207) and Chinese wood pulp imports
    (HS 4703) as the observable proxy for Chinese paper and board demand.
    Monthly periods are requested in yearly batches to stay within the
    per-request period cap.

    Locator (semicolon/pipe/&-separated 'key=value' filters): reporterCode,
    flowCode, cmdCode (each default to Comtrade's own "all"/M/TOTAL when
    absent), and optionally partnerCode to scope to bilateral trade with a
    specific partner/region instead of the world aggregate — added because
    the registry pre-flight validator (registry_validator.py) found several
    rows setting partnerCode that this connector was silently discarding,
    silently returning aggregate trade instead of the intended bilateral
    figure.
    """
    api_key = cfg.credential("comtrade_api_key")
    if not api_key:
        raise PermissionError("UN Comtrade subscription key missing. Register free at "
                              "https://comtradeplus.un.org")

    filters: dict[str, str] = {}
    # Accept ";" (the documented separator), "|" and "&" (URL-query-string
    # style, which enrichment sometimes produces) interchangeably as the
    # filter separator, so a locator like "a=1&b=2" parses into two filters
    # instead of one garbled value.
    for part in str(spec.endpoint_or_locator).replace("|", ";").replace("&", ";").split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            filters[key.strip()] = value.strip()

    reporter = filters.get("reporterCode", "all")
    flow = filters.get("flowCode", "M")
    cmd = filters.get("cmdCode", "TOTAL")
    partner = filters.get("partnerCode")

    start, end = _window(cfg, spec)
    frames = []
    loop_start = time.monotonic()
    budget = _time_budget(cfg)
    for year in range(start.year, end.year + 1):
        if time.monotonic() - loop_start > budget:
            log.warning("UN Comtrade year-by-year fetch for %s exceeded %.0fs budget at year %d; "
                       "stopping with %d year(s) collected", spec.driver_id, budget, year, len(frames))
            break
        periods = ",".join(f"{year}{m:02d}" for m in range(1, 13))
        params = {
            "reporterCode": reporter,
            "period": periods,
            "cmdCode": cmd,
            "flowCode": flow,
            "subscription-key": api_key,
        }
        if partner is not None:
            params["partnerCode"] = partner
        try:
            payload = http.get_json(
                "https://comtradeapi.un.org/data/v1/get/C/M/HS",
                params=params,
            )
        except Exception as exc:  # noqa: BLE001 - a bad/quota-exceeded year must not abort the driver
            log.warning("Comtrade returned an error for %s year %d: %s; skipping this year",
                       spec.driver_id, year, exc)
            continue
        rows = payload.get("data", [])
        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        raise ValueError("Comtrade returned no records for the requested filters")

    raw = pd.concat(frames, ignore_index=True)
    obs_date = pd.to_datetime(raw["period"].astype(str), format="%Y%m", errors="coerce")
    # netWgt is the volume measure; fall back to primary value when absent.
    value_col = "netWgt" if "netWgt" in raw.columns and raw["netWgt"].notna().any() else "primaryValue"

    return pd.DataFrame({
        "obs_date": obs_date,
        "value": pd.to_numeric(raw[value_col], errors="coerce"),
    }).dropna(subset=["obs_date"]).groupby("obs_date", as_index=False)["value"].sum()


def usda_psd(spec, http, cfg) -> pd.DataFrame:
    """
    USDA FAS Production, Supply and Distribution (PSD) Online. Free key.

    Annual granularity: PSD publishes one estimate per commodity/country/
    market-year, revised several times a year. The response's own 'month'
    field is the report vintage (when that revision was published), not a
    sub-annual reference period, so the series is stamped to Jan 1 of the
    market year.

    One request only ever covers a single market year, so — same shape as
    nass_quickstats/un_comtrade — the requested window is walked year by
    year, capped by the shared http.max_retrieval_seconds wall-clock budget.

    A commodity/country/year request returns one row per attribute
    (production, exports, ending stocks, ...), so the locator's attribute
    id selects which one — same reasoning as ibge_sidra's 'variable' filter:
    without it there is no way to tell several stacked measures apart.

    Locator: commodity:<PSD commodity code> | attribute:<attribute id>
             optionally | country:<2-letter code> (default: world aggregate)
    """
    api_key = cfg.credential("usda_fas_api_key")
    if not api_key:
        raise PermissionError("USDA FAS API key missing. Register at "
                              "https://apps.fas.usda.gov/opendataweb/home")

    commodity = spec.locator_part("commodity")
    if not commodity:
        raise ValueError("No PSD commodity code declared in the locator (format: commodity:<code>)")
    attribute = spec.locator_part("attribute")
    if not attribute:
        raise ValueError("No PSD attribute id declared in the locator (format: attribute:<id>)")
    country = (spec.locator_part("country") or "world").strip()

    headers = {"API_KEY": api_key}
    base = "https://apps.fas.usda.gov/OpenData/api/psd"
    start, end = _window(cfg, spec)

    frames = []
    loop_start = time.monotonic()
    budget = _time_budget(cfg)
    for year in range(start.year, end.year + 1):
        if time.monotonic() - loop_start > budget:
            log.warning("PSD year-by-year fetch for %s exceeded %.0fs budget at year %d; "
                       "stopping with %d year(s) collected", spec.driver_id, budget, year, len(frames))
            break
        if country.lower() == "world":
            url = f"{base}/commodity/{commodity}/world/year/{year}"
        else:
            url = f"{base}/commodity/{commodity}/country/{country}/year/{year}"
        try:
            payload = http.get_json(url, headers=headers)
        except Exception as exc:  # noqa: BLE001 - a market year with no published estimate is normal
            log.debug("PSD returned nothing for %s year %d: %s", spec.driver_id, year, exc)
            continue
        if isinstance(payload, dict):
            payload = [payload]
        if payload:
            frames.append(pd.DataFrame(payload))

    if not frames:
        raise ValueError(f"USDA PSD returned no records for commodity {commodity} ({country})")

    raw = pd.concat(frames, ignore_index=True)
    if "attributeId" not in raw.columns:
        raise ValueError(f"USDA PSD response for commodity {commodity} has no 'attributeId' column")

    raw = raw[raw["attributeId"].astype(str) == str(attribute)]
    if raw.empty:
        raise ValueError(
            f"USDA PSD commodity {commodity} returned no rows for attribute {attribute}; "
            "confirm the attribute id via GET /api/psd/commodityAttributes"
        )

    obs_date = pd.to_datetime(raw["marketYear"].astype(str) + "-01-01", format="%Y-%m-%d", errors="coerce")
    return pd.DataFrame({
        "obs_date": obs_date,
        "value": pd.to_numeric(raw["value"], errors="coerce"),
    }).dropna(subset=["obs_date"])


def usda_ers_arms(spec, http, cfg) -> pd.DataFrame:
    """
    USDA ERS Agricultural Resource Management Survey (ARMS) API. Free key
    (api.data.gov), tried before nass_quickstats/usda_psd for USDA-routed
    drivers per senior review (see registry fallback wiring — this connector
    is never the sole path for a driver, only the first-tried rung).

    Scope note (confirmed against ERS's own documentation and its published
    AllVariables.csv, not assumed): ARMS is farm-business SURVEY data — cost
    of production, income statements, farm operator/household characteristics
    — broken out by state and category, at ANNUAL granularity. It does not
    cover livestock prices, inventory, or most of what nass_quickstats/
    usda_psd actually serve; it will correctly return nothing for those and
    let the cascade fall through to the registered fallback. It is a genuine
    fit for farm cost/wage/income-type drivers (e.g. variable id 'evlabor'
    for hired-labor wage expense, confirmed in ERS's published variable list).

    Locator: 'report:<report name> | variable:<variable id>' optionally
    followed by '| category:<category name> | category_value:<value>' and/or
    '| state:<state or "all">' (default: all).

    Response field names are not independently verifiable without a live key
    (ERS requires a valid key even for schema discovery via OPTIONS) — this
    parses defensively against the documented request contract and raises a
    clear, diagnosable error naming the actual response keys if the expected
    fields aren't present, rather than silently returning nothing.
    """
    api_key = cfg.credential("usda_ers_arms_api_key")
    if not api_key:
        raise PermissionError("USDA ERS ARMS API key missing. Register free at "
                              "https://www.ers.usda.gov/developer/data-apis/arms-data-api")

    report = spec.locator_part("report")
    variable = spec.locator_part("variable")
    if not report and not variable:
        raise ValueError("ARMS requires at least one of report:<name> or variable:<id> in the locator")

    start, end = _window(cfg, spec)
    params: dict = {
        "api_key": api_key,
        "year": ",".join(str(y) for y in range(start.year, end.year + 1)),
        "state": spec.locator_part("state") or "all",
    }
    if report:
        params["report"] = report
    if variable:
        params["variable"] = variable
    category = spec.locator_part("category")
    if category:
        params["category"] = category
    category_value = spec.locator_part("category_value")
    if category_value:
        params["category_value"] = category_value

    payload = http.get_json("https://api.ers.usda.gov/data/arms/surveydata", params=params)

    # ARMS wraps results under a top-level key whose exact name isn't
    # independently verifiable without a live key; accept the documented
    # possibilities and fail loudly, naming the real keys, if none match.
    records = None
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        for key in ("data", "Data", "results", "Results", "surveydata"):
            if isinstance(payload.get(key), list):
                records = payload[key]
                break
    if records is None:
        raise ValueError(
            f"ARMS response for report={report!r} variable={variable!r} did not contain a "
            f"recognized results list; actual top-level keys: "
            f"{list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__}"
        )
    if not records:
        raise ValueError(f"ARMS returned no records for report={report!r} variable={variable!r}")

    raw = pd.DataFrame(records)
    year_col = next((c for c in ("Year", "year", "YEAR") if c in raw.columns), None)
    value_col = next((c for c in ("Value", "value", "VALUE", "Estimate", "estimate") if c in raw.columns), None)
    if year_col is None or value_col is None:
        raise ValueError(
            f"ARMS response for report={report!r} variable={variable!r} is missing a recognized "
            f"year/value column; actual columns: {list(raw.columns)}"
        )

    obs_date = pd.to_datetime(raw[year_col].astype(str) + "-01-01", format="%Y-%m-%d", errors="coerce")
    value = pd.to_numeric(raw[value_col].astype(str).str.replace(",", ""), errors="coerce")
    return pd.DataFrame({"obs_date": obs_date, "value": value}).dropna(subset=["obs_date"])


def frankfurter(spec, http, cfg) -> pd.DataFrame:
    """
    Frankfurter API: free ECB reference exchange rates. No key required.

    Used as a keyless fallback for FX pairs when FRED is unavailable.
    Historical data starts 1999-01-04. Locator: base:USD | target:EUR format.
    """
    base = spec.locator_part("base") or "USD"
    target = spec.locator_part("target")
    if not target:
        raise ValueError("No target currency declared in locator (format: base:USD | target:EUR)")

    start, end = _window(cfg, spec)

    payload = http.get_json(
        f"https://api.frankfurter.dev/v1/{start.strftime('%Y-%m-%d')}..{end.strftime('%Y-%m-%d')}",
        params={
            "base": base,
            "symbols": target,
        },
    )

    rates = payload.get("rates", {})
    if not rates:
        raise ValueError(f"Frankfurter returned no rates for {base}/{target}")

    rows = []
    for date_str, rate_dict in rates.items():
        rate = rate_dict.get(target)
        if rate is not None:
            rows.append({"obs_date": date_str, "value": float(rate)})

    if not rows:
        raise ValueError(f"No {target} rates found in Frankfurter response")

    return pd.DataFrame({
        "obs_date": pd.to_datetime([r["obs_date"] for r in rows], errors="coerce"),
        "value": [r["value"] for r in rows],
    }).dropna(subset=["obs_date"])


# Currencies whose ISO 4217 code's first two letters do NOT match the BIS
# REF_AREA reporting-area code for that currency (verified live: GBP->GB,
# JPY->JP, NZD->NZ, BRL->BR all match the derive-from-code default below and
# need no entry here). EUR is the one confirmed exception — many individual
# Eurozone members each report the same rate redundantly, so the Euro-area
# aggregate is the sane default rather than an arbitrary member country.
_BIS_REF_AREA_OVERRIDES = {"EUR": "XM"}


def bis_fx(spec, http, cfg) -> pd.DataFrame:
    """
    Bank for International Settlements US dollar exchange rates (WS_XRU
    dataflow). Public, keyless SDMX-JSON API — genuinely no registration
    required at all, unlike Frankfurter (ECB-sourced, keyless but only from
    1999-01-04 onward). Used as a second-line FX backup, tried after
    Frankfurter in the fallback chain.

    Locator: 'currency:<3-letter code>' optionally followed by '| freq:M'
    (default monthly; 'A' for annual) and/or '| ref_area:<code>' to override
    the reporting area (default: derived from the currency code — see
    _BIS_REF_AREA_OVERRIDES for the one confirmed exception).
    """
    currency = (spec.locator_part("currency") or "").strip().upper()
    if not currency:
        raise ValueError("No currency declared in the registry locator (format: currency:<3-letter code>)")

    freq = (spec.locator_part("freq") or "M").strip().upper()
    ref_area = (spec.locator_part("ref_area") or "").strip().upper()
    if not ref_area:
        ref_area = _BIS_REF_AREA_OVERRIDES.get(currency, currency[:2])

    payload = http.get_json(
        f"https://stats.bis.org/api/v1/data/BIS,WS_XRU,1.0/{freq}.{ref_area}.{currency}.A",
        headers={"Accept": "application/vnd.sdmx.data+json"},
    )

    data = payload.get("data") or {}
    datasets = data.get("dataSets") or []
    obs_dims = ((data.get("structure") or {}).get("dimensions") or {}).get("observation") or []
    if not datasets or not obs_dims:
        raise ValueError(f"BIS returned no data for currency {currency} (ref_area {ref_area})")

    time_values = obs_dims[0].get("values") or []  # TIME_PERIOD is the sole observation dimension
    series = datasets[0].get("series") or {}
    if not series:
        raise ValueError(f"BIS returned no series for currency {currency} (ref_area {ref_area})")

    # Every series/dimension component is already pinned to exactly one
    # value by the URL (freq/ref_area/currency/collection), so exactly one
    # series key is expected regardless of its literal index string.
    observations = next(iter(series.values())).get("observations") or {}

    rows = []
    for obs_index, obs_values in observations.items():
        idx = int(obs_index)
        if idx >= len(time_values) or not obs_values:
            continue
        value = obs_values[0]
        if value is None:
            continue
        rows.append({"period": time_values[idx]["id"], "value": value})

    if not rows:
        raise ValueError(f"BIS returned no populated observations for currency {currency} (ref_area {ref_area})")

    raw = pd.DataFrame(rows)
    obs_date = pd.to_datetime(raw["period"], format="%Y-%m", errors="coerce")
    if obs_date.isna().mean() > 0.5:
        obs_date = pd.to_datetime(raw["period"], errors="coerce")

    return pd.DataFrame({
        "obs_date": obs_date,
        "value": pd.to_numeric(raw["value"], errors="coerce"),
    }).dropna(subset=["obs_date"])


def eia(spec, http, cfg) -> pd.DataFrame:
    """
    EIA (U.S. Energy Information Administration) API. Free key required.

    Accepts two locator shapes:
      1. A bare classic series ID (e.g. "PET.RWTC.D") — the format naturally
         produced by minimal-input enrichment, since that's EIA's long-
         documented series-ID convention. Fetched via EIA's own backward-
         compatible endpoint, /v2/seriesid/{id}, which auto-translates a v1-
         style ID into a v2 response with no route/facets needed at all.
      2. An explicit route:<api_route> | series:<series_id> | facets:<key>:<value>
         locator, for the newer route+facets API shape, when one is declared.
    """
    api_key = cfg.credential("eia_api_key")
    if not api_key:
        raise PermissionError("EIA API key missing. Register free at "
                              "https://www.eia.gov/opendata/register/")

    route = spec.locator_part("route")
    series_id = spec.locator_part("series")

    if route and series_id:
        # Explicit route+facets locator.
        params = {
            "api_key": api_key,
            "frequency": "monthly",
            "data[0]": "value",
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": 0,
            "length": 5000,
        }
        facet_str = spec.locator_part("facets")
        if facet_str:
            parts = facet_str.split(":")
            if len(parts) >= 2:
                params[f"facets[{parts[0]}][0]"] = ":".join(parts[1:])
        url = f"https://api.eia.gov/v2/{route}/data/"
        descriptor = f"route {route} series {series_id}"
    else:
        # Bare classic series ID — no route/facets declared. Use series_id if
        # a "series:" fragment was given without a route, else fall back to
        # the whole locator string as-is (the common case for enrichment).
        bare_series = series_id or str(spec.endpoint_or_locator).strip()
        if not bare_series:
            raise ValueError("EIA connector requires a series ID in the locator")
        params = {"api_key": api_key, "offset": 0, "length": 5000}
        url = f"https://api.eia.gov/v2/seriesid/{bare_series}"
        descriptor = f"series {bare_series}"

    try:
        payload = http.get_json(url, params=params)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"EIA API request failed: {exc}")

    data = payload.get("response", {}).get("data", [])
    if not data:
        raise ValueError(f"EIA returned no data for {descriptor}")

    rows = []
    for item in data:
        try:
            period = str(item.get("period", ""))
            value = item.get("value")
            if period and value is not None:
                # EIA period format for monthly: YYYYMM
                if len(period) == 6 and period.isdigit():
                    date_str = f"{period[:4]}-{period[4:6]}-01"
                else:
                    date_str = period
                rows.append({"obs_date": date_str, "value": float(value)})
        except (TypeError, ValueError):
            continue

    if not rows:
        raise ValueError(f"No valid observations extracted from EIA response")

    return pd.DataFrame({
        "obs_date": pd.to_datetime([r["obs_date"] for r in rows], errors="coerce"),
        "value": [r["value"] for r in rows],
    }).dropna(subset=["obs_date"])


def mla_market_info(spec, http, cfg) -> pd.DataFrame:
    """Meat & Livestock Australia saleyard indicators. Registration-gated."""
    raise PermissionError(
        "MLA market information requires a registered login. Set credentials.mla_username "
        "and credentials.mla_password in config.yaml, or export the indicator series and "
        "drop it at input/manual/AU_PLT_SUBST.csv"
    )


def manual_yaml(spec, http, cfg) -> pd.DataFrame:
    """
    Terminal connector for sources with no automatable route.

    Always raises, which routes the driver to the human-in-the-loop register.
    That is the designed behaviour, not a gap: paid capacity databases and
    licensed market-size series should be a conscious procurement decision.
    """
    raise PermissionError(
        f"'{spec.source_name}' has no automatable access path. Obtain the series offline and "
        f"drop it at input/manual/{spec.driver_id}.csv with columns obs_date,value"
    )


# ---------------------------------------------------------------------------
# Registry of connectors, keyed by the 'connector' column of driver_registry.csv
# ---------------------------------------------------------------------------
CONNECTORS = {
    # Tier 1
    "bcb_sgs": bcb_sgs,
    "ibge_sidra": ibge_sidra,
    "eurostat": eurostat,
    "ccee_pld": ccee_pld,
    # Tier 2
    "worldbank_pinksheet": worldbank_pinksheet,
    "aip_tgp": aip_tgp,
    # Tier 3
    "woah_wahis": woah_wahis,
    "daff_au_outbreak": daff_au_outbreak,
    "pulp_output_composite": pulp_output_composite,
    "china_customs": china_customs,
    # Tier 4
    "fred": fred,
    "frankfurter": frankfurter,
    "bis_fx": bis_fx,
    "eia": eia,
    "nass_quickstats": nass_quickstats,
    "un_comtrade": un_comtrade,
    "usda_psd": usda_psd,
    "usda_ers_arms": usda_ers_arms,
    "mla_market_info": mla_market_info,
    "manual_yaml": manual_yaml,
}
