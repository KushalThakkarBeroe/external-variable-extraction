"""
LLM assistance, scoped narrowly on purpose.

The model is used for narrow jobs where deterministic code is genuinely weak:

  1. choose_table              Several tables on a page could be the series.
  2. locate_series_in_sheet    A workbook's header layout defeats the parser.
  3. extract_series_from_text  The only copy of a series is PDF prose.
  4. propose_source            Cold-start source discovery for a new driver.
  5. reclassify_tier           Every declared tier failed; re-read the page and
                               say what the real access pattern is.
  6. select_eurostat_dataset   A Eurostat dataset code doesn't exist; pick the
                               real replacement from a shortlist of Eurostat's
                               own catalogue, never invent a new code.
  7. select_eurostat_filters   A Eurostat locator's filters are wrong/missing;
                               pick real category codes from the dataset's own
                               dimension menu, never invent a filter value.

Guardrails that matter for a forecasting pipeline:
  - For jobs 1 and 2 the model returns COORDINATES, never numbers. The values
    are then read from the actual file, so the model cannot hallucinate data.
  - For job 3, where it must return values, every row must carry the source
    line it came from, and rows whose value does not appear in that line are
    discarded before returning.
  - Strict JSON output and a hard cap on retries. Temperature is left at the
    API default (not set) since some newer models reject an explicit value.

If llm.enabled is false or the SDK is absent, every method returns None and the
pipeline continues on deterministic paths alone.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

import pandas as pd

log = logging.getLogger(__name__)


class LlmHelper:
    def __init__(self, cfg):
        self.cfg = cfg
        self.enabled = bool(cfg.get("llm.enabled", False))
        self.model = cfg.get("llm.model", "claude-sonnet-4-6")
        self.max_tokens = int(cfg.get("llm.max_tokens", 4000))
        self.client = None

        if not self.enabled:
            return
        api_key = cfg.get("llm.api_key")
        if not api_key:
            log.warning("LLM enabled but no API key resolved; LLM assistance disabled")
            self.enabled = False
            return
        try:
            import anthropic
            self.client = anthropic.Anthropic(api_key=api_key)
        except ImportError:
            log.warning("anthropic SDK not installed; LLM assistance disabled")
            self.enabled = False

    # ------------------------------------------------------------------ core

    def _ask(self, system: str, prompt: str) -> Optional[dict]:
        """Single JSON-returning call. Returns None on any failure."""
        if not self.enabled or self.client is None:
            return None
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "".join(block.text for block in response.content if block.type == "text")
            # Strip code fences the model may add despite instructions.
            text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
            return json.loads(text)
        except Exception as exc:  # noqa: BLE001
            log.warning("LLM call failed: %s", exc)
            return None

    # -------------------------------------------------------------- job 1

    def choose_table(self, spec, tables: list[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Pick which of several scraped tables holds the driver's series."""
        previews = []
        for idx, table in enumerate(tables):
            previews.append(f"--- TABLE {idx} ---\n{table.head(8).to_string(max_cols=12)}")

        result = self._ask(
            system=("You identify which HTML table contains a specific economic time series. "
                    "Reply with JSON only: {\"table_index\": <int>, \"confidence\": <0-1>}. "
                    "If none match, use table_index -1."),
            prompt=(f"Series wanted: {spec.driver_name}\n"
                    f"Commodity: {spec.commodity} | Region: {spec.region}\n"
                    f"Expected unit: {spec.unit}\n"
                    f"Expected frequency: {spec.native_frequency}\n\n"
                    + "\n\n".join(previews)),
        )
        if not result:
            return None
        index = int(result.get("table_index", -1))
        if index < 0 or index >= len(tables) or float(result.get("confidence", 0)) < 0.5:
            return None
        log.info("LLM selected table %d for %s", index, spec.driver_id)
        return tables[index]

    # -------------------------------------------------------------- job 2

    def locate_series_in_sheet(self, spec, grid: pd.DataFrame, sheet_name: str) -> Optional[dict]:
        """
        Return {date_col, value_col, first_data_row} for a stubborn workbook.

        The caller reads those cells from the file itself, so the model never
        supplies a value.
        """
        preview = grid.head(25).to_string(max_cols=25, max_colwidth=20)
        result = self._ask(
            system=("You locate a named data series inside a spreadsheet grid given by "
                    "zero-indexed row and column positions. Reply with JSON only: "
                    "{\"date_col\": <int>, \"value_col\": <int>, \"first_data_row\": <int>, "
                    "\"confidence\": <0-1>}. Use -1 for every field if the series is absent."),
            prompt=(f"Series wanted: {spec.locator_part('series') or spec.driver_name}\n"
                    f"Unit: {spec.unit} | Frequency: {spec.native_frequency}\n"
                    f"Sheet: {sheet_name}\n\nFirst 25 rows:\n{preview}"),
        )
        if not result or float(result.get("confidence", 0)) < 0.6:
            return None
        if min(int(result.get("date_col", -1)), int(result.get("value_col", -1))) < 0:
            return None
        log.info("LLM located %s at cols (%s,%s) row %s on sheet %s", spec.driver_id,
                 result["date_col"], result["value_col"], result["first_data_row"], sheet_name)
        return result

    # -------------------------------------------------------------- job 3

    def extract_series_from_text(self, spec, text: str) -> pd.DataFrame:
        """
        Extract dated values from PDF or narrative text.

        Each returned row must carry the source line. Rows whose numeric value
        does not literally appear in their quoted line are dropped, so a
        fabricated figure cannot survive into the panel.
        """
        result = self._ask(
            system=("You extract a time series from document text. Reply with JSON only: "
                    "{\"observations\": [{\"date\": \"YYYY-MM-DD\", \"value\": <number>, "
                    "\"source_line\": \"<the exact line the value came from>\"}]}. "
                    "Never infer, interpolate or estimate a value that is not written in the "
                    "text. Return an empty list if the series is not present."),
            prompt=(f"Series wanted: {spec.driver_name}\n"
                    f"Commodity: {spec.commodity} | Region: {spec.region}\n"
                    f"Unit: {spec.unit} | Frequency: {spec.native_frequency}\n\n"
                    f"Document text:\n{text[:60000]}"),
        )
        if not result:
            return pd.DataFrame(columns=["obs_date", "value"])

        rows = []
        for item in result.get("observations", []):
            try:
                value = float(item["value"])
            except (KeyError, TypeError, ValueError):
                continue
            # Verification gate: the number must appear in its own source line.
            line = str(item.get("source_line", ""))
            digits = re.sub(r"[^\d]", "", f"{value:.10g}")
            if digits and digits not in re.sub(r"[^\d]", "", line):
                log.warning("Dropping unverifiable LLM value %s for %s", value, spec.driver_id)
                continue
            rows.append({"obs_date": item.get("date"), "value": value})

        log.info("LLM extracted %d verified observations for %s", len(rows), spec.driver_id)
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["obs_date", "value"])

    # -------------------------------------------------------------- job 4

    def propose_source(self, commodity: str, driver_name: str, region: str,
                       unit: str, already_tried: Optional[list[dict]] = None) -> Optional[dict]:
        """
        Cold-start source discovery for a new driver from minimal input.

        Given only commodity, driver_name, region, and unit, proposes:
        source_name, source_url, connector, access_mode, endpoint_or_locator,
        declared_tier, native_frequency, rollup_method, confidence, reasoning.

        IMPORTANT: This job proposes METADATA, not values. Unlike the other
        four jobs (which verify coordinates/values against source text), this
        one produces best-guess metadata that must then be validated by the
        pipeline actually attempting to fetch the data. A wrong guess is
        caught by the fetch failing and landing in a HITL task, exactly like
        a wrong analyst entry would be today.

        `already_tried` is used reactively (see CascadeOrchestrator's
        alternate-source escalation step): a list of {"source_name",
        "source_url", "connector"} dicts for sources already known not to
        work for this exact driver, across this run and prior ones. When
        given, the model is told to propose a genuinely different
        publisher/domain rather than a variant of one already excluded.

        Returns None if the LLM fails, is disabled, or returns unparseable JSON.
        """
        from .connectors import CONNECTORS

        connector_list = ", ".join(sorted(CONNECTORS.keys()))

        exclusion_clause = ""
        if already_tried:
            tried_lines = "\n".join(
                f"  - {t.get('source_name') or '(unnamed)'} ({t.get('source_url', '')}), "
                f"connector: {t.get('connector') or 'none'}"
                for t in already_tried
            )
            exclusion_clause = (
                "\n\nThe following sources have ALREADY been tried for this exact driver and "
                "did not work — do not propose any of them again, and do not propose a "
                "different page on the same site/domain as any of them. Propose a genuinely "
                f"different publisher instead:\n{tried_lines}"
            )

        result = self._ask(
            system=(
                "You propose data sources for an economic time series. "
                "Reply with JSON only: "
                "{\"source_name\": \"<publisher name>\", "
                "\"source_url\": \"<URL of landing page>\", "
                "\"connector\": \"<connector name or blank>\", "
                "\"access_mode\": \"<mode>\", "
                "\"endpoint_or_locator\": \"<locator string>\", "
                "\"declared_tier\": <1|2|3|4>, "
                "\"native_frequency\": \"<Daily|Weekly|Monthly|Quarterly|Annual|Event>\", "
                "\"rollup_method\": \"<mean|sum|last|first|count|max|min|median>\", "
                "\"confidence\": <0-1>, "
                "\"reasoning\": \"<one sentence>\"}. "
                "Prefer a pre-built connector when it plausibly applies. "
                f"Available connectors: {connector_list}. "
                "If none fits, leave connector blank. "
                "For tier 4 (gated) sources, infer the access_mode "
                "(api_key, login, or paid_or_restricted). "
                "The URL must be publicly accessible (https://...) for the pipeline to work.\n\n"
                "If you choose one of the connectors below, endpoint_or_locator MUST follow "
                "its exact format — never a raw URL or a free-text description, since the "
                "connector code parses this string mechanically and cannot interpret prose:\n"
                "  fred -> series_id:<FRED series ID>  e.g. series_id:DEXUSEU\n"
                "  frankfurter -> base:<3-letter currency> | target:<3-letter currency>  "
                "e.g. base:USD | target:EUR (one currency PAIR only — never combine multiple "
                "pairs in one locator or one driver)\n"
                "  eia -> a bare classic series code (e.g. PET.RWTC.D) OR "
                "route:<api route> | series:<series id>\n"
                "  worldbank_pinksheet -> series:<exact Pink Sheet series name>  "
                "e.g. series:Soybean meal\n"
                "  ibge_sidra -> table:<SIDRA table code> optionally | variable:<code>\n"
                "  bcb_sgs -> bcdata.sgs:<series id>\n"
                "  aip_tgp -> sheet:<diesel|ulp> optionally | cities:<comma-separated list>\n"
                "  eurostat -> dataset:<Eurostat dataset code> optionally | filters:<key=value;key=value>\n"
                "  usda_psd -> commodity:<PSD commodity code> | attribute:<PSD attribute id> "
                "optionally | country:<2-letter code> (default: world aggregate). Annual data only — "
                "do not propose this connector for a driver that needs monthly/weekly frequency.\n"
                "  nass_quickstats / un_comtrade -> semicolon-separated key=value filters "
                "(NOT pipe/colon), e.g. commodity_desc=CATTLE; statisticcat_desc=INVENTORY\n\n"
                "If source_url is a Eurostat page (ec.europa.eu/eurostat/databrowser/... or "
                "ec.europa.eu/eurostat/api/...), you MUST set connector to eurostat with a "
                "dataset: locator — never leave connector blank for a Eurostat URL, since its "
                "databrowser is a JavaScript app no generic scraper can read.\n\n"
                "For any other connector, or when leaving connector blank, source_url must be "
                "the specific data or download page itself — never a section homepage or a "
                "topic/overview landing page one or more clicks away from the actual data. "
                "Before proposing connector: blank or manual_yaml, check whether the publisher "
                "exposes the series through an open, documented API or a directly downloadable "
                "file/table; only use manual_yaml when you are confident no such automatable "
                "path exists.\n\n"
                "When the identified publisher is a known commercial/subscription data vendor "
                "(e.g. Fastmarkets, Xeneta, S&P Global Platts, LME, Euromonitor, or similar paid "
                "market-data providers), set declared_tier: 4 and access_mode: paid_or_restricted "
                "immediately rather than proposing a lower tier that is guaranteed to fail first — "
                "there is no free automated path into a paywalled vendor."
                + exclusion_clause
            ),
            prompt=(
                f"Commodity: {commodity}\n"
                f"Driver name: {driver_name}\n"
                f"Region: {region}\n"
                f"Unit: {unit}\n\n"
                "Propose the best public data source for this series. "
                "Prioritize open APIs and free, direct data sources over paywalls."
            ),
        )
        if result:
            log.info("LLM proposed source for %s / %s / %s: %s (confidence %.2f)",
                    commodity, driver_name, region, result.get("source_name", "?"),
                    float(result.get("confidence", 0)))
        return result

    # -------------------------------------------------------------- job 5

    def reclassify_tier(self, spec, page_html: str) -> Optional[dict]:
        """
        Every declared tier failed. Re-read the landing page and report what the
        access pattern actually is, so the registry can be corrected.

        This is the pipeline's self-healing loop: the suggestion is written into
        the run manifest for an analyst to accept, never applied silently.
        """
        result = self._ask(
            system=("You classify how a webpage exposes statistical data. Reply with JSON only: "
                    "{\"tier\": 1|2|3|4, \"access_mode\": \"open_api|html_table|file_download|"
                    "multipage_scrape|api_key|login|paid_or_restricted\", "
                    "\"suggested_url\": \"<direct data URL if visible, else empty>\", "
                    "\"reasoning\": \"<one sentence>\", \"confidence\": <0-1>}. "
                    "Tier 1 = data on the URL; 2 = downloadable file; 3 = spread over pages; "
                    "4 = key, login or payment required."),
            prompt=(f"Driver: {spec.driver_name} ({spec.commodity}, {spec.region})\n"
                    f"Declared tier: {int(spec.declared_tier)} | Declared mode: {spec.access_mode}\n"
                    f"URL: {spec.source_url}\n\nPage HTML (truncated):\n{page_html[:40000]}"),
        )
        if result:
            log.info("LLM re-classified %s as tier %s (%s)", spec.driver_id,
                     result.get("tier"), result.get("access_mode"))
        return result

    # -------------------------------------------------------------- job 6

    def select_eurostat_dataset(self, spec, candidates: list[dict]) -> Optional[dict]:
        """
        A Eurostat dataset code in the registry doesn't exist on Eurostat's
        live API. Given a shortlist of REAL candidate datasets (code + title)
        from Eurostat's own catalogue, select the one that actually matches
        this driver — never invent a new code. The caller validates the
        returned code is literally one of the candidates before trusting it.
        """
        listing = "\n".join(f"  {c['code']}: {c['title']}" for c in candidates)
        result = self._ask(
            system=("You match an economic time series to the correct Eurostat dataset from a "
                    "list of real candidates. Reply with JSON only: "
                    "{\"dataset_code\": \"<one of the candidate codes, verbatim>\", "
                    "\"confidence\": <0-1>, \"reasoning\": \"<one sentence>\"}. "
                    "Only ever return a code that appears in the candidate list — never invent "
                    "one. If none plausibly match, return an empty dataset_code."),
            prompt=(f"Series wanted: {spec.driver_name}\n"
                    f"Commodity: {spec.commodity} | Region: {spec.region} | Unit: {spec.unit}\n\n"
                    f"Candidate Eurostat datasets:\n{listing}"),
        )
        if not result or not str(result.get("dataset_code", "")).strip():
            return None
        log.info("LLM selected Eurostat dataset %s for %s (confidence %.2f)",
                 result.get("dataset_code"), spec.driver_id, float(result.get("confidence", 0)))
        return result

    # -------------------------------------------------------------- job 7

    def select_eurostat_filters(self, spec, dimensions_menu: dict[str, dict[str, str]]) -> Optional[dict]:
        """
        A Eurostat locator's filters don't pin every real dimension the
        dataset actually has. Given the dataset's own REAL dimension/category
        menu ({dim_id: {category_code: category_label}}), select one category
        per dimension — never invent a code. The caller validates every
        returned pin is literally present in the real menu before trusting it.
        """
        menu_text = "\n".join(
            f"  {dim_id}:\n" + "\n".join(f"    {code}: {label}" for code, label in cats.items())
            for dim_id, cats in dimensions_menu.items()
        )
        result = self._ask(
            system=("You select filter values for a Eurostat dataset query from its real "
                    "dimension/category menu. Reply with JSON only: "
                    "{\"filters\": {\"<dim_id>\": \"<category code, verbatim from that "
                    "dimension's menu>\", ...}, \"confidence\": <0-1>, "
                    "\"reasoning\": \"<one sentence>\"}. "
                    "You MUST return exactly one category code per dimension listed below, and "
                    "every code must appear verbatim under that dimension in the menu — never "
                    "invent a code."),
            prompt=(f"Series wanted: {spec.driver_name}\n"
                    f"Commodity: {spec.commodity} | Region: {spec.region} | Unit: {spec.unit}\n\n"
                    f"Dimensions needing a filter value:\n{menu_text}"),
        )
        if not result or not result.get("filters"):
            return None
        log.info("LLM selected Eurostat filters %s for %s (confidence %.2f)",
                 result.get("filters"), spec.driver_id, float(result.get("confidence", 0)))
        return result
