"""
Tier 1: the value is directly addressable on a URL.

Two sub-cases:
  a) A structured endpoint returns JSON or CSV (BCB SGS, IBGE SIDRA, CCEE).
     Handled by a named connector that knows the response shape.
  b) The value sits in an HTML table on a page. Handled generically: parse all
     tables, score them for date-plus-numeric structure, pick the best match,
     optionally asking the LLM to disambiguate when several look plausible.

Tier 1 is attempted first for everything, including drivers declared at a
higher tier, because sources occasionally expose a quiet JSON endpoint that
makes the harder path unnecessary.
"""

from __future__ import annotations

import io
import logging
import re
from typing import Optional

import pandas as pd

from ..models import DriverSpec, FetchResult, Outcome, Tier
from .base import TierHandler

log = logging.getLogger(__name__)

# Column header words that usually mark the date column of a scraped table.
DATE_HINTS = ("date", "month", "period", "data", "mes", "week", "time", "año", "ano")


class Tier1Direct(TierHandler):
    tier = Tier.DIRECT

    def can_handle(self, spec: DriverSpec) -> bool:
        # A named open API connector, or any page we can try to table-scrape.
        if spec.connector in self.connectors and spec.access_mode in ("open_api", "html_table"):
            return True
        return spec.access_mode == "html_table" or spec.declared_tier == Tier.DIRECT

    def fetch(self, spec: DriverSpec) -> FetchResult:
        # Path A: a purpose-built connector for this source.
        adapter = self.connectors.get(spec.connector)
        if adapter is not None and spec.access_mode == "open_api":
            log.info("[T1] %s via connector '%s'", spec.driver_id, spec.connector)
            return self._guard(spec, adapter, spec, self.http, self.cfg)

        # Path B: generic HTML table scrape.
        log.info("[T1] %s via generic HTML table scrape of %s", spec.driver_id, spec.source_url)
        return self._guard(spec, self._scrape_html_table, spec)

    # ------------------------------------------------------------------ path B

    def _scrape_html_table(self, spec: DriverSpec) -> pd.DataFrame:
        """
        Pull every table off the page, score them, and parse the best one.

        Scoring rewards tables that look like a time series: a parseable date
        column, a high share of numeric cells, and enough rows to be a history
        rather than a summary box.
        """
        html = self.http.get_text(spec.source_url)
        try:
            tables = pd.read_html(io.StringIO(html))
        except ValueError as exc:
            raise ValueError(f"No HTML tables found on {spec.source_url}") from exc

        scored: list[tuple[float, pd.DataFrame]] = []
        for table in tables:
            score = self._score_table(table, spec)
            if score > 0:
                scored.append((score, table))

        if not scored:
            raise ValueError(f"No table on {spec.source_url} resembles a time series")

        scored.sort(key=lambda pair: pair[0], reverse=True)

        # If the top two candidates are close, the page is ambiguous. Ask the
        # LLM which one matches the driver, rather than guessing on a tie.
        if self.llm and len(scored) > 1 and (scored[0][0] - scored[1][0]) < 0.15:
            chosen = self.llm.choose_table(spec, [t for _, t in scored[:4]])
            if chosen is not None:
                return self._table_to_observations(chosen, spec)

        return self._table_to_observations(scored[0][1], spec)

    @staticmethod
    def _score_table(table: pd.DataFrame, spec: DriverSpec) -> float:
        """Heuristic 0-1 score for how much a table looks like this driver's series."""
        if table.empty or table.shape[1] < 2 or len(table) < 6:
            return 0.0

        columns = [str(c).lower() for c in table.columns]
        score = 0.0

        # A date-like column name is the strongest single signal.
        if any(hint in col for col in columns for hint in DATE_HINTS):
            score += 0.4

        # A column that actually parses as dates is stronger still.
        for col in table.columns:
            parsed = pd.to_datetime(table[col], errors="coerce", format="mixed")
            if parsed.notna().mean() > 0.7:
                score += 0.3
                break

        # Numeric density in the remaining columns.
        numeric_share = sum(
            pd.to_numeric(table[c], errors="coerce").notna().mean() for c in table.columns
        ) / max(len(table.columns), 1)
        score += 0.2 * numeric_share

        # A column header echoing the registry's series name is a direct hit.
        series_hint = (spec.locator_part("series") or spec.driver_name).lower()
        tokens = [t for t in re.split(r"[^a-z]+", series_hint) if len(t) > 3]
        if tokens and any(tok in col for col in columns for tok in tokens):
            score += 0.2

        return min(score, 1.0)

    @staticmethod
    def _table_to_observations(table: pd.DataFrame, spec: DriverSpec) -> pd.DataFrame:
        """Reduce a scraped table to [obs_date, value]."""
        table = table.copy()
        table.columns = [str(c).strip() for c in table.columns]

        # Date column: first one that parses cleanly.
        date_col = None
        for col in table.columns:
            if pd.to_datetime(table[col], errors="coerce", format="mixed").notna().mean() > 0.7:
                date_col = col
                break
        if date_col is None:
            raise ValueError("Could not identify a date column in the scraped table")

        # Value column: prefer a header matching the registry series name,
        # otherwise the most numeric column that is not the date column.
        series_hint = (spec.locator_part("series") or "").lower()
        value_col = None
        if series_hint:
            for col in table.columns:
                if col != date_col and series_hint in col.lower():
                    value_col = col
                    break
        if value_col is None:
            candidates = [
                (pd.to_numeric(table[c], errors="coerce").notna().mean(), c)
                for c in table.columns if c != date_col
            ]
            candidates.sort(reverse=True)
            if not candidates or candidates[0][0] < 0.5:
                raise ValueError("No sufficiently numeric value column in the scraped table")
            value_col = candidates[0][1]

        return pd.DataFrame({
            "obs_date": pd.to_datetime(table[date_col], errors="coerce", format="mixed"),
            "value": table[value_col],
        }).dropna(subset=["obs_date"])
