"""
Tier 3: the series is spread across many pages.

Four patterns cover almost every real case:

  PERIOD_URL   One page per period, with the period in the URL.
               e.g. .../statistics/2023/04 . Cheapest to crawl because the
               page set is generated, not discovered.

  PAGINATED    A list view with page=1..N, each page holding a slice of rows.
               Crawled until a page yields no new rows or the cap is hit.

  JSON_API     A JavaScript dashboard backed by an undocumented JSON endpoint
               (WOAH WAHIS works this way). Far more reliable than driving a
               headless browser, so we call the endpoint directly with an
               offset/limit loop.

  INDEX_CRAWL  An index page linking to per-release detail pages. We collect
               the links, then extract from each one.

Every pattern shares the same contract: yield partial frames, concatenate,
de-duplicate on date. Partial success is real success here, because a crawl
that gets nine of ten pages still produces a usable series, reported as a
plain success with a quality note rather than a separate outcome category.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Iterable, Optional
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from ..models import DriverSpec, FetchResult, Outcome, Tier
from .base import TierHandler
from .tier1_direct import Tier1Direct

log = logging.getLogger(__name__)

MAX_PAGES = 200          # hard stop so a pagination bug cannot crawl forever
MAX_EMPTY_PAGES = 3      # consecutive empty pages that end a crawl


def _time_budget(cfg) -> float:
    """
    Wall-clock budget (seconds) for one crawl attempt, config-driven (see
    http.max_retrieval_seconds). MAX_PAGES alone bounds the page *count*, not
    elapsed time — a source that is merely slow or partially erroring can
    still burn many minutes even within that cap, since each page still pays
    its own full timeout+retry budget. This is the wall-clock backstop.
    """
    return float(cfg.get("http.max_retrieval_seconds", 180))


class Tier3MultiPage(TierHandler):
    tier = Tier.MULTIPAGE

    def can_handle(self, spec: DriverSpec) -> bool:
        # A gated API/login source has no page to crawl either — see the
        # identical guard and rationale in Tier2EmbeddedFile.can_handle.
        if spec.access_mode in ("api_key", "login", "paid_or_restricted"):
            return False
        return spec.access_mode == "multipage_scrape" or spec.declared_tier >= Tier.MULTIPAGE

    def fetch(self, spec: DriverSpec) -> FetchResult:
        adapter = self.connectors.get(spec.connector)
        if adapter is not None:
            log.info("[T3] %s via connector '%s'", spec.driver_id, spec.connector)
            return self._guard(spec, adapter, spec, self.http, self.cfg)

        # Generic route: infer the crawl pattern from the locator string.
        locator = str(spec.endpoint_or_locator).lower()
        if "private_json_api" in locator or "json" in locator:
            return self._guard(spec, self._crawl_json_api, spec)
        if "page=" in locator or "paginated" in locator:
            return self._guard(spec, self._crawl_paginated, spec)
        if "{year}" in spec.source_url or "{month}" in spec.source_url:
            return self._guard(spec, self._crawl_period_urls, spec)
        return self._guard(spec, self._crawl_index, spec)

    # ----------------------------------------------------------- crawl modes

    def _crawl_period_urls(self, spec: DriverSpec) -> pd.DataFrame:
        """
        One page per period. The URL template carries {year} and {month}.

        Unlike the other three crawl patterns, this one has no natural page
        count cap (MAX_PAGES) — it visits every month in the fetch window,
        which can be hundreds of pages for a long history_from. Bounded here
        by wall-clock time instead.
        """
        start = pd.Timestamp(self.cfg.get("run.start_date", "2014-01-01"))
        end = pd.Timestamp(self.cfg.get("run.end_date") or pd.Timestamp.today())
        collected, failures = [], 0
        crawl_start = time.monotonic()
        budget = _time_budget(self.cfg)

        for period in pd.date_range(start, end, freq="MS"):
            if time.monotonic() - crawl_start > budget:
                log.warning("Period crawl for %s exceeded %.0fs budget; stopping with %d page(s) collected",
                           spec.driver_id, budget, len(collected))
                break
            url = (spec.source_url
                   .replace("{year}", f"{period.year}")
                   .replace("{month}", f"{period.month:02d}"))
            try:
                html = self.http.get_text(url)
            except Exception as exc:  # noqa: BLE001 - a missing month is normal
                failures += 1
                log.debug("Period page missing (%s): %s", exc, url)
                continue

            rows = self._tables_from_html(html, spec)
            if rows is not None and not rows.empty:
                # Stamp the period from the URL when the page itself omits it.
                rows["obs_date"] = rows["obs_date"].fillna(period)
                collected.append(rows)

        if not collected:
            raise ValueError(f"Period crawl produced no rows across {failures} attempted pages")
        return _combine(collected)

    def _crawl_paginated(self, spec: DriverSpec) -> pd.DataFrame:
        """List view with a page parameter. Stops on repeated empty pages."""
        base = spec.source_url
        collected, empty_streak = [], 0
        crawl_start = time.monotonic()
        budget = _time_budget(self.cfg)

        for page in range(1, MAX_PAGES + 1):
            if time.monotonic() - crawl_start > budget:
                log.warning("Paginated crawl for %s exceeded %.0fs budget at page %d; stopping with %d page(s) collected",
                           spec.driver_id, budget, page, len(collected))
                break
            url = f"{base}{'&' if '?' in base else '?'}page={page}"
            try:
                html = self.http.get_text(url)
            except Exception as exc:  # noqa: BLE001
                log.info("Pagination stopped at page %d: %s", page, exc)
                break

            rows = self._tables_from_html(html, spec)
            if rows is None or rows.empty:
                empty_streak += 1
                if empty_streak >= MAX_EMPTY_PAGES:
                    break
                continue

            empty_streak = 0
            collected.append(rows)

        if not collected:
            raise ValueError("Paginated crawl returned no usable rows")
        return _combine(collected)

    def _crawl_json_api(self, spec: DriverSpec) -> pd.DataFrame:
        """
        Offset/limit loop against a dashboard's backing JSON endpoint.

        The locator carries the endpoint path and any filters, for example:
          'private_json_api:/pi/getReportList | filters: disease=...; country=...'
        """
        endpoint = spec.locator_part("private_json_api")
        if not endpoint:
            raise NotImplementedError("No private_json_api endpoint declared in the locator")

        base = spec.source_url.split("#")[0].rstrip("/")
        url = urljoin(base + "/", endpoint.lstrip("/"))
        filters = _parse_filters(spec.locator_part("filters") or "")

        collected, offset, page_size = [], 0, 100
        crawl_start = time.monotonic()
        budget = _time_budget(self.cfg)
        for _ in range(MAX_PAGES):
            if time.monotonic() - crawl_start > budget:
                log.warning("JSON API crawl for %s exceeded %.0fs budget at offset %d; stopping with %d page(s) collected",
                           spec.driver_id, budget, offset, len(collected))
                break
            payload = {**filters, "offset": offset, "limit": page_size}
            data = self.http.request(url, method="POST", json_body=payload).json()
            records = data if isinstance(data, list) else data.get("data") or data.get("results") or []
            if not records:
                break
            collected.append(pd.json_normalize(records))
            if len(records) < page_size:
                break
            offset += page_size

        if not collected:
            raise ValueError("JSON endpoint returned no records")

        raw = pd.concat(collected, ignore_index=True)
        return _records_to_observations(raw)

    def _crawl_index(self, spec: DriverSpec) -> pd.DataFrame:
        """Index page of releases; follow each link and extract from the detail page."""
        html = self.http.get_text(spec.source_url)
        soup = BeautifulSoup(html, "html.parser")

        # Only follow links that look like a dated release, to avoid crawling
        # the whole site.
        links = []
        for anchor in soup.find_all("a", href=True):
            text = anchor.get_text(" ", strip=True)
            href = urljoin(spec.source_url, anchor["href"])
            if re.search(r"(19|20)\d{2}", f"{text} {href}"):
                links.append(href)

        seen, collected = set(), []
        crawl_start = time.monotonic()
        budget = _time_budget(self.cfg)
        for href in links[:MAX_PAGES]:
            if time.monotonic() - crawl_start > budget:
                log.warning("Index crawl for %s exceeded %.0fs budget after %d link(s); stopping with %d page(s) collected",
                           spec.driver_id, budget, len(seen), len(collected))
                break
            if href in seen:
                continue
            seen.add(href)
            try:
                detail = self.http.get_text(href)
            except Exception:  # noqa: BLE001
                continue
            rows = self._tables_from_html(detail, spec)
            if rows is not None and not rows.empty:
                collected.append(rows)

        if not collected:
            raise ValueError(f"Index crawl of {spec.source_url} produced no rows")
        return _combine(collected)

    # -------------------------------------------------------------- utilities

    def _tables_from_html(self, html: str, spec: DriverSpec) -> Optional[pd.DataFrame]:
        """Reuse the Tier 1 table scorer so page parsing logic exists once."""
        import io
        try:
            tables = pd.read_html(io.StringIO(html))
        except ValueError:
            return None

        scorer = Tier1Direct(self.cfg, self.http, self.connectors, self.llm)
        best, best_score = None, 0.0
        for table in tables:
            score = scorer._score_table(table, spec)  # noqa: SLF001 - intentional reuse
            if score > best_score:
                best, best_score = table, score

        if best is None or best_score < 0.4:
            return None
        try:
            return scorer._table_to_observations(best, spec)  # noqa: SLF001
        except ValueError:
            return None


def _combine(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate crawl fragments, keeping the latest row per date."""
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["obs_date"])
    return combined.drop_duplicates(subset=["obs_date"], keep="last").sort_values("obs_date")


def _parse_filters(text: str) -> dict:
    """Turn 'disease=HPAI; country=Australia' into a dict."""
    filters = {}
    for part in text.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            filters[key.strip()] = value.strip()
    return filters


def _records_to_observations(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce an event-record frame to [obs_date, value].

    Event streams have no value column: each record is one occurrence, so the
    value is 1 and the rollup method 'count' turns them into monthly counts.
    """
    date_col = next(
        (c for c in raw.columns
         if any(k in c.lower() for k in ("date", "reportdate", "eventdate", "startdate"))),
        None,
    )
    if date_col is None:
        raise ValueError("No date-like field in the JSON records")

    value_col = next(
        (c for c in raw.columns
         if any(k in c.lower() for k in ("outbreak", "cases", "count", "measure", "value"))),
        None,
    )

    return pd.DataFrame({
        "obs_date": pd.to_datetime(raw[date_col], errors="coerce", format="mixed"),
        "value": pd.to_numeric(raw[value_col], errors="coerce") if value_col else 1.0,
    }).dropna(subset=["obs_date"])
