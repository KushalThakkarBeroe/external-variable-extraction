"""
Tier 2: the value lives inside a file that is linked from a webpage.

Three steps, each of which can fail independently:

  1. DISCOVER  Find the file link on the landing page. Hardcoding the direct
               URL is brittle: the World Bank Pink Sheet lives behind a hashed
               path that changes with each edition, and AIP republishes its
               terminal gate price workbook with a new date each week. We
               therefore crawl the landing page and match link text.

  2. DOWNLOAD  Fetch to the raw cache with the shared HTTP client, so the exact
               artifact behind every number is retained for audit.

  3. EXTRACT   Locate the series inside the workbook or PDF. Statistical
               agencies use multi-row headers, footnote markers, merged cells
               and unit rows, so deterministic parsing is attempted first and
               the LLM is used only as a fallback on the file's text content.

The extractor never lets the LLM produce numbers from nothing: it is given the
sheet text and must return row/column coordinates, which we then read from the
actual file.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from ..models import DriverSpec, FetchResult, Outcome, Tier
from .base import TierHandler

log = logging.getLogger(__name__)

FILE_EXTENSIONS = (".xlsx", ".xls", ".xlsm", ".csv", ".pdf", ".zip")


def _time_budget(cfg) -> float:
    """Wall-clock budget (seconds) for one Tier 2 extraction attempt. See
    http.max_retrieval_seconds and the identical helper in tier3_multipage.py."""
    return float(cfg.get("http.max_retrieval_seconds", 180))


class Tier2EmbeddedFile(TierHandler):
    tier = Tier.EMBEDDED_FILE

    def can_handle(self, spec: DriverSpec) -> bool:
        # A gated API/login source has no file to discover on a landing page —
        # trying it here only reproduces the same block via generic scraping.
        if spec.access_mode in ("api_key", "login", "paid_or_restricted"):
            return False
        # Worth trying whenever a file is declared, or as an escalation from
        # Tier 1 on any page that might carry a download link.
        return spec.access_mode in ("file_download", "html_table") or spec.declared_tier >= Tier.EMBEDDED_FILE

    def fetch(self, spec: DriverSpec) -> FetchResult:
        # A dedicated connector wins when one exists: it knows the workbook's
        # quirks (header rows, unit rows, series naming) and is far more robust
        # than generic discovery.
        adapter = self.connectors.get(spec.connector)
        if adapter is not None and spec.access_mode == "file_download":
            log.info("[T2] %s via connector '%s'", spec.driver_id, spec.connector)
            return self._guard(spec, adapter, spec, self.http, self.cfg)

        log.info("[T2] %s via generic file discovery on %s", spec.driver_id, spec.source_url)
        return self._guard(spec, self._discover_and_extract, spec)

    # ------------------------------------------------------------------ steps

    def _discover_and_extract(self, spec: DriverSpec) -> pd.DataFrame:
        file_url = self.discover_file_link(spec.source_url, spec.locator_part("file"))
        if not file_url:
            raise ValueError(f"No downloadable data file found on {spec.source_url}")

        suffix = Path(file_url.split("?")[0]).suffix.lower() or ".bin"
        local = self.http.download(file_url, suffix=suffix)

        # The URL's extension is only a guess, and a dynamic export link
        # often has none at all (falls back to ".bin"). Confirm the real
        # type from the file's own magic bytes before trusting the URL —
        # this is what a Content-Type-driven download link actually needs.
        sniffed = _sniff_suffix(local)
        if sniffed and sniffed != local.suffix.lower():
            renamed = local.with_suffix(sniffed)
            local.replace(renamed)
            local = renamed

        if local.suffix.lower() == ".zip":
            local = self._extract_member_from_zip(local)

        result = self.extract_from_file(local, spec)
        # Stamped on .attrs (not a new return type) so _guard() can recover
        # it generically for any tier/connector — see tiers/base.py.
        result.attrs["raw_artifact_path"] = str(local)
        return result

    def _extract_member_from_zip(self, path: Path) -> Path:
        """
        Unpack the first CSV/Excel member from a real (non-Office) zip
        archive — several statistical agencies (Eurostat bulk downloads,
        various ministries) ship a zipped CSV rather than a raw one.
        Picks the largest matching member, since archives commonly bundle a
        small readme/metadata file alongside the actual data export.
        """
        import zipfile

        with zipfile.ZipFile(path) as zf:
            candidates = [n for n in zf.namelist() if n.lower().endswith((".csv", ".xlsx", ".xls", ".xlsm"))]
            if not candidates:
                raise ValueError(f"Zip archive {path.name} contains no CSV/Excel member")
            member = max(candidates, key=lambda n: zf.getinfo(n).file_size)
            data = zf.read(member)

        inner_path = path.with_name(f"{path.stem}_extracted{Path(member).suffix.lower()}")
        inner_path.write_bytes(data)
        return inner_path

    def discover_file_link(self, page_url: str, filename_hint: Optional[str] = None) -> Optional[str]:
        """
        Crawl a landing page for the most likely data file link.

        Ranking, in order of confidence:
          1. Link href or text contains the registry's filename hint
          2. Link text mentions the data type (historical, monthly, data)
          3. Any spreadsheet-like extension, most recent-looking first

        If `page_url` itself already looks like a direct file (ends in one of
        FILE_EXTENSIONS), there is nothing to discover -- return it as-is.
        Some registry rows declare the exact file URL directly rather than a
        landing page to crawl (e.g. a source someone already pinned down by
        hand); without this check, get_text() would fetch the raw file bytes,
        BeautifulSoup would find zero real <a> tags in them, and this would
        always report "no file found" even though the file is right there.
        """
        stripped = page_url.split("?")[0].split("#")[0].lower()
        if any(stripped.endswith(ext) for ext in FILE_EXTENSIONS):
            return page_url

        html = self.http.get_text(page_url)
        soup = BeautifulSoup(html, "html.parser")

        candidates: list[tuple[int, str]] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            absolute = urljoin(page_url, href)
            text = " ".join(anchor.get_text(" ", strip=True).split()).lower()
            haystack = f"{absolute.lower()} {text}"

            if not any(ext in absolute.lower() for ext in FILE_EXTENSIONS):
                continue

            score = 1
            if filename_hint:
                # Match on the stem so 'AIP_TGP_Data.xls' still matches
                # 'AIP_TGP_Data_17-Jul-2026.xls'.
                stem = Path(filename_hint).stem.lower()
                if stem in haystack:
                    score += 10
                # Token overlap catches renamed-but-related files.
                tokens = [t for t in re.split(r"[^a-z0-9]+", stem) if len(t) > 2]
                score += sum(2 for t in tokens if t in haystack)

            for keyword, weight in (("historical", 3), ("monthly", 3), ("data", 2),
                                    ("history", 2), ("series", 1)):
                if keyword in haystack:
                    score += weight

            # Prefer spreadsheets over PDFs: same numbers, far less parsing risk.
            if absolute.lower().endswith((".xlsx", ".xls", ".xlsm", ".csv")):
                score += 4

            candidates.append((score, absolute))

        if not candidates:
            return None
        candidates.sort(reverse=True)
        log.info("Discovered file link (score %d): %s", candidates[0][0], candidates[0][1])
        return candidates[0][1]

    # -------------------------------------------------------------- extractors

    def extract_from_file(self, path: Path, spec: DriverSpec) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix in (".xlsx", ".xls", ".xlsm"):
            return self.extract_from_workbook(path, spec)
        if suffix == ".csv":
            return self._extract_from_csv(path, spec)
        if suffix == ".pdf":
            return self._extract_from_pdf(path, spec)
        raise ValueError(f"Unsupported file type for Tier 2 extraction: {suffix}")

    def extract_from_workbook(self, path: Path, spec: DriverSpec) -> pd.DataFrame:
        """
        Find a named series inside an Excel workbook.

        Handles the pattern common to statistical publications: several banner
        rows, then a header row of series names, then a units row, then data.
        We read with no header, locate the header row by searching for the
        series name, and slice from there.
        """
        sheet_hint = spec.locator_part("sheet")
        series_hint = spec.locator_part("series") or spec.driver_name

        engine = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
        book = pd.read_excel(path, sheet_name=None, header=None, engine=engine)

        # Choose the sheet: registry hint first, otherwise the sheet whose text
        # mentions the series name most often.
        sheet_names = list(book.keys())
        if sheet_hint:
            matches = [s for s in sheet_names if sheet_hint.lower() in str(s).lower()]
            target_sheets = matches or sheet_names
        else:
            target_sheets = sheet_names

        budget = _time_budget(self.cfg)
        scan_start = time.monotonic()
        scanned = 0
        for sheet in target_sheets:
            if time.monotonic() - scan_start > budget:
                log.warning("Workbook sheet scan for %s exceeded %.0fs budget after %d of %d "
                           "sheet(s); stopping deterministic pass", spec.driver_id, budget,
                           scanned, len(target_sheets))
                break
            scanned += 1
            raw = book[sheet]
            located = self._locate_series_in_grid(raw, series_hint)
            if located is not None:
                log.info("[T2] '%s' located on sheet '%s' of %s", series_hint, sheet, path.name)
                return located

        # Deterministic parsing failed. Fall back to the LLM, which returns
        # coordinates only; the values still come from the file itself.
        if self.llm:
            for sheet in target_sheets[:3]:
                if time.monotonic() - scan_start > budget:
                    log.warning("Workbook sheet scan for %s exceeded %.0fs budget during LLM "
                               "fallback; stopping", spec.driver_id, budget)
                    break
                coords = self.llm.locate_series_in_sheet(spec, book[sheet], sheet)
                if coords:
                    return self._slice_by_coords(book[sheet], coords)

        raise ValueError(f"Series '{series_hint}' not found in {path.name}")

    @staticmethod
    def _locate_series_in_grid(grid: pd.DataFrame, series_hint: str) -> Optional[pd.DataFrame]:
        """
        Search an unparsed sheet grid for the series, in either orientation.

        Wide layout (Pink Sheet style): dates run down column 0, series names
        sit in a header row across the top.
        Long layout: a label column repeats the series name per row.
        """
        hint = series_hint.lower().strip()
        if not hint:
            return None

        text_grid = grid.astype(str).apply(lambda col: col.str.lower().str.strip())

        # --- wide layout: find the header cell, take that column ---
        for row_idx in range(min(len(text_grid), 15)):       # header is near the top
            row = text_grid.iloc[row_idx]
            hits = [c for c in range(len(row)) if hint in str(row.iloc[c])]
            if not hits:
                continue
            col_idx = hits[0]
            # Data starts below the header, skipping any units row.
            body = grid.iloc[row_idx + 1:, [0, col_idx]].copy()
            body.columns = ["obs_date", "value"]
            body["obs_date"] = _parse_period_column(body["obs_date"])
            body = body.dropna(subset=["obs_date"])
            if len(body) >= 12:                              # a real history, not a stub
                return body.reset_index(drop=True)

        # --- long layout: label column repeats per row ---
        for col_idx in range(min(text_grid.shape[1], 6)):
            mask = text_grid.iloc[:, col_idx].str.contains(re.escape(hint), na=False)
            if mask.sum() >= 12:
                subset = grid[mask]
                date_col = _first_date_column(subset)
                value_col = _first_numeric_column(subset, exclude={col_idx, date_col})
                if date_col is None or value_col is None:
                    continue
                body = pd.DataFrame({
                    "obs_date": _parse_period_column(subset.iloc[:, date_col]),
                    "value": subset.iloc[:, value_col],
                })
                return body.dropna(subset=["obs_date"]).reset_index(drop=True)

        return None

    @staticmethod
    def _slice_by_coords(grid: pd.DataFrame, coords: dict) -> pd.DataFrame:
        """Read the exact cells the LLM identified. Values come from the file."""
        body = grid.iloc[
            int(coords["first_data_row"]):,
            [int(coords["date_col"]), int(coords["value_col"])],
        ].copy()
        body.columns = ["obs_date", "value"]
        body["obs_date"] = _parse_period_column(body["obs_date"])
        return body.dropna(subset=["obs_date"]).reset_index(drop=True)

    def _extract_from_csv(self, path: Path, spec: DriverSpec) -> pd.DataFrame:
        # Statistical CSVs often carry preamble lines before the real header;
        # try a few skiprows values until the frame looks tabular.
        for skip in (0, 1, 2, 3, 4, 5):
            try:
                df = pd.read_csv(path, skiprows=skip)
            except Exception:  # noqa: BLE001
                continue
            if df.shape[1] >= 2 and len(df) >= 12:
                date_col = _first_date_column(df)
                if date_col is None:
                    continue
                value_col = _first_numeric_column(df, exclude={date_col})
                if value_col is None:
                    continue
                return pd.DataFrame({
                    "obs_date": _parse_period_column(df.iloc[:, date_col]),
                    "value": df.iloc[:, value_col],
                }).dropna(subset=["obs_date"])
        raise ValueError(f"Could not parse a time series out of {path.name}")

    def _extract_from_pdf(self, path: Path, spec: DriverSpec) -> pd.DataFrame:
        """
        PDF extraction: try ruled-table detection first, then LLM on page text.

        Many industry bulletins (IBA Brazil, ministry energy reports) publish
        the only monthly series as a PDF table, so this path matters.
        """
        import pdfplumber  # imported lazily; heavy dependency

        series_hint = (spec.locator_part("series") or spec.driver_name).lower()
        collected: list[pd.DataFrame] = []
        page_texts: list[str] = []

        budget = _time_budget(self.cfg)
        scan_start = time.monotonic()

        with pdfplumber.open(path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                if time.monotonic() - scan_start > budget:
                    log.warning("PDF page scan for %s exceeded %.0fs budget after %d of %d "
                               "page(s); stopping with %d table(s) collected",
                               spec.driver_id, budget, page_num - 1, len(pdf.pages), len(collected))
                    break
                text = page.extract_text() or ""
                if series_hint and series_hint.split()[0] not in text.lower():
                    continue
                page_texts.append(text)
                for table in page.extract_tables() or []:
                    if len(table) < 4:
                        continue
                    df = pd.DataFrame(table[1:], columns=table[0])
                    date_col = _first_date_column(df)
                    if date_col is None:
                        continue
                    value_col = _first_numeric_column(df, exclude={date_col})
                    if value_col is None:
                        continue
                    collected.append(pd.DataFrame({
                        "obs_date": _parse_period_column(df.iloc[:, date_col]),
                        "value": df.iloc[:, value_col],
                    }))

        if collected:
            return pd.concat(collected, ignore_index=True).dropna(subset=["obs_date"])

        if self.llm and page_texts:
            # The LLM must quote the source line for each value it returns, so
            # every number remains traceable back to the PDF text.
            return self.llm.extract_series_from_text(spec, "\n\n".join(page_texts[:8]))

        raise ValueError(f"No extractable table for '{series_hint}' in {path.name}")


# --------------------------------------------------------------------- helpers

def _sniff_suffix(path: Path) -> Optional[str]:
    """
    Identify a file's real type from its magic bytes, since a discovered
    download URL's extension is only a guess (and often absent entirely).

    xlsx/xlsm are themselves zip containers, so a zip signature is checked
    against the archive's internal member names first to tell an Office
    workbook apart from a genuine zip archive of loose CSV/Excel files.
    Returns None when nothing recognisable is found, leaving the caller's
    original URL-based guess in place.
    """
    try:
        head = path.read_bytes()[:8]
    except OSError:
        return None

    if head.startswith(b"%PDF"):
        return ".pdf"

    if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06"):
        import zipfile
        try:
            with zipfile.ZipFile(path) as zf:
                names = zf.namelist()
        except zipfile.BadZipFile:
            return None
        if any(n.startswith("xl/") or n == "[Content_Types].xml" for n in names):
            return ".xlsx"
        return ".zip"

    return None


def _parse_period_column(series: pd.Series) -> pd.Series:
    """
    Parse the many period formats statistical files use.

    Handles ISO dates, '2019M04' (World Bank), 'Apr-19', '2019-04', and Excel
    serial numbers, all of which appear across the sample sources.
    """
    text = series.astype(str).str.strip()

    # World Bank style: 1960M01
    wb = text.str.extract(r"^(\d{4})M(\d{1,2})$")
    if wb[0].notna().mean() > 0.5:
        return pd.to_datetime(
            wb[0] + "-" + wb[1].str.zfill(2) + "-01", errors="coerce"
        )

    parsed = pd.to_datetime(text, errors="coerce", format="mixed", dayfirst=False)
    if parsed.notna().mean() > 0.5:
        return parsed

    # Excel serial dates (days since 1899-12-30) surface as bare integers.
    numeric = pd.to_numeric(text, errors="coerce")
    if numeric.between(20000, 60000).mean() > 0.5:
        return pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")

    return parsed


def _first_date_column(df: pd.DataFrame) -> Optional[int]:
    for idx in range(df.shape[1]):
        if _parse_period_column(df.iloc[:, idx]).notna().mean() > 0.6:
            return idx
    return None


def _first_numeric_column(df: pd.DataFrame, exclude: set) -> Optional[int]:
    best, best_share = None, 0.0
    for idx in range(df.shape[1]):
        if idx in exclude:
            continue
        share = pd.to_numeric(
            df.iloc[:, idx].astype(str).str.replace(r"[,\s]", "", regex=True),
            errors="coerce",
        ).notna().mean()
        if share > best_share:
            best, best_share = idx, share
    return best if best_share > 0.5 else None
