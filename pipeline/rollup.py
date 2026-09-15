"""
Frequency harmonisation: any native frequency -> monthly.

The aggregation method is not a detail, it is a modelling decision, so it lives
in the registry per driver rather than being inferred:

  mean  - prices, indices, exchange rates. A month's economic signal is its
          average level, not its last print.
  sum   - flows: trade volumes, chicks placed, production tonnes. Summing a
          partial month understates it, which is why coverage is checked.
  last  - stocks and inventories: flock size, capacity, storage. A month-end
          reading is the state of the world at month end.
  first - opening balances.
  count - event streams: outbreak notifications. Absence of events is a real
          zero, not a missing value, so months with no rows are filled with 0.
  max / min - shock indicators, e.g. peak spot energy price in the month.

Coverage guard: a month is only accepted when observed points reach
min_coverage_ratio of the expected count for that frequency. Without this, a
month where a source published twice would silently become a monthly average
of two prints and sit alongside 21-print months in the same regression.

Weekly-to-monthly note: a week can straddle a month boundary. We attribute the
whole week to the month containing its END date, which matches how USDA and
AIP report and avoids double counting. Set `split_straddling_weeks=True` to
apportion by day count instead when working with flow variables.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from .models import Frequency

log = logging.getLogger(__name__)

# Aggregation name -> pandas function.
AGG_FUNCS = {
    "mean": "mean",
    "sum": "sum",
    "last": "last",
    "first": "first",
    "count": "count",
    "max": "max",
    "min": "min",
    "median": "median",
}


def _expected_points(frequency: str) -> float:
    return Frequency.EXPECTED_PER_MONTH.get(frequency, 1.0)


_FREQUENCY_GAP_BUCKETS: list[tuple[float, str]] = [
    (5.0, Frequency.DAILY),
    (15.0, Frequency.WEEKLY),
    (55.0, Frequency.MONTHLY),
    (183.0, Frequency.QUARTERLY),
    (float("inf"), Frequency.ANNUAL),
]
MIN_OBSERVATIONS_FOR_FREQUENCY_DETECTION = 4


def _detect_frequency(obs_dates: pd.Series, declared_frequency: str) -> str:
    """
    Infer the true publication cadence from the spacing of observation dates.

    Uses the median gap (in days) between sorted, de-duplicated dates. Falls
    back to `declared_frequency` when there are too few points to be confident.
    """
    dates = pd.to_datetime(pd.Series(obs_dates)).dropna().sort_values().unique()
    if len(dates) < MIN_OBSERVATIONS_FOR_FREQUENCY_DETECTION:
        return declared_frequency

    gaps_days = np.diff(dates).astype("timedelta64[D]").astype(float)
    median_gap = float(np.median(gaps_days))

    for threshold, bucket in _FREQUENCY_GAP_BUCKETS:
        if median_gap <= threshold:
            return bucket
    return Frequency.ANNUAL


def normalise_observations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce a connector's raw output into the canonical two-column shape.

    Connectors return whatever the source gave them; everything downstream
    assumes exactly [obs_date: datetime64, value: float], sorted, de-duplicated.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["obs_date", "value"])

    out = df.copy()
    out["obs_date"] = pd.to_datetime(out["obs_date"], errors="coerce", utc=False)

    # Values arrive as strings with thousands separators, footnote markers,
    # em-dashes for nulls, and occasional parenthesised negatives.
    if out["value"].dtype == object:
        cleaned = (
            out["value"].astype(str)
            .str.replace(r"[,\s]", "", regex=True)
            .str.replace(r"^\((.*)\)$", r"-\1", regex=True)   # (123) -> -123
            .str.replace(r"[^0-9eE\.\-\+]", "", regex=True)   # strip footnote letters
            .replace({"": None, "-": None, ".": None})
        )
        out["value"] = pd.to_numeric(cleaned, errors="coerce")
    else:
        out["value"] = pd.to_numeric(out["value"], errors="coerce")

    out = out.dropna(subset=["obs_date"])
    # Same date twice usually means a revision: keep the later row.
    out = out.drop_duplicates(subset=["obs_date"], keep="last")
    return out.sort_values("obs_date").reset_index(drop=True)[["obs_date", "value"]]


def to_monthly(
    df: pd.DataFrame,
    method: str = "mean",
    frequency: str = "Monthly",
    min_coverage_ratio: float = 0.6,
    max_forward_fill_months: int = 0,
    split_straddling_weeks: bool = False,
) -> tuple[pd.DataFrame, str]:
    """
    Roll a normalised observation frame up to monthly.

    Returns (monthly_frame, detected_frequency) where monthly_frame has columns:
      month              first day of the month (YYYY-MM-01)
      value              aggregated value
      n_obs              observations that fed the aggregate
      coverage_ratio     n_obs / expected for detected frequency (capped at 1.0)
      is_complete        coverage_ratio >= min_coverage_ratio
      is_imputed         True when the row was forward-filled, not observed

    detected_frequency is the cadence inferred from observation date spacing,
    or the declared frequency if there aren't enough points to infer confidently.
    """
    obs = normalise_observations(df)
    if obs.empty:
        return pd.DataFrame(
            columns=["month", "value", "n_obs", "coverage_ratio", "is_complete", "is_imputed"]
        ), frequency

    method = str(method).lower().strip()
    agg = AGG_FUNCS.get(method)
    if agg is None:
        log.warning("Unknown rollup method '%s'; defaulting to mean", method)
        agg = "mean"
        method = "mean"

    work = obs.copy()

    # Detect actual cadence from observation date spacing. Event drivers
    # (count method) always keep Event semantics regardless of spacing.
    if method == "count":
        detected_frequency = Frequency.EVENT
    else:
        detected_frequency = _detect_frequency(obs["obs_date"], frequency)

    # Weekly flows that straddle month ends can be apportioned by day count so
    # that no volume is lost or duplicated. Off by default because most weekly
    # publishers already assign a week to a reporting month.
    if split_straddling_weeks and detected_frequency == Frequency.WEEKLY and method == "sum":
        work = _apportion_weekly(work)

    work["month"] = work["obs_date"].values.astype("datetime64[M]")

    grouped = work.groupby("month", as_index=False).agg(
        value=("value", agg),
        n_obs=("value", "count"),
    )

    expected = _expected_points(detected_frequency)
    if expected > 0:
        grouped["coverage_ratio"] = (grouped["n_obs"] / expected).clip(upper=1.0)
    else:
        # Event data: any month within the observed span is fully covered by
        # definition, because zero events is information, not absence of it.
        grouped["coverage_ratio"] = 1.0

    grouped["is_complete"] = grouped["coverage_ratio"] >= float(min_coverage_ratio)
    grouped["is_imputed"] = False

    # Rebuild a continuous month index so gaps are visible rather than implicit.
    full_index = pd.date_range(grouped["month"].min(), grouped["month"].max(), freq="MS")
    grouped = (
        grouped.set_index("month")
        .reindex(full_index)
        .rename_axis("month")
        .reset_index()
    )
    grouped["n_obs"] = grouped["n_obs"].fillna(0).astype(int)
    grouped["coverage_ratio"] = grouped["coverage_ratio"].fillna(0.0)

    if method == "count":
        # A month with no outbreak reports genuinely had zero outbreaks.
        grouped["value"] = grouped["value"].fillna(0.0)
        grouped["is_complete"] = grouped["is_complete"].fillna(True)
    else:
        grouped["is_complete"] = grouped["is_complete"].fillna(False)

    grouped["is_imputed"] = grouped["is_imputed"].fillna(False)

    # Bounded forward fill for sources that skip the odd month.
    if max_forward_fill_months > 0 and method != "count":
        grouped = _bounded_ffill(grouped, max_forward_fill_months)

    return grouped[["month", "value", "n_obs", "coverage_ratio", "is_complete", "is_imputed"]], detected_frequency


def _apportion_weekly(work: pd.DataFrame) -> pd.DataFrame:
    """
    Split each weekly flow observation across the calendar days it covers, then
    let the normal monthly groupby recombine the daily fragments.
    """
    rows = []
    for _, row in work.iterrows():
        end = row["obs_date"]
        start = end - pd.Timedelta(days=6)
        days = pd.date_range(start, end, freq="D")
        per_day = row["value"] / len(days) if pd.notna(row["value"]) else np.nan
        rows.extend({"obs_date": d, "value": per_day} for d in days)
    return pd.DataFrame(rows)


def _bounded_ffill(df: pd.DataFrame, max_months: int) -> pd.DataFrame:
    """Forward fill gaps of at most `max_months`, flagging every filled row."""
    out = df.copy()
    missing = out["value"].isna()
    if not missing.any():
        return out

    # Identify consecutive runs of missing months and only fill short ones.
    run_id = (missing != missing.shift()).cumsum()
    for _, block in out[missing].groupby(run_id[missing]):
        if len(block) <= max_months and block.index.min() > 0:
            fill_value = out.loc[block.index.min() - 1, "value"]
            if pd.notna(fill_value):
                out.loc[block.index, "value"] = fill_value
                out.loc[block.index, "is_imputed"] = True
                out.loc[block.index, "is_complete"] = False
    return out


def history_years(monthly: pd.DataFrame) -> float:
    """Length of the observed monthly series in years, for the min-history gate."""
    observed = monthly.dropna(subset=["value"])
    if observed.empty:
        return 0.0
    span_months = (
        (observed["month"].max().year - observed["month"].min().year) * 12
        + (observed["month"].max().month - observed["month"].min().month)
        + 1
    )
    return round(span_months / 12.0, 2)
