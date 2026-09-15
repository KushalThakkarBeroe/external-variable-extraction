"""
Domain objects shared by every stage of the pipeline.

Keeping these in one place means a tier handler, the rollup engine and the
storage layer all agree on what a "driver" and an "observation" are, so a new
connector can be added without touching the orchestrator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import IntEnum
from typing import Any, Optional

import pandas as pd


def utc_now() -> datetime:
    """Timezone-aware UTC timestamp. Used everywhere a run is stamped."""
    return datetime.now(timezone.utc)


class Tier(IntEnum):
    """Data availability tiers, ordered by extraction difficulty."""

    DIRECT = 1        # value sits on a URL: JSON/CSV API or an HTML table
    EMBEDDED_FILE = 2 # value sits inside an xlsx/csv/pdf linked from a page
    MULTIPAGE = 3     # value is spread across many pages, needs crawl logic
    GATED = 4         # login, API key, or paid subscription stands in the way

    @property
    def label(self) -> str:
        return {
            1: "Tier 1 - direct URL/API",
            2: "Tier 2 - embedded file",
            3: "Tier 3 - multi-page",
            4: "Tier 4 - gated",
        }[int(self)]


class Frequency(str):
    """Native publication frequency, as written in the registry."""

    DAILY = "Daily"
    WEEKLY = "Weekly"
    MONTHLY = "Monthly"
    QUARTERLY = "Quarterly"
    ANNUAL = "Annual"
    EVENT = "Event"      # irregular occurrences, e.g. disease outbreak reports

    # Expected observations per month. Used by the rollup engine to decide
    # whether a month is complete enough to trust.
    EXPECTED_PER_MONTH = {
        "Daily": 21.0,      # business days, not calendar days
        "Weekly": 4.3,
        "Monthly": 1.0,
        "Quarterly": 0.34,
        "Annual": 0.084,
        "Event": 0.0,       # no expectation: zero events is a valid observation
    }


class Outcome(str):
    """Result of an extraction attempt."""

    SUCCESS = "success"
    FAILED_RETRYABLE = "failed_retryable"   # timeout, 5xx, transient
    FAILED_PERMANENT = "failed_permanent"   # 404, schema gone, parse impossible
    BLOCKED_CREDENTIALS = "blocked_credentials"  # needs a key/login we lack
    BLOCKED_PAID = "blocked_paid"    # commercial licence required
    SKIPPED = "skipped"


@dataclass
class DriverSpec:
    """
    One row of the driver registry: a commodity x region x driver combination
    plus everything needed to go and fetch it.

    The registry is deliberately the only place source knowledge lives. Adding
    the 40-commodity full list means adding rows, not writing code.
    """

    driver_id: str
    commodity: str
    driver_name: str
    region: str
    declared_tier: Tier
    connector: str                    # key into the connector adapter table
    source_name: str
    source_url: str
    endpoint_or_locator: str          # API path, sheet name, series name, filters
    access_mode: str                  # open_api | html_table | file_download | ...
    native_frequency: str
    rollup_method: str                # mean | sum | last | first | count | max | min
    unit: str

    # Provenance and quality metadata, carried through to the final panel so a
    # modeller can see at a glance which regressors are proxies.
    tier_confidence: str = "Medium"
    history_from: Optional[str] = None
    meets_min_history: bool = True
    update_lag_days: int = 30
    is_proxy: bool = False
    proxy_note: str = ""
    human_in_loop: bool = False
    credential_key: Optional[str] = None   # "a|b" for multi-part credentials
    fallback_driver_id: Optional[str] = None
    data_quality_risk: str = "Medium"
    transform_hint: str = ""
    priority: int = 2
    # Per-driver override of run.min_history_years for the quality gate.
    # None (blank in the registry) means "use the global default" — every
    # row that doesn't set this behaves exactly as before.
    min_history_years: Optional[float] = None

    @property
    def series_key(self) -> str:
        """Stable identifier for the output panel."""
        return f"{self.commodity}|{self.region}|{self.driver_id}"

    @property
    def credential_keys(self) -> list[str]:
        if not self.credential_key:
            return []
        return [k.strip() for k in str(self.credential_key).split("|") if k.strip()]

    def locator_part(self, prefix: str) -> Optional[str]:
        """
        Pull a named fragment out of the pipe-delimited locator string.

        'file:CMO.xlsx | sheet:Monthly Prices | series:Maize'
          .locator_part('sheet') -> 'Monthly Prices'
        """
        for chunk in str(self.endpoint_or_locator).split("|"):
            chunk = chunk.strip()
            if chunk.lower().startswith(f"{prefix.lower()}:"):
                return chunk.split(":", 1)[1].strip()
        return None


@dataclass
class FetchResult:
    """
    What a tier handler hands back. Carries the data plus enough context for
    the orchestrator to decide whether to accept it or escalate a tier.
    """

    driver_id: str
    outcome: str
    tier_attempted: Tier
    # Long-format observations: columns [obs_date, value]. Empty on failure.
    observations: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=["obs_date", "value"]))
    source_url_used: str = ""
    unit: str = ""
    message: str = ""
    raw_artifact_path: Optional[str] = None   # cached file, for audit
    retrieved_at: datetime = field(default_factory=utc_now)
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.outcome == Outcome.SUCCESS and not self.observations.empty

    @property
    def should_escalate(self) -> bool:
        """A permanent failure or a credential block is worth trying another tier."""
        return self.outcome in (
            Outcome.FAILED_PERMANENT,
            Outcome.FAILED_RETRYABLE,
            Outcome.BLOCKED_CREDENTIALS,
        )

    def coverage(self) -> tuple[Optional[date], Optional[date], int]:
        if self.observations.empty:
            return None, None, 0
        dates = pd.to_datetime(self.observations["obs_date"])
        return dates.min().date(), dates.max().date(), len(self.observations)


@dataclass
class HitlTask:
    """
    A task parked for a human. Written to a YAML file the user can act on and
    then feed back through config, closing the loop without code changes.
    """

    driver_id: str
    commodity: str
    region: str
    driver_name: str
    reason: str
    source_url: str
    required_action: str
    credential_keys: list[str] = field(default_factory=list)
    suggested_drop_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "driver_id": self.driver_id,
            "commodity": self.commodity,
            "region": self.region,
            "driver": self.driver_name,
            "reason": self.reason,
            "source_url": self.source_url,
            "required_action": self.required_action,
            "credential_keys_to_fill_in_config": self.credential_keys,
            "or_drop_file_here": self.suggested_drop_path,
        }
