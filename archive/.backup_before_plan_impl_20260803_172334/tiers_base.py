"""
Tier handler contract.

Each tier is a strategy for getting at data of a given difficulty. The cascade
orchestrator does not know or care how a tier works; it only asks
`can_handle(spec)` and then `fetch(spec)`, and reads the returned FetchResult.

This is what makes the pipeline scalable: adding a new source means adding a
connector function, not editing the control flow.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd

from ..models import DriverSpec, FetchResult, Outcome, Tier

log = logging.getLogger(__name__)


class TierHandler(ABC):
    """Base class for all four tier strategies."""

    tier: Tier

    def __init__(self, cfg, http, connectors, llm=None):
        self.cfg = cfg
        self.http = http
        self.connectors = connectors   # name -> callable adapter
        self.llm = llm                 # optional LLM helper, may be None

    @abstractmethod
    def can_handle(self, spec: DriverSpec) -> bool:
        """Whether this tier has any realistic chance for the given driver."""

    @abstractmethod
    def fetch(self, spec: DriverSpec) -> FetchResult:
        """Attempt extraction. Must never raise; always return a FetchResult."""

    # ------------------------------------------------------------- utilities

    def _result(
        self,
        spec: DriverSpec,
        outcome: str,
        observations: Optional[pd.DataFrame] = None,
        message: str = "",
        url: str = "",
        artifact: Optional[str] = None,
        **extra,
    ) -> FetchResult:
        return FetchResult(
            driver_id=spec.driver_id,
            outcome=outcome,
            tier_attempted=self.tier,
            observations=observations if observations is not None else pd.DataFrame(
                columns=["obs_date", "value"]
            ),
            source_url_used=url or spec.source_url,
            unit=spec.unit,
            message=message,
            raw_artifact_path=artifact,
            extra=extra,
        )

    def _guard(self, spec: DriverSpec, func, *args, **kwargs) -> FetchResult:
        """
        Run an extraction callable and translate any exception into a
        FetchResult. Tier handlers must not propagate exceptions, because one
        broken source should never take down a 40-commodity run.
        """
        try:
            observations = func(*args, **kwargs)
            if observations is None or len(observations) == 0:
                return self._result(
                    spec, Outcome.FAILED_PERMANENT,
                    message=f"{self.tier.label}: source reachable but returned no rows",
                )
            return self._result(
                spec, Outcome.SUCCESS, observations=observations,
                message=f"{self.tier.label}: {len(observations)} raw observations",
            )
        except NotImplementedError as exc:
            return self._result(spec, Outcome.FAILED_PERMANENT, message=f"No adapter: {exc}")
        except PermissionError as exc:
            # Connectors raise PermissionError when credentials are the blocker.
            return self._result(spec, Outcome.BLOCKED_CREDENTIALS, message=str(exc))
        except Exception as exc:  # noqa: BLE001 - deliberate catch-all boundary
            log.exception("Tier %s extraction failed for %s", int(self.tier), spec.driver_id)
            retryable = isinstance(exc, (TimeoutError, ConnectionError))
            return self._result(
                spec,
                Outcome.FAILED_RETRYABLE if retryable else Outcome.FAILED_PERMANENT,
                message=f"{type(exc).__name__}: {exc}",
            )


def frame(dates, values) -> pd.DataFrame:
    """Small helper so connectors can return the canonical shape in one line."""
    return pd.DataFrame({"obs_date": list(dates), "value": list(values)})
