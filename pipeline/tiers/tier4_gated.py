"""
Tier 4: something stands between the pipeline and the data.

Three kinds of gate, handled differently:

  API KEY, free       The key exists in config, or an automated self-signup is
                      attempted where the provider supports a programmatic
                      registration endpoint. FRED and USDA NASS both do.

  LOGIN, free         Portal registration with email confirmation and often a
                      CAPTCHA. Automating this is fragile and frequently
                      against the site's terms, so the pipeline attempts a
                      simple form-post login when credentials are supplied and
                      otherwise raises a human-in-the-loop task.

  PAID / RESTRICTED   Never attempted. A commercial licence is a contractual
                      matter, not a technical one. The pipeline records the
                      task and moves on.

Also in this module: the manual drop path. Once a human has obtained a file
offline, they place it in input/manual/<driver_id>.<ext> and the next run picks
it up automatically through the Tier 2 extractor. That closes the loop without
anyone editing code.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from ..models import DriverSpec, FetchResult, Outcome, Tier
from .base import TierHandler, manual_drop_path, manual_file
from .tier2_files import Tier2EmbeddedFile

log = logging.getLogger(__name__)

# Providers exposing a genuine programmatic signup. Kept deliberately small:
# scripted signup against a site that does not offer it is both unreliable and
# a terms-of-service problem.
AUTO_SIGNUP_PROVIDERS: dict[str, dict] = {
    # Example shape; populate only where a provider documents the endpoint.
    # "some_provider": {
    #     "endpoint": "https://api.example.org/register",
    #     "payload": {"email": "data@aiqmen.com", "purpose": "research"},
    #     "key_field": "api_key",
    # },
}


class Tier4Gated(TierHandler):
    tier = Tier.GATED

    def can_handle(self, spec: DriverSpec) -> bool:
        return spec.access_mode in ("api_key", "login", "paid_or_restricted") or spec.declared_tier == Tier.GATED

    def fetch(self, spec: DriverSpec) -> FetchResult:
        # 0. A manually supplied file always wins. If a human already fetched
        #    it, do not go back to the network. Redundant with cascade.py's
        #    own pre-tier-0 check (which covers every driver, not just gated
        #    ones) when reached through the normal ladder, but kept here too
        #    since this tier can also be invoked directly (e.g. a revised
        #    spec from reclassification/alternate-source search).
        manual = self._manual_file(spec)
        if manual is not None:
            log.info("[T4] %s satisfied by manual drop %s", spec.driver_id, manual.name)
            extractor = Tier2EmbeddedFile(self.cfg, self.http, self.connectors, self.llm)
            result = self._guard(spec, extractor.extract_from_file, manual, spec)
            result.extra["source"] = "manual_drop"
            result.raw_artifact_path = str(manual)
            return result

        # 1. Paid sources are never scripted around.
        if spec.access_mode == "paid_or_restricted" and not self.cfg.has_credentials(spec.credential_keys):
            return self._result(
                spec, Outcome.BLOCKED_PAID,
                message=("Commercial subscription required. Supply credentials in "
                         f"config.yaml under credentials.{spec.credential_key} or drop the "
                         f"export at {self._manual_path(spec)}.*"),
            )

        # 2. API-key sources: use the configured key, or try a free signup.
        if spec.access_mode == "api_key":
            if not self.cfg.has_credentials(spec.credential_keys):
                key = self._attempt_auto_signup(spec)
                if not key:
                    return self._result(
                        spec, Outcome.BLOCKED_CREDENTIALS,
                        message=(f"Free API key required. Register at {spec.source_url} and set "
                                 f"credentials.{spec.credential_key} in config.yaml."),
                    )
            adapter = self.connectors.get(spec.connector)
            if adapter is None:
                return self._result(spec, Outcome.FAILED_PERMANENT,
                                    message=f"No connector implemented for '{spec.connector}'")
            log.info("[T4] %s via keyed connector '%s'", spec.driver_id, spec.connector)
            return self._guard(spec, adapter, spec, self.http, self.cfg)

        # 3. Login-gated sources.
        if spec.access_mode == "login":
            if not self.cfg.has_credentials(spec.credential_keys):
                return self._result(
                    spec, Outcome.BLOCKED_CREDENTIALS,
                    message=(f"Portal login required. Register free at {spec.source_url}, then set "
                             f"{' and '.join(spec.credential_keys)} in config.yaml."),
                )
            return self._guard(spec, self._fetch_with_login, spec)

        return self._result(spec, Outcome.FAILED_PERMANENT,
                            message=f"Unhandled Tier 4 access mode '{spec.access_mode}'")

    # ---------------------------------------------------------------- helpers

    def _manual_path(self, spec: DriverSpec) -> Path:
        return manual_drop_path(self.cfg, spec)

    def _manual_file(self, spec: DriverSpec) -> Optional[Path]:
        return manual_file(self.cfg, spec)

    def _attempt_auto_signup(self, spec: DriverSpec) -> Optional[str]:
        """
        Try to obtain a free API key programmatically.

        Only providers with a documented registration endpoint are attempted.
        A successful key is cached in memory for this run; the operator is told
        to persist it in config.yaml so subsequent runs skip this step.
        """
        if not self.cfg.get("cascade.allow_auto_signup", True):
            return None

        recipe = AUTO_SIGNUP_PROVIDERS.get(spec.connector)
        if not recipe:
            log.info("[T4] No automated signup path for '%s'; will raise a HITL task",
                     spec.connector)
            return None

        try:
            response = self.http.request(recipe["endpoint"], method="POST",
                                         json_body=recipe["payload"])
            key = response.json().get(recipe["key_field"])
        except Exception as exc:  # noqa: BLE001
            log.warning("[T4] Auto-signup failed for %s: %s", spec.connector, exc)
            return None

        if key:
            # Inject for the remainder of this run only.
            self.cfg._data.setdefault("credentials", {})[spec.credential_keys[0]] = key  # noqa: SLF001
            log.warning("[T4] Obtained a free key for %s. Persist it in config.yaml under "
                        "credentials.%s so future runs skip signup.",
                        spec.connector, spec.credential_keys[0])
        return key

    def _fetch_with_login(self, spec: DriverSpec) -> pd.DataFrame:
        """
        Session-cookie login, then hand off to the source's connector.

        Deliberately simple: a form POST of username and password against the
        page's login form. Portals with CAPTCHA, MFA or JS-driven auth are not
        automated; those raise and become a human-in-the-loop task, which is
        the correct outcome rather than a brittle browser-driving hack.
        """
        from bs4 import BeautifulSoup

        username = self.cfg.credential(spec.credential_keys[0])
        password = self.cfg.credential(spec.credential_keys[1]) if len(spec.credential_keys) > 1 else None

        login_page = self.http.get_text(spec.source_url)
        soup = BeautifulSoup(login_page, "html.parser")
        form = soup.find("form")
        if form is None:
            raise PermissionError(
                f"No login form found at {spec.source_url}. This portal likely needs "
                "interactive authentication; complete it offline and drop the export at "
                f"{self._manual_path(spec)}.*"
            )

        # Carry hidden fields (CSRF tokens and similar) through unchanged.
        payload = {
            inp.get("name"): inp.get("value", "")
            for inp in form.find_all("input")
            if inp.get("name")
        }
        for field, value in (("username", username), ("email", username), ("password", password)):
            for key in list(payload):
                if field in key.lower() and value:
                    payload[key] = value

        from urllib.parse import urljoin
        action = urljoin(spec.source_url, form.get("action") or spec.source_url)
        self.http.request(action, method="POST", data=payload)

        adapter = self.connectors.get(spec.connector)
        if adapter is None:
            raise NotImplementedError(
                f"Authenticated session established but no connector for '{spec.connector}'"
            )
        result = adapter(spec, self.http, self.cfg)
        return result.observations if hasattr(result, "observations") else result
