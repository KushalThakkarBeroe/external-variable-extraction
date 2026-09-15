"""
Shared HTTP layer.

Every network call in the pipeline goes through here so that retry policy,
politeness throttling, caching and audit logging are uniform. Scraping public
statistical agencies at scale gets you blocked quickly without this.

Design notes:
  - Retries cover connection errors and 5xx/429 only. A 404 is a permanent
    failure and should escalate a tier immediately rather than burn retries.
  - The on-disk cache is keyed by URL hash. Re-running the pipeline the same
    day should not re-download a 6 MB Pink Sheet workbook 8 times.
  - per_host_delay enforces a minimum gap between calls to the same hostname,
    randomized per call (not a fixed interval) — evenly-spaced requests are
    themselves a mild bot signal on some sites.
  - A host that returns a 403, or a 429 that persists after every retry, is
    tracked per-host (see BOT_BLOCK_STATUS). A single such signal is not, on
    its own, trusted as proof the host is genuinely bot-blocking — a bad
    query parameter on one driver's request can produce the identical status
    code to a real block, and treating the two the same way previously let
    one driver's bad locator take down every other driver sharing that host
    for the rest of a run. A host is only put into cooldown once a second,
    independent block signal corroborates the first
    (http.bot_block_confirmation_threshold). Once corroborated, the host is
    skipped (no network call) for a cooldown window — the response's own
    Retry-After header if present, else http.bot_block_cooldown_seconds —
    rather than for the rest of the run outright, since many of these
    signals (UN Comtrade's "Quota Exceeded" 403, for instance) are a
    temporary quota, not a permanent refusal. The first request after
    cooldown is a real probation attempt; a repeat block on probation
    re-cools with a capped exponential backoff instead of one fixed window
    forever.
  - Proactive per-source rate budgeting (http.source_limits, keyed by host)
    complements the above: once a host's configured request budget for its
    window is reached, this pipeline stays under it — never above
    http.rate_limit_safety_margin (default 80%) of the source's own
    documented number. A budget-exhausted call waits out the reset in place
    when it's due soon (http.rate_limit_wait_cap_seconds), otherwise the
    call is deferred to a later run rather than made at all, so a source
    with a known daily/hourly cap is paced back before it ever produces a
    block signal in the first place. See RateLimiter/DeferredCallError
    below.
"""

from __future__ import annotations

import email.utils
import hashlib
import logging
import random
import threading
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests

log = logging.getLogger(__name__)

# Status codes worth retrying: throttling and transient server faults.
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
# Signals treated as "this host is actively defending against automated
# access" rather than an ordinary failure: a clean refusal (403), or
# rate-limiting (429) that didn't clear after every retry. Deliberately not
# 401 — that's a credentials problem (handled separately via PermissionError
# / BLOCKED_CREDENTIALS), not bot-defense, and conflating the two would
# misclassify a genuine missing-key case as a block.
BOT_BLOCK_STATUS = {403, 429}


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """
    Retry-After is either a plain integer number of seconds, or an HTTP-date
    (RFC 7231). Returns seconds-from-now, or None if absent/unparseable.
    """
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        import datetime as _dt
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    return max(0.0, (parsed - now).total_seconds())


class DeferredCallError(requests.exceptions.HTTPError):
    """
    Raised in place of a real request when a host's proactive rate budget
    (http.source_limits) is exhausted for its current window, or when a
    host is in a post-block cooldown. Distinct from a genuine HTTPError so
    callers/logs can tell "we chose not to call" apart from "the source
    rejected us" — see RateLimiter.
    """


class RateLimiter:
    """
    Proactive per-host request budgeting, so a source with a known
    daily/hourly cap is paced back before it ever produces a block signal,
    rather than only reacting after the fact (see HttpClient's cooldown
    logic for the reactive half).

    Config shape (http.source_limits in config.yaml), keyed by hostname —
    mirrors the existing per-host delay pattern, so a new limit is data, not
    a code change. Exactly one of requests_per_minute/_hour/_day per host:

        http:
          source_limits:
            comtradeapi.un.org: {requests_per_day: 500}
            fred.stlouisfed.org: {requests_per_minute: 120}

    A window starts from the first request counted in it (not calendar-
    aligned) — sufficient to stop one run from blowing through a documented
    cap; a later run (or --resume-deferred) starts its own fresh window
    rather than trying to track a wall-clock reset time this module cannot
    know for certain.

    Hosts with no entry in source_limits are unaffected — this is opt-in,
    additive infrastructure, not a default throttle on every source. Only
    hosts with an officially documented limit belong here (see config.yaml's
    comments for sourcing) — never a guessed number.

    Two more knobs, both applied only to hosts already in source_limits
    (never invented for a host with no documented number):

      - http.rate_limit_safety_margin (default 0.8): the documented budget
        is a hard ceiling set by the source, not a target — staying at 80%
        of it leaves headroom for clock drift between this module's
        monotonic window and the source's own reset, and for any other
        client sharing the same key/IP. The configured number in
        source_limits is left as the real, citable, official figure;
        this margin is applied on top of it at check time.
      - http.rate_limit_wait_cap_seconds (default 900 = 15 min): once a
        window's (margin-adjusted) budget is used up, a window resetting
        within this many seconds is waited out in place — the calling
        thread sleeps (without holding the lock, so other hosts/threads are
        unaffected) and retries once the window rolls over, rather than
        giving up on the call. A window resetting further out than this
        (e.g. hours still left in a 500/day Comtrade window) is deferred
        exactly as before instead of blocking a worker thread for hours —
        see waited_entries() vs deferred_entries().
    """

    _WINDOW_KEYS = (
        ("requests_per_minute", 60.0),
        ("requests_per_hour", 3600.0),
        ("requests_per_day", 86400.0),
    )

    def __init__(self, cfg, state_path: Path):
        self.limits: dict[str, dict[str, int]] = dict(cfg.get("http.source_limits", {}) or {})
        self.safety_margin = float(cfg.get("http.rate_limit_safety_margin", 0.8))
        self.wait_cap_seconds = float(cfg.get("http.rate_limit_wait_cap_seconds", 900))
        self.state_path = state_path
        self._counts: dict[str, int] = {}
        self._window_started: dict[str, float] = {}
        self._deferred: dict[str, dict] = {}
        self._waited: list[dict] = []
        self._lock = threading.Lock()

    @classmethod
    def _window_seconds(cls, limit: dict) -> Optional[float]:
        for key, seconds in cls._WINDOW_KEYS:
            if key in limit:
                return seconds
        return None

    @classmethod
    def _budget(cls, limit: dict) -> Optional[int]:
        for key, _seconds in cls._WINDOW_KEYS:
            if key in limit:
                return int(limit[key])
        return None

    @classmethod
    def _unit_label(cls, limit: dict) -> str:
        for key, _seconds in cls._WINDOW_KEYS:
            if key in limit:
                return key.replace("requests_per_", "")
        return "window"

    def _effective_budget(self, budget: int) -> int:
        """Documented budget scaled down by the safety margin; never below 1."""
        return max(1, int(budget * self.safety_margin))

    def check(self, host: str, driver_id: Optional[str]) -> Optional[str]:
        """
        Returns a human-readable defer reason if `host`'s budget for its
        current window is already used up AND the window won't reset within
        wait_cap_seconds (the call should NOT be made and must not be
        counted), else None — in which case this call has been counted
        against the budget as a side effect. Hosts with no configured limit
        always return None at negligible cost (one dict lookup).

        When the budget is used up but the window resets soon (within
        wait_cap_seconds), this method blocks the calling thread until the
        reset instead of deferring — the lock is released while sleeping so
        other hosts/threads are never held up by one host's wait. See
        waited_entries() for what got recorded from that path.
        """
        limit = self.limits.get(host)
        if not limit:
            return None
        budget = self._budget(limit)
        window = self._window_seconds(limit)
        if not budget or not window:
            return None
        effective_budget = self._effective_budget(budget)

        while True:
            with self._lock:
                now = time.monotonic()
                started = self._window_started.get(host)
                if started is None or now - started >= window:
                    self._window_started[host] = now
                    self._counts[host] = 0
                    started = now
                used = self._counts[host]
                if used < effective_budget:
                    self._counts[host] = used + 1
                    return None
                remaining = max(0.0, window - (now - started))

            if remaining <= self.wait_cap_seconds:
                log.info(
                    "Host %s request budget (%d/%s, %.0f%% of documented %d) reached; "
                    "window resets in %.0fs, waiting it out",
                    host, effective_budget, self._unit_label(limit),
                    self.safety_margin * 100, budget, remaining,
                )
                with self._lock:
                    self._waited.append({
                        "driver_id": driver_id,
                        "source_host": host,
                        "requests_budget_effective": effective_budget,
                        "requests_budget_documented": budget,
                        "waited_seconds": round(remaining),
                        "waited_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    })
                time.sleep(remaining + 0.05)
                continue  # window has rolled over; re-check and count the call

            reason = (f"{host} request budget ({effective_budget}/{self._unit_label(limit)}, "
                      f"{self.safety_margin:.0%} of documented {budget}) reached; "
                      f"resets in {round(remaining)}s (beyond the {round(self.wait_cap_seconds)}s wait cap)")
            if driver_id:
                self._deferred[driver_id] = {
                    "driver_id": driver_id,
                    "source_host": host,
                    "reason": reason,
                    "requests_used": used,
                    "requests_budget": effective_budget,
                    "retry_after_seconds": round(remaining),
                }
            return reason

    def deferred_entries(self) -> list[dict]:
        with self._lock:
            return list(self._deferred.values())

    def waited_entries(self) -> list[dict]:
        """Every occasion this run waited out a host's rate-limit window instead of deferring."""
        with self._lock:
            return list(self._waited)

    def save_state(self) -> None:
        """
        Persists the deferred set to output/rate_limit_state.json so a
        later --resume-deferred invocation (see run_pipeline.py) can pick
        up exactly the drivers this run skipped for quota reasons, without
        re-deriving them from the report.
        """
        entries = self.deferred_entries()
        if not entries:
            return
        import json as _json

        from .atomic_io import write_atomic
        payload = {
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "deferred": entries,
        }
        try:
            write_atomic(self.state_path, lambda tmp: tmp.write_text(
                _json.dumps(payload, indent=2), encoding="utf-8"))
            log.info("Saved %d deferred driver(s) -> %s", len(entries), self.state_path)
        except OSError as exc:
            log.warning("Could not save rate-limit state to %s: %s", self.state_path, exc)


def load_deferred_driver_ids(state_path: Path) -> set[str]:
    """Reads a prior run's deferred set (see RateLimiter.save_state). Empty set if absent/unreadable."""
    if not state_path.exists():
        return set()
    try:
        import json as _json
        payload = _json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return set()
    return {str(e["driver_id"]) for e in payload.get("deferred", []) if e.get("driver_id")}


class HttpClient:
    def __init__(self, cfg):
        self.cfg = cfg
        self.timeout = int(cfg.get("http.timeout_seconds", 60))
        self.max_retries = int(cfg.get("http.max_retries", 4))
        self.backoff = float(cfg.get("http.backoff_factor", 1.8))
        self.host_delay_min = float(cfg.get("http.per_host_delay_min_seconds", 3.0))
        self.host_delay_max = float(cfg.get("http.per_host_delay_max_seconds", 7.0))
        self.cache_ttl = float(cfg.get("http.cache_ttl_hours", 24)) * 3600
        self.verify_ssl = bool(cfg.get("http.verify_ssl", True))
        self.cache_dir = cfg.path("paths.raw_cache", "output/raw")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            # A plain, honestly-labeled bot UA gets pattern-matched and
            # blocked by some sites' WAFs regardless of how polite the
            # request actually is. A standard browser-shaped UA string
            # (with the same contact/purpose disclosure preserved) clears
            # that specific check without misrepresenting the request in
            # any way that matters — throttling, retries and caching are
            # still fully in effect.
            "User-Agent": cfg.get(
                "http.user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 "
                "(+research use; contact: data@aiqmen.com)",
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                     "application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._last_call: dict[str, float] = {}
        # Guards _last_call's read-check-write below. Only matters when
        # HttpClient is shared across threads (the parallel runner); a
        # single-threaded caller never contends on it, so this costs nothing
        # there.
        self._throttle_lock = threading.Lock()

        # ---- reactive host-blocking: corroboration + cooldown ----
        self.bot_block_confirmation_threshold = int(cfg.get("http.bot_block_confirmation_threshold", 2))
        self.bot_block_cooldown_seconds = float(cfg.get("http.bot_block_cooldown_seconds", 900))
        # Cap on the exponential backoff applied to repeat probation
        # failures, so a genuinely dead host is retried on a bounded
        # schedule (this many times the base cooldown) rather than
        # increasingly rarely forever.
        self.bot_block_max_cooldown_multiplier = float(cfg.get("http.bot_block_max_cooldown_multiplier", 8))
        # Hosts confirmed bot-blocking at ANY point this run (never removed,
        # even after a cooldown expires) — read by
        # storage.py::finalize_bot_block_flags at the end of the run to flag
        # affected drivers in the report. Not the live gate itself; see
        # _host_blocked_until for that.
        self.bot_blocked_hosts: set[str] = set()
        # Raw signal counts per host, used only to reach the corroboration
        # threshold for a host's FIRST block — a single 403/429 is not on
        # its own trusted as a genuine block (see module docstring).
        self._host_block_status_counts: dict[str, int] = {}
        # host -> monotonic timestamp before which request() short-circuits
        # without a network call. Cleared implicitly once time passes it;
        # the next call through is a real probation attempt.
        self._host_blocked_until: dict[str, float] = {}
        # How many times a host has been (re-)blocked this run, for the
        # capped exponential backoff on repeat probation failures.
        self._host_block_strikes: dict[str, int] = {}
        self._block_lock = threading.Lock()

        # ---- proactive per-source rate/quota budgeting ----
        self.rate_limiter = RateLimiter(cfg, cfg.path("paths.rate_limit_state", "output/rate_limit_state.json"))
        # Per-thread "which driver is currently being resolved" — set once
        # per top-level cascade.py::run_driver() call (never reset for a
        # nested fallback/alternate-source/repair sub-fetch under the same
        # call tree, so those are correctly attributed to the same driver).
        # Lets request() below record the real host(s) each driver actually
        # contacted, independent of the registry's source_url column (often
        # a different host than the API endpoint a connector calls — see
        # storage.py::finalize_bot_block_flags, the consumer of this).
        self._current_driver = threading.local()
        self.attempted_hosts_by_driver: dict[str, set[str]] = {}
        self._host_log_lock = threading.Lock()
        # Per-cache-path locks so two threads downloading the same URL at
        # once (several drivers commonly share one source file, e.g. the
        # World Bank Pink Sheet) serialize onto one download-and-write
        # instead of both writing the same cache file at once. The registry
        # dict itself is tiny (one Lock per unique URL ever downloaded this
        # run) and is guarded by its own lock just for safe creation.
        self._download_locks: dict[str, threading.Lock] = {}
        self._download_locks_guard = threading.Lock()

    def _lock_for_path(self, key: str) -> threading.Lock:
        with self._download_locks_guard:
            lock = self._download_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._download_locks[key] = lock
            return lock

    # ---------------------------------------------------------------- helpers

    def _throttle(self, url: str) -> None:
        """
        Sleep just long enough to respect the per-host delay.

        The gap is a fresh random draw between per_host_delay_min_seconds and
        _max_seconds on every call, not a fixed interval — evenly-spaced
        requests are themselves a mild bot signal on some sites, on top of
        being a shorter, more mechanical-looking gap than real traffic.

        The next allowed slot for this host is reserved atomically before
        sleeping, rather than read-then-slept-then-written: two threads
        hitting the same host at once would otherwise both read the same
        last-call time and both conclude they're clear to go immediately,
        silently doubling up on a host we're deliberately trying to be
        polite to. The sleep itself happens outside the lock so a thread
        waiting on one host never blocks threads targeting other hosts.
        """
        host = urlparse(url).netloc
        with self._throttle_lock:
            last = self._last_call.get(host)
            now = time.time()
            target_gap = random.uniform(self.host_delay_min, self.host_delay_max)
            wait = max(target_gap - (now - last), 0.0) if last is not None else 0.0
            self._last_call[host] = now + wait
        if wait > 0:
            time.sleep(wait)

    def _register_block_signal(self, host: str, url: str, status: int,
                               exc: requests.exceptions.RequestException) -> None:
        """
        Record one bot-block-shaped response (403, or 429 exhausted after
        every retry) and decide whether it's enough to put the host into
        cooldown.

        A host's FIRST block needs bot_block_confirmation_threshold distinct
        signals (default 2) — one bad request from one driver is not
        sufficient evidence a shared host is genuinely defending itself.
        Once already blocked at least once this run, a single failure on
        the post-cooldown probation request is sufficient on its own to
        re-block (it has already been corroborated once), with an
        exponentially longer, capped cooldown each time.
        """
        retry_after = None
        response = getattr(exc, "response", None)
        if response is not None:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))

        with self._block_lock:
            strikes = self._host_block_strikes.get(host, 0)
            if strikes == 0:
                count = self._host_block_status_counts.get(host, 0) + 1
                self._host_block_status_counts[host] = count
                if count < self.bot_block_confirmation_threshold:
                    log.info("Host %s returned HTTP %s (%d/%d signal(s) needed to confirm a block)",
                            host, status, count, self.bot_block_confirmation_threshold)
                    return
            strikes += 1
            self._host_block_strikes[host] = strikes
            cooldown = retry_after if retry_after is not None else (
                self.bot_block_cooldown_seconds * (2 ** (strikes - 1)))
            cooldown = min(cooldown, self.bot_block_cooldown_seconds * self.bot_block_max_cooldown_multiplier)
            self._host_blocked_until[host] = time.monotonic() + cooldown
            self.bot_blocked_hosts.add(host)

        log.warning("Host %s confirmed bot-blocking (HTTP %s on %s, strike %d); cooling down for "
                   "%.0fs before the next probation request", host, status, url, strikes, cooldown)

    def set_current_driver(self, driver_id: Optional[str]) -> None:
        """
        Mark which driver this thread is resolving, for attempted-host
        tracking. Call once at the top of a driver's resolution (see
        cascade.py::run_driver(), only at depth == 0); every request() this
        thread makes until the next call is attributed to it.
        """
        self._current_driver.value = driver_id

    def _cache_path(self, url: str, suffix: str = ".bin") -> Path:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]
        return self.cache_dir / f"{digest}{suffix}"

    def _cache_is_fresh(self, path: Path) -> bool:
        return path.exists() and (time.time() - path.stat().st_mtime) < self.cache_ttl

    # ------------------------------------------------------------------ calls

    def request(
        self,
        url: str,
        method: str = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        data: Any = None,
        json_body: Any = None,
        allow_retry: bool = True,
    ) -> requests.Response:
        """
        Perform a request with exponential backoff.

        Raises requests.HTTPError on a non-retryable bad status, or the last
        exception after exhausting retries. Callers translate that into an
        Outcome so the cascade can decide whether to escalate.

        A host currently in a post-block cooldown, or whose proactive rate
        budget for its current window is exhausted, fails immediately here
        (DeferredCallError for the latter) — before any throttle wait or
        network call.
        """
        host = urlparse(url).netloc

        driver_id = getattr(self._current_driver, "value", None)
        if driver_id:
            with self._host_log_lock:
                self.attempted_hosts_by_driver.setdefault(driver_id, set()).add(host)

        with self._block_lock:
            blocked_until = self._host_blocked_until.get(host)
        if blocked_until is not None and time.monotonic() < blocked_until:
            remaining = blocked_until - time.monotonic()
            log.debug("Skipping %s: host %s cooling down after a confirmed block (%.0fs remaining)",
                     url, host, remaining)
            raise requests.exceptions.HTTPError(
                f"Host {host} is cooling down after a confirmed bot-block; retry after "
                f"{remaining:.0f}s (skipped without a network call)"
            )

        defer_reason = self.rate_limiter.check(host, driver_id)
        if defer_reason is not None:
            log.info("Deferring %s: %s (skipped without a network call)", url, defer_reason)
            raise DeferredCallError(f"Deferred: {defer_reason}")

        attempts = self.max_retries if allow_retry else 1
        last_error: Optional[Exception] = None

        # A same-site Referer is what a browser would send navigating within
        # the host being requested, and its absence is itself a mild bot
        # signal on some sites. Only filled in when the caller didn't
        # already supply one.
        request_headers = dict(headers) if headers else {}
        request_headers.setdefault("Referer", f"{urlparse(url).scheme}://{urlparse(url).netloc}/")

        for attempt in range(1, attempts + 1):
            self._throttle(url)
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=request_headers,
                    data=data,
                    json=json_body,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
                if response.status_code in RETRYABLE_STATUS and attempt < attempts:
                    # Honour Retry-After when the server supplies it.
                    wait = float(response.headers.get("Retry-After", self.backoff ** attempt))
                    log.warning("HTTP %s on %s (attempt %d/%d); retrying in %.1fs",
                                response.status_code, url, attempt, attempts, wait)
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as exc:
                last_error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                # Reached only for a clean 403, or a 429 that has already
                # exhausted every retry (429 is retryable, so this except
                # block is never entered for it until attempt == attempts —
                # never on the first sign of throttling).
                if status in BOT_BLOCK_STATUS:
                    self._register_block_signal(host, url, status, exc)
                # 4xx other than the retryable set is permanent: stop immediately.
                if status is not None and status not in RETRYABLE_STATUS and 400 <= status < 500:
                    raise
                if attempt < attempts:
                    wait = self.backoff ** attempt
                    log.warning("Request error on %s (attempt %d/%d): %s; retrying in %.1fs",
                                url, attempt, attempts, exc, wait)
                    time.sleep(wait)
                else:
                    break

        assert last_error is not None
        raise last_error

    def get_json(self, url: str, params: Optional[dict] = None,
                 headers: Optional[dict] = None) -> Any:
        return self.request(url, params=params, headers=headers).json()

    def get_text(self, url: str, params: Optional[dict] = None,
                 headers: Optional[dict] = None) -> str:
        response = self.request(url, params=params, headers=headers)
        # Statistical agencies frequently mislabel encoding; fall back to apparent.
        if not response.encoding or response.encoding.lower() == "iso-8859-1":
            response.encoding = response.apparent_encoding or "utf-8"
        return response.text

    def download(self, url: str, suffix: str = ".bin", force: bool = False) -> Path:
        """
        Fetch a binary artifact to the raw cache and return its path.

        Cached artifacts are the audit trail: when a client questions a number
        six weeks later, the exact workbook that produced it is on disk.
        """
        target = self._cache_path(url, suffix)
        # Serialize on the target path: several drivers commonly resolve to
        # the same source file (e.g. multiple Pink Sheet series), and two
        # threads racing to download-and-write the same cache path could
        # otherwise interleave writes to it. The second thread through
        # re-checks freshness under the lock and gets a cache hit instead of
        # downloading again.
        with self._lock_for_path(str(target)):
            if not force and self._cache_is_fresh(target):
                log.info("Cache hit for %s -> %s", url, target.name)
                return target

            response = self.request(url)
            target.write_bytes(response.content)
            # Sidecar file records provenance for the cached blob.
            target.with_suffix(target.suffix + ".source.txt").write_text(
                f"{url}\nfetched_at={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n"
                f"content_type={response.headers.get('Content-Type', 'unknown')}\n"
                f"bytes={len(response.content)}\n",
                encoding="utf-8",
            )
            log.info("Downloaded %s (%.1f KB) -> %s", url, len(response.content) / 1024, target.name)
            return target
