"""Minimal config test double shared by this package's tests.

Not a mock of pipeline.config.Config's full interface — only the two
methods HttpClient/RateLimiter actually call (get, path). Kept separate
from unittest.mock so test bodies can override individual keys tersely.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Optional


class FakeConfig:
    def __init__(self, overrides: Optional[dict[str, Any]] = None, tmp_path: Optional[Path] = None):
        self._data: dict[str, Any] = {
            "http.timeout_seconds": 5,
            "http.max_retries": 1,
            "http.backoff_factor": 1.1,
            "http.per_host_delay_min_seconds": 0,
            "http.per_host_delay_max_seconds": 0,
            "http.cache_ttl_hours": 24,
            "http.verify_ssl": True,
            "http.bot_block_confirmation_threshold": 2,
            "http.bot_block_cooldown_seconds": 5,
            "http.bot_block_max_cooldown_multiplier": 4,
            "http.source_limits": {},
            "http.rate_limit_safety_margin": 1.0,
            # 0 so pre-existing budget/defer tests (written before the
            # wait-in-place behavior existed) keep deferring immediately
            # instead of blocking on a real time.sleep(); tests that
            # exercise the wait path override this explicitly.
            "http.rate_limit_wait_cap_seconds": 0,
        }
        if overrides:
            self._data.update(overrides)
        self._tmp_path = tmp_path or Path(tempfile.mkdtemp(prefix="pipeline_test_"))

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def path(self, key: str, default: str) -> Path:
        return self._tmp_path / default
