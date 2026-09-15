"""
Tests for RateLimiter (proactive per-source rate/quota budgeting) in
pipeline/http_client.py — the "scheduled/batched instead of continuous
calls" mechanism. Verifies budget counting, deferral once exhausted,
window reset, and the deferred-state persistence used by --resume-deferred.
"""

from __future__ import annotations

import json
import time
import unittest
from unittest.mock import patch

from pipeline.http_client import RateLimiter, load_deferred_driver_ids
from pipeline.tests._fakes import FakeConfig

HOST = "api.example.org"


class TestRateLimiterBudgeting(unittest.TestCase):
    def _limiter(self, limits: dict, safety_margin: float = 1.0, wait_cap_seconds: float = 0) -> RateLimiter:
        cfg = FakeConfig(overrides={
            "http.source_limits": limits,
            "http.rate_limit_safety_margin": safety_margin,
            "http.rate_limit_wait_cap_seconds": wait_cap_seconds,
        })
        return RateLimiter(cfg, cfg.path("paths.rate_limit_state", "output/rate_limit_state.json"))

    def test_host_with_no_configured_limit_never_defers(self):
        limiter = self._limiter({})
        for _ in range(50):
            self.assertIsNone(limiter.check(HOST, "SOME_DRIVER"))

    def test_budget_exhausted_defers_and_records_the_driver(self):
        limiter = self._limiter({HOST: {"requests_per_minute": 3}})
        for i in range(3):
            self.assertIsNone(limiter.check(HOST, f"DRIVER_{i}"), f"call {i} should be within budget")

        reason = limiter.check(HOST, "DRIVER_OVER_BUDGET")
        self.assertIsNotNone(reason)
        self.assertIn(HOST, reason)

        entries = limiter.deferred_entries()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["driver_id"], "DRIVER_OVER_BUDGET")
        self.assertEqual(entries[0]["requests_budget"], 3)
        self.assertEqual(entries[0]["source_host"], HOST)

    def test_window_reset_allows_further_calls(self):
        limiter = self._limiter({HOST: {"requests_per_minute": 1}})
        self.assertIsNone(limiter.check(HOST, "DRIVER_A"))
        self.assertIsNotNone(limiter.check(HOST, "DRIVER_B"))

        # Simulate the 60s window having fully elapsed.
        with patch("pipeline.http_client.time.monotonic", return_value=time.monotonic() + 61):
            self.assertIsNone(limiter.check(HOST, "DRIVER_C"))

    def test_independent_hosts_have_independent_budgets(self):
        limiter = self._limiter({
            HOST: {"requests_per_minute": 1},
            "other.example.org": {"requests_per_minute": 1},
        })
        self.assertIsNone(limiter.check(HOST, "DRIVER_A"))
        self.assertIsNone(limiter.check("other.example.org", "DRIVER_B"))
        self.assertIsNotNone(limiter.check(HOST, "DRIVER_C"))
        self.assertIsNotNone(limiter.check("other.example.org", "DRIVER_D"))

    def test_safety_margin_reduces_effective_budget(self):
        # 50% of a documented 10/minute budget is 5 — the 6th call should
        # already be treated as over budget, not the 11th.
        limiter = self._limiter({HOST: {"requests_per_minute": 10}}, safety_margin=0.5)
        for i in range(5):
            self.assertIsNone(limiter.check(HOST, f"DRIVER_{i}"), f"call {i} should be within the 50% margin")

        reason = limiter.check(HOST, "DRIVER_OVER_MARGIN")
        self.assertIsNotNone(reason)
        entries = limiter.deferred_entries()
        self.assertEqual(entries[0]["requests_budget"], 5)

    def test_safety_margin_never_rounds_effective_budget_to_zero(self):
        # A tiny documented budget scaled by a small margin must still allow
        # at least one call through, not lock the host out entirely.
        limiter = self._limiter({HOST: {"requests_per_minute": 1}}, safety_margin=0.1)
        self.assertIsNone(limiter.check(HOST, "DRIVER_A"))

    def test_window_resetting_beyond_wait_cap_defers_without_sleeping(self):
        # A freshly-started daily window is nowhere near resetting, so even
        # with waiting enabled (cap 900s) a 500/day-style budget still
        # defers immediately rather than blocking a thread for ~a day.
        limiter = self._limiter({HOST: {"requests_per_day": 1}}, wait_cap_seconds=900)
        self.assertIsNone(limiter.check(HOST, "DRIVER_A"))

        with patch("pipeline.http_client.time.sleep") as fake_sleep:
            reason = limiter.check(HOST, "DRIVER_OVER_BUDGET")
        fake_sleep.assert_not_called()
        self.assertIsNotNone(reason)
        self.assertEqual(limiter.waited_entries(), [])

    def test_window_resetting_within_wait_cap_waits_then_succeeds(self):
        # A per-minute budget's 60s window is within a 120s wait cap, so the
        # over-budget call should block until reset and then go through —
        # recorded as "waited", never deferred.
        limiter = self._limiter({HOST: {"requests_per_minute": 1}}, wait_cap_seconds=120)
        clock = {"t": 1_000.0}

        def fake_monotonic():
            return clock["t"]

        def fake_sleep(seconds):
            clock["t"] += seconds

        with patch("pipeline.http_client.time.monotonic", side_effect=fake_monotonic), \
             patch("pipeline.http_client.time.sleep", side_effect=fake_sleep):
            self.assertIsNone(limiter.check(HOST, "DRIVER_A"))       # consumes the only slot
            result = limiter.check(HOST, "DRIVER_B")                 # over budget, but resets in 60s <= 120s cap

        self.assertIsNone(result, "call should succeed after waiting out the window, not defer")
        self.assertEqual(limiter.deferred_entries(), [])
        waited = limiter.waited_entries()
        self.assertEqual(len(waited), 1)
        self.assertEqual(waited[0]["driver_id"], "DRIVER_B")
        self.assertEqual(waited[0]["source_host"], HOST)
        self.assertEqual(waited[0]["waited_seconds"], 60)


class TestRateLimiterStatePersistence(unittest.TestCase):
    def test_save_state_writes_nothing_when_no_deferrals(self):
        cfg = FakeConfig()
        state_path = cfg.path("paths.rate_limit_state", "output/rate_limit_state.json")
        limiter = RateLimiter(cfg, state_path)
        limiter.save_state()
        self.assertFalse(state_path.exists())

    def test_save_and_load_round_trip(self):
        cfg = FakeConfig(overrides={"http.source_limits": {HOST: {"requests_per_minute": 1}}})
        state_path = cfg.path("paths.rate_limit_state", "output/rate_limit_state.json")
        limiter = RateLimiter(cfg, state_path)

        limiter.check(HOST, "DRIVER_A")               # consumes the only slot
        limiter.check(HOST, "DRIVER_DEFERRED")         # over budget -> recorded
        limiter.save_state()

        self.assertTrue(state_path.exists())
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(len(payload["deferred"]), 1)

        loaded = load_deferred_driver_ids(state_path)
        self.assertEqual(loaded, {"DRIVER_DEFERRED"})

    def test_load_deferred_driver_ids_missing_file_returns_empty_set(self):
        cfg = FakeConfig()
        missing = cfg.path("paths.rate_limit_state", "output/does_not_exist.json")
        self.assertEqual(load_deferred_driver_ids(missing), set())


if __name__ == "__main__":
    unittest.main()
