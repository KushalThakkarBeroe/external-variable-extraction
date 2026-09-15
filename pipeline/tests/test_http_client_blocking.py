"""
Tests for the corroboration + cooldown host-blocking mechanism in
pipeline/http_client.py.

This exists because a single confirmed block used to poison a shared host
for every other driver for the rest of a run (see the module docstring in
http_client.py for the full history) — the exact bug that wiped out 10
UN Comtrade drivers in one run despite a genuine 403 having come from only
one of them. These tests simulate the HTTP layer via a mocked
requests.Session.request so the behavior can be verified without a live
network call or a real quota to burn.
"""

from __future__ import annotations

import time
import unittest
from unittest.mock import MagicMock

import requests

from pipeline.http_client import HttpClient
from pipeline.tests._fakes import FakeConfig

HOST = "example.org"
URL = f"https://{HOST}/api"


def _mock_response(status_code: int, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    resp.encoding = "utf-8"
    resp.apparent_encoding = "utf-8"
    resp.content = b"{}"
    resp.json.return_value = {}
    return resp


class TestBotBlockCorroborationAndCooldown(unittest.TestCase):
    def test_single_signal_does_not_block_host(self):
        http = HttpClient(FakeConfig())
        http.session.request = MagicMock(side_effect=[_mock_response(403)])

        with self.assertRaises(requests.exceptions.RequestException):
            http.request(URL)

        self.assertNotIn(HOST, http.bot_blocked_hosts)
        self.assertEqual(http.session.request.call_count, 1)

    def test_second_signal_blocks_host_and_third_call_is_short_circuited(self):
        http = HttpClient(FakeConfig())
        http.session.request = MagicMock(side_effect=[_mock_response(403), _mock_response(403)])

        for _ in range(2):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request(URL)

        self.assertIn(HOST, http.bot_blocked_hosts)
        self.assertEqual(http.session.request.call_count, 2)

        # Third call must not touch the network at all.
        with self.assertRaises(requests.exceptions.RequestException):
            http.request(URL)
        self.assertEqual(http.session.request.call_count, 2)

    def test_retry_after_header_sets_the_cooldown_window(self):
        http = HttpClient(FakeConfig())
        http.session.request = MagicMock(side_effect=[
            _mock_response(403),
            _mock_response(403, headers={"Retry-After": "5"}),
        ])

        for _ in range(2):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request(URL)

        remaining = http._host_blocked_until[HOST] - time.monotonic()
        self.assertGreater(remaining, 0)
        self.assertLessEqual(remaining, 5.5)

    def test_cooldown_expiry_allows_one_probation_request(self):
        http = HttpClient(FakeConfig(overrides={"http.bot_block_cooldown_seconds": 5}))
        http.session.request = MagicMock(side_effect=[
            _mock_response(403), _mock_response(403), _mock_response(200),
        ])

        for _ in range(2):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request(URL)
        self.assertIn(HOST, http.bot_blocked_hosts)

        # Simulate the cooldown having elapsed.
        http._host_blocked_until[HOST] = time.monotonic() - 0.01

        response = http.request(URL)  # probation request: a real network call
        self.assertEqual(response.status_code, 200)
        self.assertEqual(http.session.request.call_count, 3)

    def test_repeat_block_on_probation_extends_cooldown(self):
        http = HttpClient(FakeConfig(overrides={"http.bot_block_cooldown_seconds": 5}))
        http.session.request = MagicMock(side_effect=[
            _mock_response(403), _mock_response(403), _mock_response(403),
        ])

        for _ in range(2):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request(URL)
        first_remaining = http._host_blocked_until[HOST] - time.monotonic()

        http._host_blocked_until[HOST] = time.monotonic() - 0.01
        with self.assertRaises(requests.exceptions.RequestException):
            http.request(URL)  # probation attempt fails again

        self.assertEqual(http.session.request.call_count, 3)
        second_remaining = http._host_blocked_until[HOST] - time.monotonic()
        self.assertGreater(second_remaining, first_remaining)

    def test_finalize_bot_block_flags_uses_real_attempted_host_not_source_url(self):
        """
        Regression test for the reporting bug this session fixed: the flag
        must be driven by the host a driver's connector actually called
        (attempted_hosts_by_driver), not the registry's landing-page
        source_url, which is frequently a different host entirely.
        """
        http = HttpClient(FakeConfig())
        http.set_current_driver("SOME_DRIVER")
        http.session.request = MagicMock(side_effect=[_mock_response(403), _mock_response(403)])
        for _ in range(2):
            with self.assertRaises(requests.exceptions.RequestException):
                http.request(URL)

        self.assertIn(HOST, http.bot_blocked_hosts)
        self.assertEqual(http.attempted_hosts_by_driver["SOME_DRIVER"], {HOST})


if __name__ == "__main__":
    unittest.main()
