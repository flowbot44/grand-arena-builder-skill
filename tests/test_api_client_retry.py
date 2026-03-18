from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from app.api_client import GrandArenaClient, RateLimiter


class _FakeHTTPResponse:
    status = 200

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ApiClientRetryTests(unittest.TestCase):
    def test_retries_on_timeout_error(self) -> None:
        limiter = RateLimiter(max_per_minute=1000, min_interval_seconds=0.0, now_fn=lambda: 0.0, sleep_fn=lambda _x: None)
        client = GrandArenaClient(
            base_url="https://api.example.test",
            api_key="k",
            rate_limiter=limiter,
            timeout_seconds=1,
            retries=2,
        )
        good = _FakeHTTPResponse(json.dumps({"data": [], "pagination": {"page": 1, "pages": 1}}).encode("utf-8"))
        with patch("app.api_client.urlopen", side_effect=[TimeoutError("read timed out"), good]):
            with patch("app.api_client.time.sleep", return_value=None):
                payload = client.list_matches("2026-02-28", page=1, limit=100)
        self.assertIn("data", payload)
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["attempts"], 2)
        self.assertEqual(telemetry["successes"], 1)
        self.assertEqual(telemetry["timeouts"], 1)
        self.assertEqual(telemetry["retries"], 1)
        self.assertGreaterEqual(telemetry["backoff_sleep_seconds"], 1.0)

    def test_list_matches_defaults_to_asc_order_by_id(self) -> None:
        limiter = RateLimiter(max_per_minute=1000, min_interval_seconds=0.0, now_fn=lambda: 0.0, sleep_fn=lambda _x: None)
        client = GrandArenaClient(
            base_url="https://api.example.test",
            api_key="k",
            rate_limiter=limiter,
            timeout_seconds=1,
            retries=0,
        )

        captured = {}

        def fake_urlopen(req, timeout=0):
            captured["url"] = req.full_url
            return _FakeHTTPResponse(json.dumps({"data": [], "pagination": {"page": 1, "pages": 1}}).encode("utf-8"))

        with patch("app.api_client.urlopen", side_effect=fake_urlopen):
            client.list_matches("2026-02-28", page=2, limit=50)

        self.assertIn("order=asc", captured["url"])
        self.assertIn("sort=matchDate", captured["url"])


if __name__ == "__main__":
    unittest.main()
