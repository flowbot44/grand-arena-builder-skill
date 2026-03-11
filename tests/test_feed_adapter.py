from __future__ import annotations

import gzip
import json
import unittest
from datetime import date
from unittest.mock import patch
from urllib.error import URLError

from app.feed_adapter import FeedAdapter, FeedUnavailableError
from app.serve import create_app


class _FakeHTTPResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _gz_json(payload):
    return gzip.compress(json.dumps(payload).encode("utf-8"))


class FeedAdapterTests(unittest.TestCase):
    def test_champion_rows_prefers_stats_over_performances(self) -> None:
        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=600, timeout_seconds=1)

        adapter.get_latest_manifest = lambda: (  # type: ignore[method-assign]
            {
                "lookahead_days": 2,
                "available_dates": ["2026-02-27"],
            },
            type("Meta", (), {"data_generated_at": "2026-02-27T00:00:00+00:00", "cache_age_seconds": 0, "stale_data": False})(),
        )
        adapter._all_partitions_payloads = lambda: (  # type: ignore[method-assign]
            [
                {
                    "match": {"state": "scored", "team_won": 1},
                    "players": [{"token_id": 73, "team": 1, "name": "Champ", "is_champion": True}],
                    "stats_players": [
                        {
                            "token_id": 73,
                            "team": 1,
                            "deposits": 2,
                            "eliminations": 3,
                            "wart_distance": 10,
                        }
                    ],
                    "performances": [
                        {
                            "token_id": 73,
                            "deposits": 9,
                            "eliminations": 9,
                            "wart_distance": 160,
                        }
                    ],
                }
            ],
            type("Meta", (), {"data_generated_at": "2026-02-27T00:00:00+00:00", "cache_age_seconds": 0, "stale_data": False})(),
        )

        payload, _meta = adapter.champion_rows()
        row = payload["rows"][0]
        self.assertEqual(row["token_id"], 73)
        self.assertEqual(row["avg_points"], 640.0)

    def test_gzip_decode_current_totals(self) -> None:
        calls = []

        def fake_urlopen(url, timeout=0):
            calls.append(url)
            if url.endswith("/cumulative/latest.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T00:00:00+00:00",
                            "current_totals": {"url": "cumulative/current_totals.json.gz"},
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/cumulative/current_totals.json.gz"):
                return _FakeHTTPResponse(_gz_json([{"token_id": 1, "games_played_cum": 2}]))
            raise AssertionError(f"unexpected URL: {url}")

        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=600, timeout_seconds=1)
        with patch("app.feed_adapter.urlopen", side_effect=fake_urlopen):
            rows, _meta = adapter.get_current_totals()
        self.assertEqual(rows[0]["token_id"], 1)
        self.assertEqual(len(calls), 2)

    def test_manifest_parse_error_without_cache_raises(self) -> None:
        def fake_urlopen(url, timeout=0):
            return _FakeHTTPResponse(json.dumps({"bad": "shape"}).encode("utf-8"))

        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=600, timeout_seconds=1)
        with patch("app.feed_adapter.urlopen", side_effect=fake_urlopen):
            with self.assertRaises(FeedUnavailableError):
                adapter.get_latest_manifest()

    def test_ttl_cache_hit_skips_refetch(self) -> None:
        call_count = {"n": 0}

        def fake_urlopen(url, timeout=0):
            call_count["n"] += 1
            return _FakeHTTPResponse(
                json.dumps(
                    {
                        "generated_at_utc": "2026-02-27T00:00:00+00:00",
                        "window_days": 7,
                        "available_dates": ["2026-02-26"],
                        "partitions": [],
                    }
                ).encode("utf-8")
            )

        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=600, timeout_seconds=1)
        with patch("app.feed_adapter.urlopen", side_effect=fake_urlopen):
            adapter.get_latest_manifest()
            adapter.get_latest_manifest()
        self.assertEqual(call_count["n"], 1)

    def test_stale_cache_fallback_after_refresh_failure(self) -> None:
        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=1, timeout_seconds=1)
        adapter.ttl_seconds = 0
        ok_response = _FakeHTTPResponse(
            json.dumps(
                {
                    "generated_at_utc": "2026-02-27T00:00:00+00:00",
                    "window_days": 7,
                    "available_dates": ["2026-02-26"],
                    "partitions": [],
                }
            ).encode("utf-8")
        )
        with patch("app.feed_adapter.urlopen", side_effect=[ok_response, URLError("network down")]):
            adapter.get_latest_manifest()
            _payload, meta = adapter.get_latest_manifest()
        self.assertTrue(meta.stale_data)
        self.assertGreaterEqual(meta.cache_age_seconds, 0)

    def test_fetch_moki_totals_from_latest_manifest(self) -> None:
        calls = []

        def fake_urlopen(url, timeout=0):
            calls.append(url)
            if url.endswith("/cumulative/latest.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T00:00:00+00:00",
                            "current_totals": {"url": "cumulative/current_totals.json.gz"},
                            "support_stats": {"url": "support_stats.json"},
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/latest.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T00:00:00+00:00",
                            "window_days": 7,
                            "available_dates": ["2026-02-26"],
                            "partitions": [],
                            "moki_totals": {"url": "moki_totals.json"},
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/moki_totals.json"):
                return _FakeHTTPResponse(
                    json.dumps({"count": 1, "data": [{"tokenId": 807, "name": "T807"}]}).encode("utf-8")
                )
            raise AssertionError(f"unexpected URL: {url}")

        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=600, timeout_seconds=1)
        with patch("app.feed_adapter.urlopen", side_effect=fake_urlopen):
            payload, _meta = adapter.get_moki_totals()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["data"][0]["tokenId"], 807)
        self.assertEqual(len(calls), 2)

    def test_champion_next_matches_uses_support_stats_feed(self) -> None:
        calls = []

        def fake_urlopen(url, timeout=0):
            calls.append(url)
            if url.endswith("/cumulative/latest.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T00:00:00+00:00",
                            "current_totals": {"url": "cumulative/current_totals.json.gz"},
                            "support_stats": {"url": "support_stats.json"},
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/latest.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T00:00:00+00:00",
                            "window_days": 7,
                            "lookahead_days": 1,
                            "available_dates": ["2026-02-27"],
                            "partitions": [
                                {
                                    "date": "2026-02-27",
                                    "url": "partitions/raw_matches_2026-02-27.json.gz",
                                    "sha256": "x",
                                    "bytes": 1,
                                    "match_count": 1,
                                }
                            ],
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/partitions/raw_matches_2026-02-27.json.gz"):
                return _FakeHTTPResponse(
                    _gz_json(
                        [
                            {
                                "match": {"match_id": "m1", "match_date": "2026-02-27", "state": "scheduled"},
                                "players": [
                                    {"token_id": 807, "name": "Champ", "team": 1, "is_champion": 1},
                                    {"token_id": 1001, "name": "NC1", "team": 1, "is_champion": 0},
                                    {"token_id": 962, "name": "Opp", "team": 2, "is_champion": 1},
                                ],
                                "stats_players": [],
                                "performances": [],
                            }
                        ]
                    )
                )
            if url.endswith("/support_stats.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T00:00:00+00:00",
                            "player_games": {"1001": {"games": 4, "wins": 3}},
                            "champion_games": {"807": {"games": 5, "wins": 4}, "962": {"games": 6, "wins": 2}},
                        }
                    ).encode("utf-8")
                )
            raise AssertionError(f"unexpected URL: {url}")

        adapter = FeedAdapter(base_url="https://example.com/data", ttl_seconds=600, timeout_seconds=1)
        with patch("app.feed_adapter.urlopen", side_effect=fake_urlopen), patch("app.feed_adapter.utc_today", return_value=date(2026, 2, 27)):
            payload, _meta = adapter.champion_next_matches(807, limit=10, lookahead_days=1)
        self.assertEqual(payload["champion"]["token_id"], 807)
        self.assertEqual(len(payload["matches"]), 1)
        self.assertTrue(any(url.endswith("/support_stats.json") for url in calls))


class FeedRoutesTests(unittest.TestCase):
    def test_api_routes_use_feed_without_db(self) -> None:
        def fake_urlopen(url, timeout=0):
            if url.endswith("/latest.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T10:00:00+00:00",
                            "window_days": 7,
                            "lookahead_days": 1,
                            "available_dates": ["2026-02-26", "2026-02-27"],
                            "partitions": [
                                {
                                    "date": "2026-02-27",
                                    "url": "partitions/raw_matches_2026-02-27.json.gz",
                                    "sha256": "x",
                                    "bytes": 1,
                                    "match_count": 1,
                                }
                            ],
                            "moki_totals": {"url": "moki_totals.json"},
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/status.json"):
                return _FakeHTTPResponse(
                    json.dumps(
                        {
                            "generated_at_utc": "2026-02-27T10:01:00+00:00",
                            "window_days": 7,
                            "window_start": "2026-02-21",
                            "window_end": "2026-02-28",
                            "raw_dates": ["2026-02-27"],
                            "latest_ingestion_run": {"run_id": 1, "status": "success"},
                        }
                    ).encode("utf-8")
                )
            if url.endswith("/partitions/raw_matches_2026-02-27.json.gz"):
                return _FakeHTTPResponse(
                    _gz_json(
                        [
                            {
                                "match": {"match_id": "m1", "match_date": "2026-02-27", "state": "scored", "team_won": 1, "win_type": "gacha"},
                                "players": [
                                    {"token_id": 807, "name": "T807", "team": 1, "is_champion": 1},
                                    {"token_id": 1001, "name": "NC1", "team": 1, "is_champion": 0},
                                ],
                                "stats_players": [
                                    {"token_id": 807, "team": 1, "deposits": 1, "eliminations": 2, "wart_distance": 80},
                                    {"token_id": 1001, "team": 1, "deposits": 0, "eliminations": 0, "wart_distance": 0},
                                ],
                                "performances": [],
                            }
                        ]
                    )
                )
            if url.endswith("/moki_totals.json"):
                return _FakeHTTPResponse(
                    json.dumps({"count": 1, "data": [{"tokenId": 807, "name": "T807"}]}).encode("utf-8")
                )
            raise AssertionError(f"unexpected URL: {url}")

        with patch("app.feed_adapter.urlopen", side_effect=fake_urlopen):
            app = create_app()
            with app.test_client() as client:
                champions_resp = client.get("/api/champions")
                self.assertEqual(champions_resp.status_code, 200)
                champions_body = champions_resp.get_json()
                self.assertEqual(champions_body["source"], "github_feed")
                self.assertIn("data_generated_at", champions_body)
                self.assertIn("cache_age_seconds", champions_body)
                self.assertIn("stale_data", champions_body)

                status_resp = client.get("/api/system/status")
                self.assertEqual(status_resp.status_code, 200)
                status_body = status_resp.get_json()
                self.assertEqual(status_body["source"], "github_feed")

                moki_resp = client.get("/api/moki-totals")
                self.assertEqual(moki_resp.status_code, 200)
                moki_body = moki_resp.get_json()
                self.assertEqual(moki_body["source"], "github_feed")
                self.assertEqual(moki_body["count"], 1)


if __name__ == "__main__":
    unittest.main()
