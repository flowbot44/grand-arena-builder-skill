from __future__ import annotations

import json
from datetime import date
import tempfile
import unittest

from app.analytics import (
    build_champion_history,
    build_champion_match_info,
    build_champion_next_matches,
    build_non_champion_history,
    build_non_champion_next_matches,
    recompute_champion_metrics,
)
from app.db import get_connection, init_db
from app.ingest import IngestionService


class FakeClient:
    def __init__(self) -> None:
        self.matches_by_date = {
            "2026-02-19": {"data": [], "pagination": {"page": 1, "pages": 1}},
            "2026-02-20": {
                "data": [
                    {
                        "id": "m1",
                        "gameType": "mokiMayhem",
                        "state": "scored",
                        "isBye": False,
                        "matchDate": "2026-02-20",
                        "updatedAt": "2026-02-20T10:00:00.000Z",
                        "players": [
                            {"mokiId": "a", "team": 1, "name": "Champ One", "tokenId": 73, "class": "Center", "imageUrl": ""},
                            {"mokiId": "b", "team": 1, "name": "Non Champ", "tokenId": 1111, "class": "Support", "imageUrl": ""},
                            {"mokiId": "c", "team": 2, "name": "Champ Two", "tokenId": 962, "class": "Grinder", "imageUrl": ""},
                            {"mokiId": "d", "team": 2, "name": "Non Champ 2", "tokenId": 2222, "class": "Flanker", "imageUrl": ""},
                        ],
                        "result": {"teamWon": 1, "winType": "eliminations"},
                    }
                ],
                "pagination": {"page": 1, "pages": 1},
            },
            "2026-02-21": {
                "data": [
                    {
                        "id": "m2",
                        "gameType": "mokiMayhem",
                        "state": "scheduled",
                        "isBye": False,
                        "matchDate": "2026-02-21",
                        "updatedAt": "2026-02-21T10:00:00.000Z",
                        "players": [
                            {"mokiId": "a2", "team": 1, "name": "Champ One", "tokenId": 73, "class": "Center", "imageUrl": ""},
                            {"mokiId": "b2", "team": 1, "name": "Non Champ", "tokenId": 1111, "class": "Support", "imageUrl": ""},
                            {"mokiId": "c2", "team": 2, "name": "Champ Two", "tokenId": 962, "class": "Grinder", "imageUrl": ""},
                            {"mokiId": "d2", "team": 2, "name": "Non Champ 2", "tokenId": 2222, "class": "Flanker", "imageUrl": ""},
                        ],
                    }
                ],
                "pagination": {"page": 1, "pages": 1},
            },
        }

    def list_matches(self, match_date: str, page: int, limit: int = 100):
        return self.matches_by_date.get(match_date, {"data": [], "pagination": {"page": 1, "pages": 1}})

    def get_match_stats(self, match_id: str):
        if match_id != "m1":
            return {"data": {"matchId": match_id, "state": "scheduled"}}
        return {
            "data": {
                "matchId": "m1",
                "state": "scored",
                "teamWon": 1,
                "winType": "eliminations",
                "teams": [
                    {
                        "teamNumber": 1,
                        "players": [
                            {"tokenId": 73, "team": 1, "won": True, "points": 14, "eliminations": 3, "deposits": 2, "wartDistance": 10},
                            {"tokenId": 1111, "team": 1, "won": True, "points": 11, "eliminations": 1, "deposits": 4, "wartDistance": 0},
                        ],
                    },
                    {
                        "teamNumber": 2,
                        "players": [
                            {"tokenId": 962, "team": 2, "won": False, "points": 8, "eliminations": 1, "deposits": 1, "wartDistance": 5},
                            {"tokenId": 2222, "team": 2, "won": False, "points": 7, "eliminations": 0, "deposits": 2, "wartDistance": 0},
                        ],
                    },
                ],
            }
        }

    def get_match_performances(self, match_id: str, page: int = 1, limit: int = 100):
        if match_id != "m1":
            return {"data": {"matchId": match_id, "state": "scheduled", "gameType": "mokiMayhem", "performances": []}, "pagination": {"page": 1, "pages": 1}}
        return {
            "data": {
                "matchId": "m1",
                "state": "scored",
                "gameType": "mokiMayhem",
                "performances": [
                    {
                        "id": "p1",
                        "matchId": "m1",
                        "mokiId": "a",
                        "tokenId": 73,
                        "matchDate": "2026-02-20",
                        "isBye": False,
                        "updatedAt": "2026-02-20T10:01:00.000Z",
                        "results": {"winType": "eliminations", "deposits": 2, "eliminations": 3, "wartDistance": 10},
                    },
                    {
                        "id": "p2",
                        "matchId": "m1",
                        "mokiId": "b",
                        "tokenId": 1111,
                        "matchDate": "2026-02-20",
                        "isBye": False,
                        "updatedAt": "2026-02-20T10:01:00.000Z",
                        "results": {"winType": "eliminations", "deposits": 4, "eliminations": 1, "wartDistance": 0},
                    },
                ],
            },
            "pagination": {"page": 1, "pages": 1},
        }


class BackfillAndUpcomingTests(unittest.TestCase):
    def _seeded_service(self) -> IngestionService:
        self.tmp = tempfile.TemporaryDirectory()
        db_path = f"{self.tmp.name}/test.db"
        champions_path = f"{self.tmp.name}/champions.json"
        with open(champions_path, "w", encoding="utf-8") as f:
            json.dump(
                [
                    {"id": 73, "name": "Champ One", "traits": ["A"]},
                    {"id": 962, "name": "Champ Two", "traits": ["B"]},
                ],
                f,
            )

        conn = get_connection(db_path)
        init_db(conn)
        self.conn = conn
        return IngestionService(conn, FakeClient(), champions_path=champions_path)

    def tearDown(self) -> None:
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    def test_backfill_from_2026_02_19_loads_and_enriches(self) -> None:
        service = self._seeded_service()
        details = service.run_date_range(date(2026, 2, 19), date(2026, 2, 21))

        self.assertEqual(details["start"], "2026-02-19")
        self.assertEqual(details["end"], "2026-02-21")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM matches").fetchone()["c"], 2)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) AS c FROM match_stats_players WHERE match_id = 'm1'").fetchone()["c"],
            4,
        )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM performances WHERE match_id = 'm1'").fetchone()["c"], 2)

    def test_sparse_upcoming_sets_insufficient_flag(self) -> None:
        service = self._seeded_service()
        service.run_date_range(date(2026, 2, 19), date(2026, 2, 21))
        recompute_champion_metrics(self.conn)

        payload = build_champion_next_matches(
            self.conn,
            73,
            limit=10,
            today=date(2026, 2, 21),
            lookahead_days=2,
        )

        self.assertEqual(payload["lookahead_days"], 2)
        self.assertEqual(payload["window_start"], "2026-02-21")
        self.assertEqual(payload["window_end"], "2026-02-23")
        self.assertTrue(payload["insufficient_upcoming"])
        self.assertLess(len(payload["matches"]), 10)
        self.assertIn("opponent_team_support_win_pct", payload["matches"][0]["components"])

    def test_history_returns_games_and_totals(self) -> None:
        service = self._seeded_service()
        service.run_date_range(date(2026, 2, 19), date(2026, 2, 21))
        payload = build_champion_history(self.conn, 73)

        self.assertEqual(payload["champion"]["token_id"], 73)
        self.assertEqual(payload["totals"]["games"], 1)
        self.assertEqual(payload["totals"]["wins"], 1)
        self.assertEqual(payload["totals"]["losses"], 0)
        self.assertAlmostEqual(payload["totals"]["points"], 640.0)
        self.assertAlmostEqual(payload["totals"]["deposits"], 2.0)
        self.assertAlmostEqual(payload["totals"]["eliminations"], 3.0)
        self.assertAlmostEqual(payload["totals"]["wart_distance"], 10.0)
        self.assertAlmostEqual(payload["summary"]["win_pct"], 1.0)
        self.assertAlmostEqual(payload["summary"]["avg_points"], 640.0)
        self.assertAlmostEqual(payload["summary"]["win_type_pct"]["eliminations"], 1.0)

    def test_enrich_only_fills_missing_stats_without_matches_pull(self) -> None:
        service = self._seeded_service()
        service.run_date_range(date(2026, 2, 19), date(2026, 2, 21))

        self.conn.execute("DELETE FROM match_stats_players WHERE match_id = 'm1'")
        self.conn.commit()

        details = service.run_enrichment_only(
            start=date(2026, 2, 19),
            end=date(2026, 2, 21),
            max_matches=10,
        )
        self.assertEqual(details["mode"], "enrich-only")
        self.assertEqual(details["processed_matches"], 1)
        self.assertGreater(details["stats_upserts"], 0)

    def test_non_champion_history_and_lookahead(self) -> None:
        service = self._seeded_service()
        service.run_date_range(date(2026, 2, 19), date(2026, 2, 21))

        history = build_non_champion_history(self.conn, 1111)
        self.assertEqual(history["player"]["token_id"], 1111)
        self.assertEqual(history["totals"]["games"], 1)
        self.assertEqual(history["totals"]["wins"], 1)
        self.assertAlmostEqual(history["totals"]["deposits"], 4.0)

        upcoming = build_non_champion_next_matches(
            self.conn,
            1111,
            limit=10,
            today=date(2026, 2, 21),
            lookahead_days=2,
        )
        self.assertEqual(upcoming["player"]["token_id"], 1111)
        self.assertTrue(upcoming["insufficient_upcoming"])

    def test_champion_match_info_contains_requested_cohort_stats(self) -> None:
        service = self._seeded_service()
        service.run_date_range(date(2026, 2, 19), date(2026, 2, 21))

        payload = build_champion_match_info(self.conn, 73)
        self.assertEqual(payload["champion"]["token_id"], 73)
        self.assertGreaterEqual(len(payload["matches"]), 1)
        row = payload["matches"][0]
        self.assertEqual(row["result"], "W")
        self.assertEqual(row["win_type"], "eliminations")
        self.assertIn("Center", row["champion"]["classes"])
        self.assertIn("Support", row["team_non_champion"]["classes"])
        self.assertIn("Grinder", row["opponent_champion"]["classes"])
        self.assertIn("Flanker", row["opponent_non_champion"]["classes"])
        self.assertIn("Support", payload["combined_classes"]["team_non_champion"]["unique_classes"])
        self.assertIn("Grinder", payload["combined_classes"]["opponent_champion"]["unique_classes"])
        self.assertIn("Flanker", payload["combined_classes"]["opponent_non_champion"]["unique_classes"])


if __name__ == "__main__":
    unittest.main()
