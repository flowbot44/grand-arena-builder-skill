from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from app.db import get_connection, init_db
from app.export_feed import export_feed


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _read_gzip_json(path: Path):
    with gzip.open(path, "rb") as fh:
        return json.loads(fh.read().decode("utf-8"))


class ExportFeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = f"{self.tmp.name}/test.db"
        self.conn = get_connection(self.db_path)
        init_db(self.conn)
        self.out_dir = Path(self.tmp.name) / "exports" / "data"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _insert_seed_data(self) -> None:
        self.conn.execute(
            """
            INSERT INTO matches (
                match_id, game_type, match_date, state, is_bye, team_won, win_type, updated_at, last_seen_at
            ) VALUES
                ('m1', 'mokiMayhem', '2026-02-25', 'scored', 0, 1, 'eliminations', '2026-02-25T10:00:00Z', '2026-02-25T10:00:00Z'),
                ('m2', 'mokiMayhem', '2026-02-26', 'scored', 0, 2, 'gacha', '2026-02-26T10:00:00Z', '2026-02-26T10:00:00Z')
            """
        )
        self.conn.execute(
            """
            INSERT INTO match_players (
                match_id, moki_id, token_id, team, name, class, image_url, is_champion
            ) VALUES
                ('m1', 'mk-111', 111, 1, 'Alpha', 'Center', '', 0),
                ('m1', 'mk-222', 222, 2, 'Beta', 'Flanker', '', 0),
                ('m2', 'mk-111', 111, 1, 'Alpha', 'Center', '', 0),
                ('m2', 'mk-333', 333, 2, 'Gamma', 'Support', '', 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO match_stats_players (
                match_id, token_id, team, won, points, eliminations, deposits, wart_distance
            ) VALUES
                ('m1', 111, 1, 1, 0, 1, 2, 80),
                ('m1', 222, 2, 0, 0, 0, 1, 0),
                ('m2', 111, 1, 0, 0, 0, 1, 160),
                ('m2', 333, 2, 1, 0, 2, 3, 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO performances (
                performance_id, match_id, moki_id, token_id, match_date, is_bye, win_type,
                deposits, eliminations, wart_distance, updated_at
            ) VALUES
                ('p1', 'm1', 'mk-111', 111, '2026-02-25', 0, 'eliminations', 2, 1, 80, '2026-02-25T10:01:00Z'),
                ('p2', 'm2', 'mk-111', 111, '2026-02-26', 0, 'gacha', 1, 0, 160, '2026-02-26T10:01:00Z')
            """
        )
        self.conn.execute(
            """
            INSERT INTO ingestion_runs (started_at, finished_at, status, details_json)
            VALUES ('2026-02-26T09:00:00Z', '2026-02-26T09:01:00Z', 'success', '{"ok":true}')
            """
        )
        self.conn.commit()

    def test_export_generates_raw_and_cumulative_contract(self) -> None:
        self._insert_seed_data()
        summary = export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 2, 26))
        self.assertEqual(summary["raw_partitions"], 7)

        latest = _read_json(self.out_dir / "latest.json")
        self.assertEqual(len(latest["available_dates"]), 7)
        self.assertEqual(latest["available_dates"][0], "2026-02-20")
        self.assertEqual(latest["available_dates"][-1], "2026-02-26")
        for part in latest["partitions"]:
            self.assertEqual(len(part["sha256"]), 64)
            self.assertTrue(part["url"].startswith("partitions/raw_matches_"))

        day_file = self.out_dir / "partitions" / "raw_matches_2026-02-26.json.gz"
        day_payload = _read_gzip_json(day_file)
        self.assertEqual(len(day_payload), 1)
        self.assertEqual(day_payload[0]["match"]["match_id"], "m2")
        self.assertGreaterEqual(len(day_payload[0]["players"]), 2)

        cumulative_latest = _read_json(self.out_dir / "cumulative" / "latest.json")
        self.assertEqual(cumulative_latest["available_dates"][-1], "2026-02-26")
        self.assertEqual(len(cumulative_latest["files"]), 7)
        self.assertEqual(len(cumulative_latest["current_totals"]["sha256"]), 64)

        current_totals = _read_gzip_json(self.out_dir / "cumulative" / "current_totals.json.gz")
        row_111 = [row for row in current_totals if int(row["token_id"]) == 111][0]
        self.assertEqual(row_111["games_played_cum"], 2)
        self.assertEqual(row_111["wins_cum"], 1)
        self.assertAlmostEqual(float(row_111["deposits_cum"]), 3.0)
        self.assertAlmostEqual(float(row_111["eliminations_cum"]), 1.0)
        self.assertAlmostEqual(float(row_111["wart_distance_cum"]), 240.0)
        self.assertAlmostEqual(float(row_111["points_cum"]), 665.0)

        daily_latest = _read_gzip_json(self.out_dir / "cumulative" / "daily_totals_2026-02-26.json.gz")
        self.assertEqual(current_totals, daily_latest)


if __name__ == "__main__":
    unittest.main()
