from __future__ import annotations

import tempfile
import unittest
from datetime import date

from app.db import get_connection, init_db
from app.maintenance import prune_old_matches


class MaintenancePruneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = f"{self.tmp.name}/test.db"
        self.conn = get_connection(self.db_path)
        init_db(self.conn)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _insert_match_bundle(self, *, match_id: str, match_date: str, token_id: int) -> None:
        self.conn.execute(
            """
            INSERT INTO matches (
                match_id, game_type, match_date, state, is_bye, team_won, win_type, updated_at, last_seen_at
            ) VALUES (?, 'mokiMayhem', ?, 'scored', 0, 1, 'eliminations', '2026-02-26T00:00:00Z', '2026-02-26T00:00:00Z')
            """,
            (match_id, match_date),
        )
        self.conn.execute(
            """
            INSERT INTO match_players (
                match_id, moki_id, token_id, team, name, class, image_url, is_champion
            ) VALUES (?, ?, ?, 1, 'Player', 'Center', '', 0)
            """,
            (match_id, f"moki-{token_id}", token_id),
        )
        self.conn.execute(
            """
            INSERT INTO match_stats_players (
                match_id, token_id, team, won, points, eliminations, deposits, wart_distance
            ) VALUES (?, ?, 1, 1, 100, 1, 1, 80)
            """,
            (match_id, token_id),
        )
        self.conn.execute(
            """
            INSERT INTO performances (
                performance_id, match_id, moki_id, token_id, match_date, is_bye, win_type,
                deposits, eliminations, wart_distance, updated_at
            ) VALUES (?, ?, ?, ?, ?, 0, 'eliminations', 1, 1, 80, '2026-02-26T00:00:00Z')
            """,
            (f"perf-{match_id}", match_id, f"moki-{token_id}", token_id, match_date),
        )
        self.conn.commit()

    def test_prune_only_deletes_rows_older_than_cutoff(self) -> None:
        self._insert_match_bundle(match_id="old", match_date="2026-02-19", token_id=1001)
        self._insert_match_bundle(match_id="new", match_date="2026-02-25", token_id=1002)

        result = prune_old_matches(self.conn, keep_days=7, today=date(2026, 2, 26))

        self.assertEqual(result["cutoff_date"], "2026-02-20")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM matches").fetchone()["c"], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM matches WHERE match_id='new'").fetchone()["c"], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM match_players").fetchone()["c"], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM match_stats_players").fetchone()["c"], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) AS c FROM performances").fetchone()["c"], 1)

    def test_prune_empty_tables_noop(self) -> None:
        result = prune_old_matches(self.conn, keep_days=7, today=date(2026, 2, 26))
        self.assertEqual(result["deleted_matches"], 0)
        self.assertEqual(result["deleted_match_players"], 0)
        self.assertEqual(result["deleted_match_stats_players"], 0)
        self.assertEqual(result["deleted_performances"], 0)


if __name__ == "__main__":
    unittest.main()
