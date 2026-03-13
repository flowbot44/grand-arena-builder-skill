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
        self.assertIn("support_stats", cumulative_latest)
        self.assertEqual(cumulative_latest["support_stats"]["url"], "support_stats.json")

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

        support_stats = _read_json(self.out_dir / "support_stats.json")
        self.assertEqual(support_stats["player_games"]["111"]["games"], 2)
        self.assertEqual(support_stats["player_games"]["111"]["wins"], 1)

    def test_export_includes_moki_totals_metadata_when_present(self) -> None:
        self._insert_seed_data()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        (self.out_dir / "moki_totals.json").write_text(
            json.dumps({"count": 2, "data": [{"tokenId": 111}, {"tokenId": 222}]}),
            encoding="utf-8",
        )

        export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 2, 26))

        latest = _read_json(self.out_dir / "latest.json")
        self.assertIn("moki_totals", latest)
        self.assertEqual(latest["moki_totals"]["url"], "moki_totals.json")
        self.assertEqual(latest["moki_totals"]["count"], 2)
        self.assertEqual(len(latest["moki_totals"]["sha256"]), 64)

    def test_export_rebuilds_when_immutable_raw_partition_is_missing(self) -> None:
        self._insert_seed_data()
        export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 2, 26))

        missing_path = self.out_dir / "partitions" / "raw_matches_2026-02-20.json.gz"
        missing_path.unlink()

        export_feed(
            self.conn,
            out_dir=self.out_dir,
            days=7,
            today=date(2026, 2, 26),
            mutable_days_back=2,
            cumulative_mutable_days_back=2,
        )

        self.assertTrue(missing_path.exists())
        rebuilt_rows = _read_gzip_json(missing_path)
        self.assertEqual(rebuilt_rows, [])

    def test_export_rebuilds_when_immutable_cumulative_seed_is_missing(self) -> None:
        self._insert_seed_data()
        export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 2, 26))

        missing_seed = self.out_dir / "cumulative" / "daily_totals_2026-02-23.json.gz"
        missing_seed.unlink()

        export_feed(
            self.conn,
            out_dir=self.out_dir,
            days=7,
            today=date(2026, 2, 26),
            mutable_days_back=2,
            cumulative_mutable_days_back=2,
        )

        self.assertTrue(missing_seed.exists())
        rebuilt_seed_rows = _read_gzip_json(missing_seed)
        self.assertEqual(rebuilt_seed_rows, [])

    def test_export_can_refresh_only_requested_raw_dates_and_skip_cumulative(self) -> None:
        self._insert_seed_data()
        export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 2, 26))

        baseline_old = _read_gzip_json(self.out_dir / "partitions" / "raw_matches_2026-02-25.json.gz")
        baseline_new = _read_gzip_json(self.out_dir / "partitions" / "raw_matches_2026-02-26.json.gz")
        cumulative_before = (self.out_dir / "cumulative" / "current_totals.json.gz").read_bytes()

        self.conn.execute(
            """
            INSERT INTO matches (
                match_id, game_type, match_date, state, is_bye, team_won, win_type, updated_at, last_seen_at
            ) VALUES
                ('m3', 'mokiMayhem', '2026-02-26', 'scheduled', 0, NULL, NULL, '2026-02-26T11:00:00Z', '2026-02-26T11:00:00Z')
            """
        )
        self.conn.execute(
            """
            INSERT INTO match_players (
                match_id, moki_id, token_id, team, name, class, image_url, is_champion
            ) VALUES
                ('m3', 'mk-444', 444, 1, 'Delta', 'Center', '', 0),
                ('m3', 'mk-555', 555, 2, 'Echo', 'Support', '', 0)
            """
        )
        self.conn.commit()

        export_feed(
            self.conn,
            out_dir=self.out_dir,
            days=7,
            today=date(2026, 2, 26),
            raw_refresh_start=date(2026, 2, 26),
            raw_refresh_end=date(2026, 2, 26),
            export_cumulative=False,
        )

        refreshed_old = _read_gzip_json(self.out_dir / "partitions" / "raw_matches_2026-02-25.json.gz")
        refreshed_new = _read_gzip_json(self.out_dir / "partitions" / "raw_matches_2026-02-26.json.gz")
        cumulative_after = (self.out_dir / "cumulative" / "current_totals.json.gz").read_bytes()

        self.assertEqual(baseline_old, refreshed_old)
        self.assertEqual(len(baseline_new), 1)
        self.assertEqual(len(refreshed_new), 2)
        self.assertEqual(cumulative_before, cumulative_after)

    def test_export_preserves_non_empty_old_partition_when_rebuild_is_empty(self) -> None:
        self._insert_seed_data()
        export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 3, 1))

        preserved_path = self.out_dir / "partitions" / "raw_matches_2026-02-25.json.gz"
        preserved_before = _read_gzip_json(preserved_path)
        self.assertEqual(len(preserved_before), 1)

        self.conn.execute("DELETE FROM performances WHERE match_id = 'm1'")
        self.conn.execute("DELETE FROM match_stats_players WHERE match_id = 'm1'")
        self.conn.execute("DELETE FROM match_players WHERE match_id = 'm1'")
        self.conn.execute("DELETE FROM matches WHERE match_id = 'm1'")
        self.conn.commit()

        export_feed(self.conn, out_dir=self.out_dir, days=7, today=date(2026, 3, 1))

        preserved_after = _read_gzip_json(preserved_path)
        self.assertEqual(preserved_after, preserved_before)

        latest = _read_json(self.out_dir / "latest.json")
        preserved_entry = next(part for part in latest["partitions"] if part["date"] == "2026-02-25")
        self.assertEqual(preserved_entry["match_count"], 1)


if __name__ == "__main__":
    unittest.main()
