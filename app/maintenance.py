from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, timedelta
from typing import Dict

from .analytics import recompute_champion_metrics
from .config import SETTINGS
from .db import get_connection, init_db
from .time_utils import utc_today_iso


def _window_start(today: date, keep_days: int) -> date:
    if keep_days <= 1:
        return today
    return today - timedelta(days=keep_days - 1)


def prune_old_matches(conn: sqlite3.Connection, *, keep_days: int, today: date) -> Dict[str, int]:
    cutoff = _window_start(today, keep_days).isoformat()

    match_rows = conn.execute(
        """
        SELECT match_id
        FROM matches
        WHERE match_date < ?
        """,
        (cutoff,),
    ).fetchall()
    match_ids = [row["match_id"] for row in match_rows]
    if not match_ids:
        return {
            "cutoff_date": cutoff,
            "deleted_matches": 0,
            "deleted_match_players": 0,
            "deleted_match_stats_players": 0,
            "deleted_performances": 0,
        }

    placeholders = ",".join("?" for _ in match_ids)

    cur = conn.cursor()
    cur.execute(f"DELETE FROM performances WHERE match_id IN ({placeholders})", match_ids)
    deleted_performances = cur.rowcount
    cur.execute(f"DELETE FROM match_stats_players WHERE match_id IN ({placeholders})", match_ids)
    deleted_match_stats_players = cur.rowcount
    cur.execute(f"DELETE FROM match_players WHERE match_id IN ({placeholders})", match_ids)
    deleted_match_players = cur.rowcount
    cur.execute(f"DELETE FROM matches WHERE match_id IN ({placeholders})", match_ids)
    deleted_matches = cur.rowcount
    conn.commit()

    recompute_champion_metrics(conn)
    return {
        "cutoff_date": cutoff,
        "deleted_matches": deleted_matches,
        "deleted_match_players": deleted_match_players,
        "deleted_match_stats_players": deleted_match_stats_players,
        "deleted_performances": deleted_performances,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Maintenance utilities for grand arena DB.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prune_parser = subparsers.add_parser("prune", help="Prune old rows to a rolling window.")
    prune_parser.add_argument("--db", default=SETTINGS.db_path, help="SQLite DB path")
    prune_parser.add_argument("--keep-days", type=int, default=7, help="Rolling day window to retain")
    prune_parser.add_argument("--today", default=utc_today_iso(), help="UTC date override YYYY-MM-DD")
    prune_parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after prune")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = get_connection(args.db)
    init_db(conn)

    if args.command == "prune":
        today = date.fromisoformat(args.today)
        result = prune_old_matches(conn, keep_days=max(1, int(args.keep_days)), today=today)
        if args.vacuum:
            conn.execute("VACUUM")
            conn.commit()
        print(json.dumps(result, sort_keys=True))
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
