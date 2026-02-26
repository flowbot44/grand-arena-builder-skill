from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .config import SETTINGS
from .db import get_connection, init_db


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def window_start(today: date, days: int) -> date:
    if days <= 1:
        return today
    return today - timedelta(days=days - 1)


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_gzip_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    with gzip.open(path, "wb") as fh:
        fh.write(raw)


def _sha256_hex(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _match_rows_for_date(conn: sqlite3.Connection, match_date: str) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            match_id,
            game_type,
            match_date,
            state,
            is_bye,
            team_won,
            win_type,
            updated_at,
            last_seen_at
        FROM matches
        WHERE match_date = ?
        ORDER BY updated_at ASC, match_id ASC
        """,
        (match_date,),
    ).fetchall()


def _players_for_match(conn: sqlite3.Connection, match_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            match_id,
            moki_id,
            token_id,
            team,
            name,
            class,
            image_url,
            is_champion
        FROM match_players
        WHERE match_id = ?
        ORDER BY team ASC, is_champion DESC, token_id ASC
        """,
        (match_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _stats_for_match(conn: sqlite3.Connection, match_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            match_id,
            token_id,
            team,
            won,
            points,
            eliminations,
            deposits,
            wart_distance
        FROM match_stats_players
        WHERE match_id = ?
        ORDER BY token_id ASC
        """,
        (match_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _performances_for_match(conn: sqlite3.Connection, match_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            performance_id,
            match_id,
            moki_id,
            token_id,
            match_date,
            is_bye,
            win_type,
            deposits,
            eliminations,
            wart_distance,
            updated_at
        FROM performances
        WHERE match_id = ?
        ORDER BY performance_id ASC
        """,
        (match_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _calc_points(deposits: float, eliminations: float, wart_distance: float, won: bool) -> float:
    wart_points = math.floor(wart_distance / 80.0) * 45.0
    return (deposits * 50.0) + (eliminations * 80.0) + wart_points + (300.0 if won else 0.0)


@dataclass
class CumulativeTotals:
    token_id: int
    moki_id: Optional[str]
    games_played_cum: int = 0
    wins_cum: int = 0
    points_cum: float = 0.0
    eliminations_cum: float = 0.0
    deposits_cum: float = 0.0
    wart_distance_cum: float = 0.0

    def as_row(self, as_of_date: str) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "moki_id": self.moki_id,
            "as_of_date": as_of_date,
            "games_played_cum": self.games_played_cum,
            "wins_cum": self.wins_cum,
            "points_cum": round(self.points_cum, 2),
            "eliminations_cum": round(self.eliminations_cum, 2),
            "deposits_cum": round(self.deposits_cum, 2),
            "wart_distance_cum": round(self.wart_distance_cum, 2),
        }


def _scored_rows_for_window(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[sqlite3.Row]:
    return conn.execute(
        """
        WITH perf_avg AS (
            SELECT
                match_id,
                token_id,
                AVG(deposits) AS deposits,
                AVG(eliminations) AS eliminations,
                AVG(wart_distance) AS wart_distance
            FROM performances
            GROUP BY match_id, token_id
        )
        SELECT
            m.match_id,
            m.match_date,
            m.team_won,
            mp.team,
            mp.token_id,
            mp.moki_id,
            COALESCE(p.deposits, msp.deposits, 0) AS deposits,
            COALESCE(p.eliminations, msp.eliminations, 0) AS eliminations,
            COALESCE(p.wart_distance, msp.wart_distance, 0) AS wart_distance
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        LEFT JOIN match_stats_players msp
            ON msp.match_id = m.match_id AND msp.token_id = mp.token_id
        LEFT JOIN perf_avg p
            ON p.match_id = m.match_id AND p.token_id = mp.token_id
        WHERE m.state = 'scored'
          AND m.match_date >= ?
          AND m.match_date <= ?
        ORDER BY m.match_date ASC, m.updated_at ASC, m.match_id ASC, mp.token_id ASC
        """,
        (start_date, end_date),
    ).fetchall()


def export_feed(conn: sqlite3.Connection, *, out_dir: Path, days: int, today: date) -> Dict[str, Any]:
    start = window_start(today, days)
    calendar_dates = [d.isoformat() for d in date_range(start, today)]

    out_dir.mkdir(parents=True, exist_ok=True)
    partitions_dir = out_dir / "partitions"
    cumulative_dir = out_dir / "cumulative"
    partitions_dir.mkdir(parents=True, exist_ok=True)
    cumulative_dir.mkdir(parents=True, exist_ok=True)

    partition_entries: List[Dict[str, Any]] = []
    for day_iso in calendar_dates:
        match_rows = _match_rows_for_date(conn, day_iso)
        payload_matches: List[Dict[str, Any]] = []
        for row in match_rows:
            match_id = row["match_id"]
            payload_matches.append(
                {
                    "match": dict(row),
                    "players": _players_for_match(conn, match_id),
                    "stats_players": _stats_for_match(conn, match_id),
                    "performances": _performances_for_match(conn, match_id),
                }
            )

        rel_path = Path("partitions") / f"raw_matches_{day_iso}.json.gz"
        abs_path = out_dir / rel_path
        _write_gzip_json(abs_path, payload_matches)
        partition_entries.append(
            {
                "date": day_iso,
                "url": rel_path.as_posix(),
                "sha256": _sha256_hex(abs_path),
                "bytes": abs_path.stat().st_size,
                "match_count": len(payload_matches),
            }
        )

    raw_manifest = {
        "generated_at_utc": utc_now_iso(),
        "window_days": days,
        "available_dates": calendar_dates,
        "partitions": partition_entries,
    }
    _write_json(out_dir / "latest.json", raw_manifest)

    scored_rows = _scored_rows_for_window(conn, start.isoformat(), today.isoformat())
    by_date: Dict[str, List[sqlite3.Row]] = {}
    for row in scored_rows:
        by_date.setdefault(row["match_date"], []).append(row)

    cumulative_entries: List[Dict[str, Any]] = []
    running: Dict[int, CumulativeTotals] = {}
    current_rows: List[Dict[str, Any]] = []
    for day_iso in calendar_dates:
        for row in by_date.get(day_iso, []):
            token_id = int(row["token_id"])
            moki_id = row["moki_id"]
            deposits = float(row["deposits"] or 0.0)
            eliminations = float(row["eliminations"] or 0.0)
            wart_distance = float(row["wart_distance"] or 0.0)
            won = row["team_won"] is not None and row["team_won"] == row["team"]

            current = running.get(token_id)
            if current is None:
                current = CumulativeTotals(token_id=token_id, moki_id=moki_id)
                running[token_id] = current
            elif moki_id:
                current.moki_id = moki_id

            current.games_played_cum += 1
            current.wins_cum += 1 if won else 0
            current.points_cum += _calc_points(deposits, eliminations, wart_distance, won)
            current.eliminations_cum += eliminations
            current.deposits_cum += deposits
            current.wart_distance_cum += wart_distance

        day_rows = [running[token_id].as_row(day_iso) for token_id in sorted(running.keys())]
        rel_path = Path("cumulative") / f"daily_totals_{day_iso}.json.gz"
        abs_path = out_dir / rel_path
        _write_gzip_json(abs_path, day_rows)
        cumulative_entries.append(
            {
                "date": day_iso,
                "url": rel_path.as_posix(),
                "sha256": _sha256_hex(abs_path),
                "bytes": abs_path.stat().st_size,
                "player_count": len(day_rows),
            }
        )
        current_rows = day_rows

    current_totals_path = out_dir / "cumulative" / "current_totals.json.gz"
    _write_gzip_json(current_totals_path, current_rows)
    cumulative_manifest = {
        "generated_at_utc": utc_now_iso(),
        "window_days": days,
        "available_dates": calendar_dates,
        "files": cumulative_entries,
        "current_totals": {
            "url": "cumulative/current_totals.json.gz",
            "sha256": _sha256_hex(current_totals_path),
            "bytes": current_totals_path.stat().st_size,
            "player_count": len(current_rows),
        },
    }
    _write_json(out_dir / "cumulative" / "latest.json", cumulative_manifest)

    latest_run = conn.execute(
        """
        SELECT run_id, started_at, finished_at, status, details_json
        FROM ingestion_runs
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    status_payload = {
        "generated_at_utc": utc_now_iso(),
        "window_days": days,
        "window_start": start.isoformat(),
        "window_end": today.isoformat(),
        "raw_dates": calendar_dates,
        "latest_ingestion_run": dict(latest_run) if latest_run else None,
    }
    _write_json(out_dir / "status.json", status_payload)

    return {
        "window_start": start.isoformat(),
        "window_end": today.isoformat(),
        "raw_partitions": len(partition_entries),
        "cumulative_files": len(cumulative_entries),
        "current_player_count": len(current_rows),
        "output_dir": str(out_dir),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export rolling raw/cumulative feeds for static hosting.")
    parser.add_argument("--db", default=SETTINGS.db_path, help="SQLite DB path")
    parser.add_argument("--out", default="exports/data", help="Output directory (e.g. exports/data)")
    parser.add_argument("--days", type=int, default=7, help="Rolling window day count")
    parser.add_argument("--today", default=date.today().isoformat(), help="UTC date override YYYY-MM-DD")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = get_connection(args.db)
    init_db(conn)
    result = export_feed(
        conn,
        out_dir=Path(args.out),
        days=max(1, int(args.days)),
        today=date.fromisoformat(args.today),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
