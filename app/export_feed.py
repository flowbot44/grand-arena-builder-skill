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
from .time_utils import utc_today_iso


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


def _read_json_if_exists(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_gzip_json(path: Path) -> Any:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _moki_totals_entry_from_file(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    count = 0
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            count = len(data)
    return {
        "url": "moki_totals.json",
        "sha256": _sha256_hex(path),
        "bytes": path.stat().st_size,
        "count": count,
    }


def _raw_partition_entry_from_file(day_iso: str, path: Path) -> Dict[str, Any]:
    payload = _read_gzip_json(path)
    match_count = len(payload) if isinstance(payload, list) else 0
    return {
        "date": day_iso,
        "url": (Path("partitions") / f"raw_matches_{day_iso}.json.gz").as_posix(),
        "sha256": _sha256_hex(path),
        "bytes": path.stat().st_size,
        "match_count": match_count,
    }


def _cumulative_entry_from_file(day_iso: str, path: Path) -> Dict[str, Any]:
    payload = _read_gzip_json(path)
    player_count = len(payload) if isinstance(payload, list) else 0
    return {
        "date": day_iso,
        "url": (Path("cumulative") / f"daily_totals_{day_iso}.json.gz").as_posix(),
        "sha256": _sha256_hex(path),
        "bytes": path.stat().st_size,
        "player_count": player_count,
    }


def _support_stats_entry_from_file(path: Path) -> Dict[str, Any]:
    payload = _read_json_if_exists(path) or {}
    player_count = len(payload.get("player_games") or {}) if isinstance(payload, dict) else 0
    champion_count = len(payload.get("champion_games") or {}) if isinstance(payload, dict) else 0
    return {
        "url": "support_stats.json",
        "sha256": _sha256_hex(path),
        "bytes": path.stat().st_size,
        "player_count": player_count,
        "champion_count": champion_count,
    }


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
            mp.is_champion,
            COALESCE(msp.deposits, p.deposits, 0) AS deposits,
            COALESCE(msp.eliminations, p.eliminations, 0) AS eliminations,
            COALESCE(msp.wart_distance, p.wart_distance, 0) AS wart_distance
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


def export_feed(
    conn: sqlite3.Connection,
    *,
    out_dir: Path,
    days: int,
    today: date,
    lookahead_days: int = 0,
    mutable_days_back: Optional[int] = None,
    mutable_days_forward: Optional[int] = None,
    cumulative_mutable_days_back: Optional[int] = None,
    raw_refresh_start: Optional[date] = None,
    raw_refresh_end: Optional[date] = None,
    export_cumulative: bool = True,
) -> Dict[str, Any]:
    start = window_start(today, days)
    raw_end = today + timedelta(days=max(0, int(lookahead_days)))
    raw_calendar_dates = [d.isoformat() for d in date_range(start, raw_end)]
    cumulative_calendar_dates = [d.isoformat() for d in date_range(start, today)]

    out_dir.mkdir(parents=True, exist_ok=True)
    partitions_dir = out_dir / "partitions"
    cumulative_dir = out_dir / "cumulative"
    partitions_dir.mkdir(parents=True, exist_ok=True)
    cumulative_dir.mkdir(parents=True, exist_ok=True)

    prior_raw_manifest = _read_json_if_exists(out_dir / "latest.json") or {}
    prior_raw_by_date = {
        str(entry.get("date")): entry
        for entry in prior_raw_manifest.get("partitions", [])
        if isinstance(entry, dict) and entry.get("date")
    }

    partition_entries: List[Dict[str, Any]] = []
    raw_full_refresh = mutable_days_back is None and mutable_days_forward is None
    raw_mutable_back = max(0, int(mutable_days_back or 0))
    raw_mutable_forward = max(0, int(mutable_days_forward if mutable_days_forward is not None else lookahead_days))
    raw_mutable_start = max(start, today - timedelta(days=raw_mutable_back))
    raw_mutable_end = today + timedelta(days=raw_mutable_forward)
    explicit_raw_refresh = raw_refresh_start is not None or raw_refresh_end is not None
    if raw_refresh_start is None:
        raw_refresh_start = start
    if raw_refresh_end is None:
        raw_refresh_end = raw_end

    for day_iso in raw_calendar_dates:
        day_value = date.fromisoformat(day_iso)
        should_refresh = (
            raw_full_refresh
            if not explicit_raw_refresh
            else raw_refresh_start <= day_value <= raw_refresh_end
        )
        if not explicit_raw_refresh and not raw_full_refresh:
            should_refresh = raw_mutable_start <= day_value <= raw_mutable_end
        rel_path = Path("partitions") / f"raw_matches_{day_iso}.json.gz"
        abs_path = out_dir / rel_path

        if not should_refresh:
            prior_entry = prior_raw_by_date.get(day_iso)
            if prior_entry and abs_path.exists():
                partition_entries.append(dict(prior_entry))
                continue
            if abs_path.exists():
                partition_entries.append(_raw_partition_entry_from_file(day_iso, abs_path))
                continue
            # If a preserved partition is missing, rebuild it from SQLite rather than
            # failing the whole export. This keeps scheduled publishes resilient when
            # the prior feed artifact is unavailable or incomplete.

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
        "lookahead_days": max(0, int(lookahead_days)),
        "available_dates": raw_calendar_dates,
        "partitions": partition_entries,
    }
    moki_totals_path = out_dir / "moki_totals.json"
    if moki_totals_path.exists():
        raw_manifest["moki_totals"] = _moki_totals_entry_from_file(moki_totals_path)
    _write_json(out_dir / "latest.json", raw_manifest)

    cumulative_files_count = 0
    current_player_count = 0
    if not export_cumulative:
        status_payload = {
            "generated_at_utc": utc_now_iso(),
            "window_days": days,
            "lookahead_days": max(0, int(lookahead_days)),
            "window_start": start.isoformat(),
            "window_end": raw_end.isoformat(),
            "cumulative_window_end": today.isoformat(),
            "raw_dates": raw_calendar_dates,
            "latest_ingestion_run": dict(
                conn.execute(
                    """
                    SELECT run_id, started_at, finished_at, status, details_json
                    FROM ingestion_runs
                    ORDER BY run_id DESC
                    LIMIT 1
                    """
                ).fetchone()
                or {}
            ) or None,
        }
        _write_json(out_dir / "status.json", status_payload)
        return {
            "window_start": start.isoformat(),
            "window_end": raw_end.isoformat(),
            "raw_partitions": len(partition_entries),
            "cumulative_files": cumulative_files_count,
            "current_player_count": current_player_count,
            "output_dir": str(out_dir),
        }

    prior_cumulative_manifest = _read_json_if_exists(out_dir / "cumulative" / "latest.json") or {}
    prior_cumulative_by_date = {
        str(entry.get("date")): entry
        for entry in prior_cumulative_manifest.get("files", [])
        if isinstance(entry, dict) and entry.get("date")
    }

    cumulative_full_refresh = cumulative_mutable_days_back is None
    cumulative_back = max(0, int(cumulative_mutable_days_back if cumulative_mutable_days_back is not None else 0))
    cumulative_mutable_start = max(start, today - timedelta(days=cumulative_back))

    running: Dict[int, CumulativeTotals] = {}
    cumulative_entries: List[Dict[str, Any]] = []
    current_rows: List[Dict[str, Any]] = []
    cumulative_compute_start = start

    if not cumulative_full_refresh and cumulative_mutable_start > start:
        seed_day = cumulative_mutable_start - timedelta(days=1)
        seed_path = out_dir / "cumulative" / f"daily_totals_{seed_day.isoformat()}.json.gz"
        reuse_cumulative_seed = False
        if seed_path.exists():
            try:
                seed_rows = _read_gzip_json(seed_path)
                if isinstance(seed_rows, list):
                    for row in seed_rows:
                        token_id = int(row["token_id"])
                        running[token_id] = CumulativeTotals(
                            token_id=token_id,
                            moki_id=row.get("moki_id"),
                            games_played_cum=int(row.get("games_played_cum", 0) or 0),
                            wins_cum=int(row.get("wins_cum", 0) or 0),
                            points_cum=float(row.get("points_cum", 0.0) or 0.0),
                            eliminations_cum=float(row.get("eliminations_cum", 0.0) or 0.0),
                            deposits_cum=float(row.get("deposits_cum", 0.0) or 0.0),
                            wart_distance_cum=float(row.get("wart_distance_cum", 0.0) or 0.0),
                        )
                    for static_day in date_range(start, seed_day):
                        day_iso = static_day.isoformat()
                        rel_path = Path("cumulative") / f"daily_totals_{day_iso}.json.gz"
                        abs_path = out_dir / rel_path
                        prior_entry = prior_cumulative_by_date.get(day_iso)
                        if prior_entry and abs_path.exists():
                            cumulative_entries.append(dict(prior_entry))
                        elif abs_path.exists():
                            cumulative_entries.append(_cumulative_entry_from_file(day_iso, abs_path))
                        else:
                            raise FileNotFoundError(
                                f"Missing preserved cumulative file for immutable date {day_iso}: {abs_path}"
                            )
                    if running:
                        cumulative_compute_start = cumulative_mutable_start
                        reuse_cumulative_seed = True
            except Exception:
                running.clear()
                cumulative_entries = []
        if not reuse_cumulative_seed:
            running.clear()
            cumulative_entries = []
            cumulative_compute_start = start

    scored_rows = _scored_rows_for_window(conn, cumulative_compute_start.isoformat(), today.isoformat())
    by_date: Dict[str, List[sqlite3.Row]] = {}
    for row in scored_rows:
        by_date.setdefault(row["match_date"], []).append(row)

    for day_value in date_range(cumulative_compute_start, today):
        day_iso = day_value.isoformat()
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
    support_stats_path = out_dir / "support_stats.json"
    player_games: Dict[int, Dict[str, int]] = {}
    champion_games: Dict[int, Dict[str, int]] = {}
    for rows in by_date.values():
        for row in rows:
            token_id = int(row["token_id"])
            won = row["team_won"] is not None and row["team_won"] == row["team"]
            stats = player_games.setdefault(token_id, {"games": 0, "wins": 0})
            stats["games"] += 1
            stats["wins"] += 1 if won else 0

            if int(row["is_champion"]) == 1:
                champ_stats = champion_games.setdefault(token_id, {"games": 0, "wins": 0})
                champ_stats["games"] += 1
                champ_stats["wins"] += 1 if won else 0

    _write_json(
        support_stats_path,
        {
            "generated_at_utc": utc_now_iso(),
            "player_games": {str(token_id): stats for token_id, stats in sorted(player_games.items())},
            "champion_games": {str(token_id): stats for token_id, stats in sorted(champion_games.items())},
        },
    )
    cumulative_manifest = {
        "generated_at_utc": utc_now_iso(),
        "window_days": days,
        "available_dates": cumulative_calendar_dates,
        "files": cumulative_entries,
        "current_totals": {
            "url": "cumulative/current_totals.json.gz",
            "sha256": _sha256_hex(current_totals_path),
            "bytes": current_totals_path.stat().st_size,
            "player_count": len(current_rows),
        },
        "support_stats": _support_stats_entry_from_file(support_stats_path),
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
        "lookahead_days": max(0, int(lookahead_days)),
        "window_start": start.isoformat(),
        "window_end": raw_end.isoformat(),
        "cumulative_window_end": today.isoformat(),
        "raw_dates": raw_calendar_dates,
        "latest_ingestion_run": dict(latest_run) if latest_run else None,
    }
    _write_json(out_dir / "status.json", status_payload)

    return {
        "window_start": start.isoformat(),
        "window_end": raw_end.isoformat(),
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
    parser.add_argument("--lookahead-days", type=int, default=0, help="Additional future days for raw partition export")
    parser.add_argument(
        "--mutable-days-back",
        type=int,
        default=None,
        help="If set, only refresh raw partitions from today-N days onward; older partition files are reused.",
    )
    parser.add_argument(
        "--mutable-days-forward",
        type=int,
        default=None,
        help="If set, only refresh raw partitions up to today+N future days.",
    )
    parser.add_argument(
        "--cumulative-mutable-days-back",
        type=int,
        default=None,
        help="If set, only refresh cumulative files from today-N days onward; older cumulative files are reused.",
    )
    parser.add_argument("--raw-refresh-start", default=None, help="Explicit raw partition refresh start date YYYY-MM-DD")
    parser.add_argument("--raw-refresh-end", default=None, help="Explicit raw partition refresh end date YYYY-MM-DD")
    parser.add_argument("--skip-cumulative", action="store_true", help="Skip rebuilding cumulative outputs during export")
    parser.add_argument("--today", default=utc_today_iso(), help="UTC date override YYYY-MM-DD")
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
        lookahead_days=max(0, int(args.lookahead_days)),
        mutable_days_back=args.mutable_days_back,
        mutable_days_forward=args.mutable_days_forward,
        cumulative_mutable_days_back=args.cumulative_mutable_days_back,
        raw_refresh_start=date.fromisoformat(args.raw_refresh_start) if args.raw_refresh_start else None,
        raw_refresh_end=date.fromisoformat(args.raw_refresh_end) if args.raw_refresh_end else None,
        export_cumulative=not args.skip_cumulative,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
