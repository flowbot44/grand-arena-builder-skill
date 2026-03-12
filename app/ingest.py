from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from .analytics import recompute_champion_metrics
from .api_client import GrandArenaClient, RateLimiter
from .config import SETTINGS
from .db import get_connection, init_db, transaction
from .time_utils import utc_today, utc_today_iso


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def compute_window_dates(today: date, lookbehind_days: int = 2, lookahead_days: int = 2) -> List[date]:
    start = today - timedelta(days=lookbehind_days)
    end = today + timedelta(days=lookahead_days)
    return [d for d in date_range(start, end)]


@dataclass
class SyncResult:
    matches_seen: int = 0
    matches_updated: int = 0
    enrich_candidates: int = 0
    stats_upserts: int = 0
    perf_upserts: int = 0


class IngestionService:
    def __init__(self, conn: sqlite3.Connection, client: GrandArenaClient, champions_path: str = "champions.json") -> None:
        self.conn = conn
        self.client = client
        self.champions_path = champions_path
        self._champion_token_ids: set[int] = set()
        self._force_full_refresh = False

    def _attach_api_telemetry(self, details: Dict[str, Any]) -> None:
        if hasattr(self.client, "telemetry_snapshot"):
            details["api"] = self.client.telemetry_snapshot()

    def seed_champions(self) -> int:
        raw = self._read_champions_file()
        champions_hash = hashlib.sha256(raw).hexdigest()
        stored_hash = self._load_cursor("champions:sha256")
        champion_count = self.conn.execute("SELECT COUNT(*) AS c FROM champions").fetchone()["c"]
        if stored_hash == champions_hash and champion_count > 0:
            rows = self.conn.execute("SELECT token_id FROM champions").fetchall()
            self._champion_token_ids = {int(r["token_id"]) for r in rows}
            return len(self._champion_token_ids)

        champions = json.loads(raw.decode("utf-8"))

        now = utc_now_iso()
        self._champion_token_ids = {int(row["id"]) for row in champions}
        with transaction(self.conn):
            self.conn.executemany(
                """
                INSERT INTO champions (token_id, name, traits_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(token_id) DO UPDATE SET
                    name = excluded.name,
                    traits_json = excluded.traits_json,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        int(row["id"]),
                        row["name"],
                        json.dumps(row.get("traits", []), separators=(",", ":")),
                        now,
                    )
                    for row in champions
                ],
            )
            champion_ids = tuple(int(row["id"]) for row in champions)
            if champion_ids:
                self.conn.execute(
                    "DELETE FROM champions WHERE token_id NOT IN ({})".format(",".join("?" for _ in champion_ids)),
                    champion_ids,
                )
            else:
                self.conn.execute("DELETE FROM champions")
        self._store_cursor("champions:sha256", champions_hash)
        return len(champions)

    def _read_champions_file(self) -> bytes:
        with open(self.champions_path, "rb") as fh:
            return fh.read()

    def _is_champion(self, token_id: int) -> int:
        if self._champion_token_ids:
            return 1 if token_id in self._champion_token_ids else 0
        row = self.conn.execute("SELECT 1 FROM champions WHERE token_id = ?", (token_id,)).fetchone()
        return 1 if row else 0

    def _match_includes_champion(self, match: Dict[str, Any]) -> bool:
        if not self._champion_token_ids:
            rows = self.conn.execute("SELECT token_id FROM champions").fetchall()
            self._champion_token_ids = {int(r["token_id"]) for r in rows}
        for player in match.get("players", []):
            token_id = player.get("tokenId")
            if token_id is not None and int(token_id) in self._champion_token_ids:
                return True
        return False

    def _stats_present(self, match_id: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM match_stats_players WHERE match_id = ? LIMIT 1", (match_id,)).fetchone()
        return row is not None

    def _performances_present(self, match_id: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM performances WHERE match_id = ? LIMIT 1", (match_id,)).fetchone()
        return row is not None

    def _load_cursor(self, key: str) -> str:
        row = self.conn.execute("SELECT value FROM api_cursors WHERE key = ?", (key,)).fetchone()
        if not row:
            return ""
        return str(row["value"] or "")

    def _upsert_match(self, match: Dict[str, Any], now: str) -> bool:
        result = match.get("result") or {}
        updated = match.get("updatedAt") or now
        current_state = match.get("state") or "scheduled"
        with transaction(self.conn):
            cursor = self.conn.execute(
                """
                INSERT INTO matches (
                    match_id, game_type, match_date, state, is_bye,
                    team_won, win_type, updated_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    game_type = excluded.game_type,
                    match_date = excluded.match_date,
                    state = excluded.state,
                    is_bye = excluded.is_bye,
                    team_won = excluded.team_won,
                    win_type = excluded.win_type,
                    updated_at = excluded.updated_at,
                    last_seen_at = excluded.last_seen_at
                WHERE matches.state != excluded.state
                   OR matches.updated_at != excluded.updated_at
                """,
                (
                    match["id"],
                    match.get("gameType", "mokiMayhem"),
                    match.get("matchDate") or "",
                    current_state,
                    1 if match.get("isBye") else 0,
                    result.get("teamWon"),
                    result.get("winType"),
                    updated,
                    now,
                ),
            )
            changed = cursor.rowcount > 0
            if not changed:
                self.conn.execute("UPDATE matches SET last_seen_at = ? WHERE match_id = ?", (now, match["id"]))
                return False

            self.conn.execute("DELETE FROM match_players WHERE match_id = ?", (match["id"],))
            player_rows = []
            for player in match.get("players", []):
                token_id = int(player.get("tokenId") or 0)
                if token_id == 0:
                    continue
                player_rows.append(
                    (
                        match["id"],
                        player.get("mokiId") or "",
                        token_id,
                        int(player.get("team") or 0),
                        player.get("name"),
                        player.get("class"),
                        player.get("imageUrl"),
                        self._is_champion(token_id),
                    )
                )
            if player_rows:
                self.conn.executemany(
                    """
                    INSERT INTO match_players (
                        match_id, moki_id, token_id, team, name, class, image_url, is_champion
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    player_rows,
                )

        return True

    def _store_cursor(self, key: str, value: str) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO api_cursors (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
        self.conn.commit()

    def sync_match_date(self, match_date: date) -> SyncResult:
        result = SyncResult()
        enrich_ids: set[str] = set()
        cursor_key = f"matches:{match_date.isoformat()}"
        use_cursor_cutoff = (match_date < utc_today()) and not self._force_full_refresh
        previous_cursor = self._load_cursor(cursor_key) if use_cursor_cutoff else ""
        max_updated_at = previous_cursor
        today_iso = utc_now_iso()

        page = 1
        pages = 1
        stop_paging = False
        while page <= pages:
            payload = self.client.list_matches(
                match_date.isoformat(),
                page=page,
                limit=SETTINGS.api_page_limit,
                order="desc",
            )
            items = payload.get("data", [])
            pagination = payload.get("pagination", {})
            pages = int(pagination.get("pages") or 1)
            page = int(pagination.get("page") or page)

            for match in items:
                source_updated = match.get("updatedAt") or ""
                if use_cursor_cutoff and previous_cursor and source_updated and source_updated < previous_cursor:
                    stop_paging = True
                    break

                result.matches_seen += 1
                if SETTINGS.champion_only_matches and not self._match_includes_champion(match):
                    continue
                changed = self._upsert_match(match, now=today_iso)
                if changed:
                    result.matches_updated += 1

                if source_updated > max_updated_at:
                    max_updated_at = source_updated

                state = match.get("state")
                if state == "scored":
                    match_id = match["id"]
                    if changed or not self._stats_present(match_id) or not self._performances_present(match_id):
                        enrich_ids.add(match_id)

            if stop_paging:
                break
            page += 1

        for match_id in enrich_ids:
            stats_upserted = self.enrich_match_stats(match_id)
            perf_upserted = 0
            if SETTINGS.fetch_match_performances:
                perf_upserted = self.enrich_match_performances(match_id)
            result.stats_upserts += stats_upserted
            result.perf_upserts += perf_upserted

        result.enrich_candidates = len(enrich_ids)
        self._store_cursor(cursor_key, max_updated_at)
        return result

    def enrich_match_stats(self, match_id: str) -> int:
        payload = self.client.get_match_stats(match_id)
        data = payload.get("data", {})

        team_won = data.get("teamWon")
        win_type = data.get("winType")
        state = data.get("state")
        teams = data.get("teams") or []
        upserts = 0
        with transaction(self.conn):
            if state:
                self.conn.execute(
                    "UPDATE matches SET state = ?, team_won = ?, win_type = ? WHERE match_id = ?",
                    (state, team_won, win_type, match_id),
                )
            stat_rows = []
            for team in teams:
                for player in team.get("players", []):
                    token_id = player.get("tokenId")
                    if token_id is None:
                        continue
                    stat_rows.append(
                        (
                            match_id,
                            int(token_id),
                            int(player.get("team") or team.get("teamNumber") or 0),
                            1 if player.get("won") else 0,
                            player.get("points"),
                            player.get("eliminations"),
                            player.get("deposits"),
                            player.get("wartDistance"),
                        )
                    )
                    upserts += 1
            if stat_rows:
                self.conn.executemany(
                    """
                    INSERT INTO match_stats_players (
                        match_id, token_id, team, won, points,
                        eliminations, deposits, wart_distance
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(match_id, token_id) DO UPDATE SET
                        team = excluded.team,
                        won = excluded.won,
                        points = excluded.points,
                        eliminations = excluded.eliminations,
                        deposits = excluded.deposits,
                        wart_distance = excluded.wart_distance
                    """,
                    stat_rows,
                )
        return upserts

    def enrich_match_performances(self, match_id: str) -> int:
        page = 1
        pages = 1
        upserts = 0

        while page <= pages:
            payload = self.client.get_match_performances(match_id, page=page, limit=SETTINGS.api_page_limit)
            root = payload.get("data", {})
            performances = root.get("performances") or []
            pagination = payload.get("pagination", {})
            pages = int(pagination.get("pages") or 1)
            page = int(pagination.get("page") or page)

            with transaction(self.conn):
                perf_rows = []
                for perf in performances:
                    results = perf.get("results") or {}
                    perf_rows.append(
                        (
                            perf.get("id"),
                            perf.get("matchId") or match_id,
                            perf.get("mokiId") or "",
                            perf.get("tokenId"),
                            perf.get("matchDate") or "",
                            1 if perf.get("isBye") else 0,
                            results.get("winType"),
                            results.get("deposits"),
                            results.get("eliminations"),
                            results.get("wartDistance"),
                            perf.get("updatedAt") or utc_now_iso(),
                        )
                    )
                    upserts += 1
                if perf_rows:
                    self.conn.executemany(
                        """
                        INSERT INTO performances (
                            performance_id, match_id, moki_id, token_id, match_date,
                            is_bye, win_type, deposits, eliminations, wart_distance, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(performance_id) DO UPDATE SET
                            match_id = excluded.match_id,
                            moki_id = excluded.moki_id,
                            token_id = excluded.token_id,
                            match_date = excluded.match_date,
                            is_bye = excluded.is_bye,
                            win_type = excluded.win_type,
                            deposits = excluded.deposits,
                            eliminations = excluded.eliminations,
                            wart_distance = excluded.wart_distance,
                            updated_at = excluded.updated_at
                        """,
                        perf_rows,
                    )

            page += 1

        return upserts

    def run_date_range(
        self,
        start: date,
        end: date,
        *,
        force_full_refresh: bool = False,
        recompute_metrics_at_end: bool = True,
    ) -> Dict[str, Any]:
        started_at = utc_now_iso()
        run_id = self.conn.execute(
            "INSERT INTO ingestion_runs (started_at, status) VALUES (?, ?)",
            (started_at, "running"),
        ).lastrowid
        self.conn.commit()

        details: Dict[str, Any] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "by_date": {},
            "seeded_champions": self.seed_champions(),
            "recomputed_metrics": False,
        }
        should_recompute_metrics = False

        try:
            self._force_full_refresh = force_full_refresh
            for day in date_range(start, end):
                day_result = self.sync_match_date(day)
                details["by_date"][day.isoformat()] = day_result.__dict__
                should_recompute_metrics = should_recompute_metrics or any(
                    (
                        day_result.matches_updated,
                        day_result.stats_upserts,
                        day_result.perf_upserts,
                    )
                )

            if recompute_metrics_at_end and should_recompute_metrics:
                recompute_champion_metrics(self.conn)
                details["recomputed_metrics"] = True
            self._attach_api_telemetry(details)
            finished_at = utc_now_iso()
            self.conn.execute(
                "UPDATE ingestion_runs SET finished_at = ?, status = ?, details_json = ? WHERE run_id = ?",
                (finished_at, "success", json.dumps(details, separators=(",", ":")), run_id),
            )
            self.conn.commit()
            return details
        except Exception as exc:
            finished_at = utc_now_iso()
            details["error"] = str(exc)
            self._attach_api_telemetry(details)
            self.conn.execute(
                "UPDATE ingestion_runs SET finished_at = ?, status = ?, details_json = ? WHERE run_id = ?",
                (finished_at, "failed", json.dumps(details, separators=(",", ":")), run_id),
            )
            self.conn.commit()
            raise
        finally:
            self._force_full_refresh = False

    def run_enrichment_only(
        self,
        *,
        start: Optional[date] = None,
        end: Optional[date] = None,
        max_matches: Optional[int] = None,
    ) -> Dict[str, Any]:
        started_at = utc_now_iso()
        run_id = self.conn.execute(
            "INSERT INTO ingestion_runs (started_at, status) VALUES (?, ?)",
            (started_at, "running"),
        ).lastrowid
        self.conn.commit()

        self.seed_champions()
        where = [
            "m.state = 'scored'",
            "EXISTS (SELECT 1 FROM match_players mp WHERE mp.match_id = m.match_id AND mp.is_champion = 1)",
        ]
        if SETTINGS.fetch_match_performances:
            where.append(
                "("
                "NOT EXISTS (SELECT 1 FROM match_stats_players s WHERE s.match_id = m.match_id)"
                " OR "
                "NOT EXISTS (SELECT 1 FROM performances p WHERE p.match_id = m.match_id)"
                ")"
            )
        else:
            where.append("NOT EXISTS (SELECT 1 FROM match_stats_players s WHERE s.match_id = m.match_id)")
        params: List[Any] = []
        if start is not None:
            where.append("m.match_date >= ?")
            params.append(start.isoformat())
        if end is not None:
            where.append("m.match_date <= ?")
            params.append(end.isoformat())

        limit_sql = ""
        if max_matches is not None and max_matches > 0:
            limit_sql = f" LIMIT {int(max_matches)}"

        rows = self.conn.execute(
            f"""
            SELECT m.match_id
            FROM matches m
            WHERE {" AND ".join(where)}
            ORDER BY m.match_date ASC, m.updated_at ASC
            {limit_sql}
            """
            ,
            params,
        ).fetchall()

        stats_upserts = 0
        perf_upserts = 0
        processed = 0
        for row in rows:
            match_id = row["match_id"]
            stats_upserts += self.enrich_match_stats(match_id)
            if SETTINGS.fetch_match_performances:
                perf_upserts += self.enrich_match_performances(match_id)
            processed += 1

        if processed > 0:
            recompute_champion_metrics(self.conn)
        details = {
            "mode": "enrich-only",
            "start": start.isoformat() if start is not None else None,
            "end": end.isoformat() if end is not None else None,
            "processed_matches": processed,
            "stats_upserts": stats_upserts,
            "perf_upserts": perf_upserts,
            "recomputed_metrics": processed > 0,
        }
        self._attach_api_telemetry(details)
        finished_at = utc_now_iso()
        self.conn.execute(
            "UPDATE ingestion_runs SET finished_at = ?, status = ?, details_json = ? WHERE run_id = ?",
            (finished_at, "success", json.dumps(details, separators=(",", ":")), run_id),
        )
        self.conn.commit()
        return details


def build_client() -> GrandArenaClient:
    limiter = RateLimiter(
        max_per_minute=SETTINGS.request_limit_per_minute,
        min_interval_seconds=SETTINGS.min_request_interval_seconds,
    )
    return GrandArenaClient(
        base_url=SETTINGS.api_base_url,
        api_key=SETTINGS.api_key,
        rate_limiter=limiter,
        timeout_seconds=SETTINGS.api_timeout_seconds,
        retries=SETTINGS.api_retries,
    )


def run_backfill(
    db_path: str,
    start: date,
    end: date,
    champions_path: str,
    *,
    recompute_metrics_at_end: bool = True,
) -> Dict[str, Any]:
    conn = get_connection(db_path)
    init_db(conn)
    service = IngestionService(conn, build_client(), champions_path=champions_path)
    return service.run_date_range(start, end, force_full_refresh=True, recompute_metrics_at_end=recompute_metrics_at_end)


def run_hourly(db_path: str, today: date, champions_path: str) -> Dict[str, Any]:
    start = today - timedelta(days=SETTINGS.lookbehind_days)
    end = today + timedelta(days=SETTINGS.lookahead_days)
    conn = get_connection(db_path)
    init_db(conn)
    service = IngestionService(conn, build_client(), champions_path=champions_path)
    details = service.run_date_range(start, end)
    details["window_start"] = start.isoformat()
    details["window_end"] = end.isoformat()
    details["lookahead_days"] = SETTINGS.lookahead_days
    return details


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Grand Arena ingest and sync runner")
    parser.add_argument("command", choices=["backfill", "hourly", "enrich-only"], help="Run mode")
    parser.add_argument("--db", default=SETTINGS.db_path, help="SQLite DB path")
    parser.add_argument("--champions", default=SETTINGS.champions_path, help="Path to champions.json")
    parser.add_argument("--from", dest="from_date", default=SETTINGS.backfill_start_default.isoformat(), help="Backfill start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=utc_today_iso(), help="Backfill end date YYYY-MM-DD")
    parser.add_argument("--today", dest="today", default=utc_today_iso(), help="Override today for hourly window")
    parser.add_argument("--max-matches", dest="max_matches", type=int, default=0, help="Max matches to enrich in enrich-only mode (0 = no limit)")
    parser.add_argument(
        "--skip-metrics-recompute",
        action="store_true",
        help="Skip recomputing champion_metrics at the end of the run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "backfill":
        details = run_backfill(
            args.db,
            parse_date(args.from_date),
            parse_date(args.to_date),
            args.champions,
            recompute_metrics_at_end=not args.skip_metrics_recompute,
        )
    elif args.command == "enrich-only":
        conn = get_connection(args.db)
        init_db(conn)
        service = IngestionService(conn, build_client(), champions_path=args.champions)
        max_matches = args.max_matches if args.max_matches > 0 else None
        details = service.run_enrichment_only(
            start=parse_date(args.from_date) if args.from_date else None,
            end=parse_date(args.to_date) if args.to_date else None,
            max_matches=max_matches,
        )
    else:
        details = run_hourly(args.db, parse_date(args.today), args.champions)
    print(json.dumps(details, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
