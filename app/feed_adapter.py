from __future__ import annotations

import gzip
import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import urlopen

from .time_utils import utc_today

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FeedUnavailableError(RuntimeError):
    pass


class FeedFormatError(ValueError):
    pass


@dataclass
class CacheEntry:
    value: Any
    fetched_at: float
    expires_at: float


@dataclass
class FeedMeta:
    data_generated_at: Optional[str]
    cache_age_seconds: int
    stale_data: bool

    def as_headers(self) -> Dict[str, str]:
        return {
            "X-Data-Generated-At": self.data_generated_at or "",
            "X-Cache-Age-Seconds": str(self.cache_age_seconds),
            "X-Stale-Data": "true" if self.stale_data else "false",
        }

    def as_body(self) -> Dict[str, Any]:
        return {
            "data_generated_at": self.data_generated_at,
            "cache_age_seconds": self.cache_age_seconds,
            "stale_data": self.stale_data,
        }


def _validate_latest_manifest(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise FeedFormatError("latest manifest is not an object")
    for key in ("generated_at_utc", "window_days", "available_dates", "partitions"):
        if key not in payload:
            raise FeedFormatError(f"latest manifest missing key: {key}")
    if not isinstance(payload["available_dates"], list):
        raise FeedFormatError("available_dates must be an array")
    if not isinstance(payload["partitions"], list):
        raise FeedFormatError("partitions must be an array")
    for idx, part in enumerate(payload["partitions"]):
        if not isinstance(part, dict):
            raise FeedFormatError(f"partition[{idx}] must be an object")
        for key in ("date", "url", "sha256", "bytes", "match_count"):
            if key not in part:
                raise FeedFormatError(f"partition[{idx}] missing key: {key}")
    return payload


def _validate_status(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise FeedFormatError("status is not an object")
    for key in ("generated_at_utc", "window_days", "window_start", "window_end", "raw_dates"):
        if key not in payload:
            raise FeedFormatError(f"status missing key: {key}")
    return payload


def _validate_partition(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        raise FeedFormatError("partition payload must be an array")
    return payload


def _validate_current_totals(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        raise FeedFormatError("current totals payload must be an array")
    return payload


def _validate_moki_totals(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise FeedFormatError("moki totals payload must be an object")
    data = payload.get("data")
    if not isinstance(data, list):
        raise FeedFormatError("moki totals payload missing data array")
    return payload


def _validate_support_stats(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise FeedFormatError("support stats payload must be an object")
    if not isinstance(payload.get("player_games"), dict):
        raise FeedFormatError("support stats missing player_games")
    if not isinstance(payload.get("champion_games"), dict):
        raise FeedFormatError("support stats missing champion_games")
    return payload


class FeedAdapter:
    def __init__(
        self,
        *,
        base_url: str,
        ttl_seconds: int = 600,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.ttl_seconds = max(10, int(ttl_seconds))
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self._cache: Dict[str, CacheEntry] = {}

    def _fetch_url(self, url: str, *, gzip_json: bool) -> Any:
        started = time.monotonic()
        with urlopen(url, timeout=self.timeout_seconds) as resp:  # nosec B310
            raw = resp.read()
        latency_ms = int((time.monotonic() - started) * 1000)
        logger.info("feed_fetch url=%s latency_ms=%s", url, latency_ms)
        if gzip_json:
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))

    def _cached_fetch(
        self,
        *,
        cache_key: str,
        url: str,
        gzip_json: bool,
        validator: Callable[[Any], Any],
        generated_at_extractor: Callable[[Any], Optional[str]],
    ) -> Tuple[Any, FeedMeta]:
        now = time.time()
        entry = self._cache.get(cache_key)
        if entry and entry.expires_at > now:
            value = entry.value
            return (
                value,
                FeedMeta(
                    data_generated_at=generated_at_extractor(value),
                    cache_age_seconds=max(0, int(now - entry.fetched_at)),
                    stale_data=False,
                ),
            )

        try:
            raw = self._fetch_url(url, gzip_json=gzip_json)
            value = validator(raw)
            self._cache[cache_key] = CacheEntry(
                value=value,
                fetched_at=now,
                expires_at=now + self.ttl_seconds,
            )
            logger.info("feed_cache_refresh key=%s status=ok", cache_key)
            return (
                value,
                FeedMeta(
                    data_generated_at=generated_at_extractor(value),
                    cache_age_seconds=0,
                    stale_data=False,
                ),
            )
        except (URLError, TimeoutError, OSError, json.JSONDecodeError, FeedFormatError) as exc:
            if entry is not None:
                value = entry.value
                logger.warning("feed_cache_refresh key=%s status=stale_fallback error=%s", cache_key, exc)
                return (
                    value,
                    FeedMeta(
                        data_generated_at=generated_at_extractor(value),
                        cache_age_seconds=max(0, int(now - entry.fetched_at)),
                        stale_data=True,
                    ),
                )
            raise FeedUnavailableError(
                f"Feed fetch failed for {cache_key}. Retry shortly. Error: {exc}"
            ) from exc

    def get_latest_manifest(self) -> Tuple[Dict[str, Any], FeedMeta]:
        return self._cached_fetch(
            cache_key="latest_manifest",
            url=f"{self.base_url}/latest.json",
            gzip_json=False,
            validator=_validate_latest_manifest,
            generated_at_extractor=lambda d: d.get("generated_at_utc"),
        )

    def get_status(self) -> Tuple[Dict[str, Any], FeedMeta]:
        return self._cached_fetch(
            cache_key="status_manifest",
            url=f"{self.base_url}/status.json",
            gzip_json=False,
            validator=_validate_status,
            generated_at_extractor=lambda d: d.get("generated_at_utc"),
        )

    def get_partition_by_date(self, day_iso: str) -> Tuple[List[Dict[str, Any]], FeedMeta]:
        latest, latest_meta = self.get_latest_manifest()
        part = None
        for candidate in latest.get("partitions", []):
            if str(candidate.get("date")) == day_iso:
                part = candidate
                break
        if part is None:
            raise FeedUnavailableError(f"No partition available for date {day_iso}")
        rel_url = str(part["url"])
        return self._cached_fetch(
            cache_key=f"raw_partition_{day_iso}",
            url=urljoin(f"{self.base_url}/", rel_url),
            gzip_json=True,
            validator=_validate_partition,
            generated_at_extractor=lambda _d: latest_meta.data_generated_at,
        )

    def get_current_totals(self) -> Tuple[List[Dict[str, Any]], FeedMeta]:
        cumulative_latest, cumulative_meta = self._cached_fetch(
            cache_key="cumulative_latest_manifest",
            url=f"{self.base_url}/cumulative/latest.json",
            gzip_json=False,
            validator=lambda d: d if isinstance(d, dict) and "current_totals" in d else (_ for _ in ()).throw(
                FeedFormatError("cumulative latest missing current_totals")
            ),
            generated_at_extractor=lambda d: d.get("generated_at_utc"),
        )
        rel_url = str(cumulative_latest["current_totals"]["url"])
        rows, meta = self._cached_fetch(
            cache_key="cumulative_current",
            url=urljoin(f"{self.base_url}/", rel_url),
            gzip_json=True,
            validator=_validate_current_totals,
            generated_at_extractor=lambda _d: cumulative_meta.data_generated_at,
        )
        return rows, meta

    def get_moki_totals(self) -> Tuple[Dict[str, Any], FeedMeta]:
        latest, latest_meta = self.get_latest_manifest()
        moki_totals_meta = latest.get("moki_totals") if isinstance(latest, dict) else None
        rel_url = "moki_totals.json"
        if isinstance(moki_totals_meta, dict) and moki_totals_meta.get("url"):
            rel_url = str(moki_totals_meta["url"])
        payload, meta = self._cached_fetch(
            cache_key="moki_totals",
            url=urljoin(f"{self.base_url}/", rel_url),
            gzip_json=False,
            validator=_validate_moki_totals,
            generated_at_extractor=lambda _d: latest_meta.data_generated_at,
        )
        return payload, meta

    def get_support_stats(self) -> Tuple[Dict[str, Any], FeedMeta]:
        cumulative_latest, cumulative_meta = self._cached_fetch(
            cache_key="cumulative_latest_manifest",
            url=f"{self.base_url}/cumulative/latest.json",
            gzip_json=False,
            validator=lambda d: d if isinstance(d, dict) and "current_totals" in d else (_ for _ in ()).throw(
                FeedFormatError("cumulative latest missing current_totals")
            ),
            generated_at_extractor=lambda d: d.get("generated_at_utc"),
        )
        support_meta = cumulative_latest.get("support_stats") if isinstance(cumulative_latest, dict) else None
        rel_url = "support_stats.json"
        if isinstance(support_meta, dict) and support_meta.get("url"):
            rel_url = str(support_meta["url"])
        payload, meta = self._cached_fetch(
            cache_key="support_stats",
            url=urljoin(f"{self.base_url}/", rel_url),
            gzip_json=False,
            validator=_validate_support_stats,
            generated_at_extractor=lambda _d: cumulative_meta.data_generated_at,
        )
        return payload, meta

    def _all_partitions_payloads(self) -> Tuple[List[Dict[str, Any]], FeedMeta]:
        latest, latest_meta = self.get_latest_manifest()
        rows: List[Dict[str, Any]] = []
        stale = latest_meta.stale_data
        max_age = latest_meta.cache_age_seconds
        for part in latest.get("partitions", []):
            day_iso = str(part["date"])
            payload, meta = self.get_partition_by_date(day_iso)
            rows.extend(payload)
            stale = stale or meta.stale_data
            max_age = max(max_age, meta.cache_age_seconds)
        return rows, FeedMeta(latest_meta.data_generated_at, max_age, stale)

    @staticmethod
    def _stats_lookup(payload: Dict[str, Any]) -> Tuple[Dict[Tuple[int, Any], Dict[str, Any]], Dict[int, Dict[str, float]]]:
        stats_by_token_team: Dict[Tuple[int, Any], Dict[str, Any]] = {}
        for stat_row in payload.get("stats_players") or []:
            token_id = stat_row.get("token_id")
            if token_id is None:
                continue
            stats_by_token_team[(int(token_id), stat_row.get("team"))] = stat_row

        perf_acc: Dict[int, Dict[str, float]] = {}
        perf_cnt: Dict[int, int] = {}
        for perf_row in payload.get("performances") or []:
            token_id = perf_row.get("token_id")
            if token_id is None:
                continue
            tid = int(token_id)
            cur = perf_acc.setdefault(tid, {"deposits": 0.0, "eliminations": 0.0, "wart_distance": 0.0})
            cur["deposits"] += float(perf_row.get("deposits", 0.0) or 0.0)
            cur["eliminations"] += float(perf_row.get("eliminations", 0.0) or 0.0)
            cur["wart_distance"] += float(perf_row.get("wart_distance", 0.0) or 0.0)
            perf_cnt[tid] = perf_cnt.get(tid, 0) + 1
        perf_avg: Dict[int, Dict[str, float]] = {}
        for tid, sums in perf_acc.items():
            c = max(1, perf_cnt.get(tid, 1))
            perf_avg[tid] = {
                "deposits": sums["deposits"] / c,
                "eliminations": sums["eliminations"] / c,
                "wart_distance": sums["wart_distance"] / c,
            }
        return stats_by_token_team, perf_avg

    def _payloads_for_dates(self, dates: List[str]) -> Tuple[List[Dict[str, Any]], FeedMeta]:
        if not dates:
            latest, latest_meta = self.get_latest_manifest()
            return [], FeedMeta(latest_meta.data_generated_at, latest_meta.cache_age_seconds, latest_meta.stale_data)
        latest, latest_meta = self.get_latest_manifest()
        stale = latest_meta.stale_data
        max_age = latest_meta.cache_age_seconds
        rows: List[Dict[str, Any]] = []
        for day_iso in dates:
            payload, meta = self.get_partition_by_date(day_iso)
            rows.extend(payload)
            stale = stale or meta.stale_data
            max_age = max(max_age, meta.cache_age_seconds)
        return rows, FeedMeta(latest_meta.data_generated_at, max_age, stale)

    def champion_rows(self) -> Tuple[Dict[str, Any], FeedMeta]:
        latest, latest_meta = self.get_latest_manifest()
        payload_rows, part_meta = self._all_partitions_payloads()
        stale = latest_meta.stale_data or part_meta.stale_data
        cache_age = max(latest_meta.cache_age_seconds, part_meta.cache_age_seconds)

        aggregates: Dict[int, Dict[str, Any]] = {}
        for payload in payload_rows:
            match = payload.get("match") or {}
            players = payload.get("players") or []
            state = match.get("state")
            team_won = match.get("team_won")
            stats_by_token_team, perf_avg = self._stats_lookup(payload)
            for player in players:
                if not player.get("is_champion"):
                    continue
                token_id = int(player["token_id"])
                row = aggregates.setdefault(
                    token_id,
                    {
                        "token_id": token_id,
                        "name": str(player.get("name") or f"#{token_id}"),
                        "matches_played": 0,
                        "wins": 0,
                        "points_total": 0.0,
                        "next_count": 0,
                    },
                )
                if state == "scheduled":
                    row["next_count"] += 1
                    continue
                if state != "scored":
                    continue
                row["matches_played"] += 1
                won = team_won is not None and team_won == player.get("team")
                if won:
                    row["wins"] += 1
                perf = perf_avg.get(token_id)
                if perf is not None:
                    deposits = float(perf.get("deposits", 0.0) or 0.0)
                    eliminations = float(perf.get("eliminations", 0.0) or 0.0)
                    wart_distance = float(perf.get("wart_distance", 0.0) or 0.0)
                else:
                    stats = stats_by_token_team.get((token_id, player.get("team")), {})
                    deposits = float(stats.get("deposits", 0.0) or 0.0)
                    eliminations = float(stats.get("eliminations", 0.0) or 0.0)
                    wart_distance = float(stats.get("wart_distance", 0.0) or 0.0)
                row["points_total"] += (deposits * 50.0) + (eliminations * 80.0) + (math.floor(wart_distance / 80.0) * 45.0) + (
                    300.0 if won else 0.0
                )

        result_rows: List[Dict[str, Any]] = []
        for row in aggregates.values():
            games = int(row["matches_played"])
            wins = int(row["wins"])
            win_pct = (wins / games) if games > 0 else 0.0
            avg_points = (float(row["points_total"]) / games) if games > 0 else 0.0
            result_rows.append(
                {
                    "token_id": int(row["token_id"]),
                    "name": row["name"],
                    "matches_played": games,
                    "wins": wins,
                    "win_pct": round(win_pct, 4),
                    "avg_points": round(avg_points, 4),
                    "next_count": int(row["next_count"]),
                }
            )

        result_rows.sort(key=lambda r: (-r["win_pct"], r["token_id"]))
        available_dates = latest.get("available_dates") or []
        today_iso = utc_today().isoformat()
        return (
            {
                "lookahead_days": int(latest.get("lookahead_days", 0)),
                "window_start": available_dates[0] if available_dates else today_iso,
                "window_end": available_dates[-1] if available_dates else today_iso,
                "rows": result_rows,
            },
            FeedMeta(latest_meta.data_generated_at, cache_age, stale),
        )

    def non_champion_rows(self) -> Tuple[List[Dict[str, Any]], FeedMeta]:
        payload_rows, meta = self._all_partitions_payloads()
        by_token: Dict[int, Dict[str, Any]] = {}
        for payload in payload_rows:
            match = payload.get("match") or {}
            players = payload.get("players") or []
            stats_by_token_team, perf_avg = self._stats_lookup(payload)
            for player in players:
                if player.get("is_champion"):
                    continue
                token_id = int(player["token_id"])
                row = by_token.setdefault(
                    token_id,
                    {
                        "token_id": token_id,
                        "name": player.get("name") or f"#{token_id}",
                        "games": 0,
                        "wins": 0,
                        "points_total": 0.0,
                    },
                )
                if not row.get("name") and player.get("name"):
                    row["name"] = player["name"]
                if match.get("state") != "scored":
                    continue
                row["games"] += 1
                team_won = match.get("team_won")
                won = team_won is not None and team_won == player.get("team")
                if won:
                    row["wins"] += 1
                perf = perf_avg.get(token_id)
                if perf is not None:
                    deposits = float(perf.get("deposits", 0.0) or 0.0)
                    eliminations = float(perf.get("eliminations", 0.0) or 0.0)
                    wart = float(perf.get("wart_distance", 0.0) or 0.0)
                else:
                    stat = stats_by_token_team.get((token_id, player.get("team")), {})
                    deposits = float(stat.get("deposits", 0.0) or 0.0)
                    eliminations = float(stat.get("eliminations", 0.0) or 0.0)
                    wart = float(stat.get("wart_distance", 0.0) or 0.0)
                row["points_total"] += (deposits * 50.0) + (eliminations * 80.0) + (math.floor(wart / 80.0) * 45.0) + (
                    300.0 if won else 0.0
                )
        rows = []
        for row in by_token.values():
            games = int(row["games"])
            wins = int(row["wins"])
            win_pct = (wins / games) if games > 0 else 0.0
            avg_points = (float(row["points_total"]) / games) if games > 0 else 0.0
            rows.append(
                {
                    "token_id": row["token_id"],
                    "name": row["name"],
                    "games": games,
                    "win_pct": round(win_pct, 4),
                    "avg_points": round(avg_points, 2),
                }
            )
        rows.sort(key=lambda r: r["token_id"])
        return rows, meta

    def _history_for_token(self, token_id: int, *, champion_only: Optional[bool]) -> Tuple[Dict[str, Any], FeedMeta]:
        payload_rows, meta = self._all_partitions_payloads()
        games: List[Dict[str, Any]] = []
        total_wins = 0
        total_losses = 0
        total_points = 0.0
        total_deposits = 0.0
        total_elims = 0.0
        total_wart = 0.0
        win_type_counts = {"gacha": 0, "eliminations": 0, "wart": 0}
        resolved_name: Optional[str] = None

        for payload in payload_rows:
            match = payload.get("match") or {}
            if match.get("state") != "scored":
                continue
            team_won = match.get("team_won")
            player_rows = payload.get("players") or []
            stats_by_token_team, perf_avg = self._stats_lookup(payload)
            for player in player_rows:
                if int(player.get("token_id", -1)) != int(token_id):
                    continue
                is_champion = bool(player.get("is_champion"))
                if champion_only is True and not is_champion:
                    continue
                if champion_only is False and is_champion:
                    continue
                resolved_name = str(player.get("name") or resolved_name or f"#{token_id}")
                won = team_won is not None and team_won == player.get("team")
                if won:
                    total_wins += 1
                else:
                    total_losses += 1
                perf = perf_avg.get(int(token_id))
                if perf is not None:
                    deposits = float(perf.get("deposits", 0.0) or 0.0)
                    eliminations = float(perf.get("eliminations", 0.0) or 0.0)
                    wart_distance = float(perf.get("wart_distance", 0.0) or 0.0)
                else:
                    stats = stats_by_token_team.get((int(token_id), player.get("team")), {})
                    deposits = float(stats.get("deposits", 0.0) or 0.0)
                    eliminations = float(stats.get("eliminations", 0.0) or 0.0)
                    wart_distance = float(stats.get("wart_distance", 0.0) or 0.0)
                points = (deposits * 50.0) + (eliminations * 80.0) + (math.floor(wart_distance / 80.0) * 45.0) + (
                    300.0 if won else 0.0
                )
                total_points += points
                total_deposits += deposits
                total_elims += eliminations
                total_wart += wart_distance
                if won and match.get("win_type") in win_type_counts:
                    win_type_counts[str(match.get("win_type"))] += 1
                games.append(
                    {
                        "match_id": match.get("match_id"),
                        "match_date": match.get("match_date"),
                        "result": "W" if won else "L",
                        "win_type": match.get("win_type"),
                        "points": round(points, 2),
                        "deposits": round(deposits, 2),
                        "eliminations": round(eliminations, 2),
                        "wart_distance": round(wart_distance, 2),
                    }
                )

        games.sort(key=lambda g: (str(g.get("match_date")), str(g.get("match_id"))), reverse=True)
        game_count = len(games)
        win_pct = (float(total_wins) / float(game_count)) if game_count > 0 else 0.0
        win_total = float(total_wins) if total_wins > 0 else 1.0
        history = {
            "games": games,
            "summary": {
                "win_pct": round(win_pct, 4),
                "avg_points": round((total_points / game_count) if game_count > 0 else 0.0, 2),
                "avg_deposits": round((total_deposits / game_count) if game_count > 0 else 0.0, 2),
                "avg_eliminations": round((total_elims / game_count) if game_count > 0 else 0.0, 2),
                "avg_wart_distance": round((total_wart / game_count) if game_count > 0 else 0.0, 2),
                "win_type_pct": {
                    "gacha": round(float(win_type_counts["gacha"]) / win_total, 4),
                    "eliminations": round(float(win_type_counts["eliminations"]) / win_total, 4),
                    "wart": round(float(win_type_counts["wart"]) / win_total, 4),
                },
            },
            "totals": {
                "games": game_count,
                "wins": total_wins,
                "losses": total_losses,
                "points": round(total_points, 2),
                "deposits": round(total_deposits, 2),
                "eliminations": round(total_elims, 2),
                "wart_distance": round(total_wart, 2),
            },
            "generated_at": utc_now_iso(),
        }
        if champion_only:
            history["champion"] = {"token_id": token_id, "name": resolved_name} if resolved_name else None
        else:
            history["player"] = {"token_id": token_id, "name": resolved_name} if resolved_name else None
        return history, meta

    def champion_history(self, token_id: int) -> Tuple[Dict[str, Any], FeedMeta]:
        return self._history_for_token(token_id, champion_only=True)

    def non_champion_history(self, token_id: int) -> Tuple[Dict[str, Any], FeedMeta]:
        return self._history_for_token(token_id, champion_only=False)

    def champion_next_matches(self, token_id: int, *, limit: int = 10, lookahead_days: int = 2) -> Tuple[Dict[str, Any], FeedMeta]:
        latest, latest_meta = self.get_latest_manifest()
        available_dates = list(latest.get("available_dates") or [])
        today = utc_today()
        today_iso = today.isoformat()
        window_start = today_iso
        window_end = date.fromordinal(today.toordinal() + lookahead_days).isoformat()
        window_dates = [d for d in available_dates if window_start <= d <= window_end]
        payload_rows, part_meta = self._payloads_for_dates(window_dates)
        stale = latest_meta.stale_data or part_meta.stale_data
        age = max(latest_meta.cache_age_seconds, part_meta.cache_age_seconds)

        support_stats, support_meta = self.get_support_stats()
        stale = stale or support_meta.stale_data
        age = max(age, support_meta.cache_age_seconds)
        player_games = {
            int(token_id): (int(stats.get("games", 0) or 0), int(stats.get("wins", 0) or 0))
            for token_id, stats in (support_stats.get("player_games") or {}).items()
        }
        champion_games = {
            int(token_id): (int(stats.get("games", 0) or 0), int(stats.get("wins", 0) or 0))
            for token_id, stats in (support_stats.get("champion_games") or {}).items()
        }

        def _win_pct(stats: Tuple[int, int]) -> float:
            g, w = stats
            return (float(w) / float(g)) if g > 0 else 0.5

        found_name: Optional[str] = None
        matches: List[Dict[str, Any]] = []
        for payload in payload_rows:
            match = payload.get("match") or {}
            if match.get("state") != "scheduled":
                continue
            players = payload.get("players") or []
            me = None
            for p in players:
                if int(p.get("token_id", -1)) == int(token_id) and p.get("is_champion"):
                    me = p
                    break
            if me is None:
                continue
            found_name = str(me.get("name") or found_name or f"#{token_id}")
            my_team = me.get("team")
            team_players = [p for p in players if p.get("team") == my_team]
            opp_players = [p for p in players if p.get("team") != my_team]

            teammate_non = []
            teammate_scores: List[float] = []
            for p in team_players:
                if p.get("is_champion"):
                    continue
                pct = _win_pct(player_games.get(int(p["token_id"]), (0, 0)))
                teammate_scores.append(pct)
                teammate_non.append({"token_id": int(p["token_id"]), "name": p.get("name"), "global_win_pct": round(pct, 4)})

            opp_non = []
            opp_non_scores: List[float] = []
            opp_champ = []
            opp_champ_scores: List[float] = []
            for p in opp_players:
                if p.get("is_champion"):
                    pct = _win_pct(champion_games.get(int(p["token_id"]), (0, 0)))
                    opp_champ_scores.append(pct)
                    opp_champ.append({"token_id": int(p["token_id"]), "name": p.get("name"), "win_pct": round(pct, 4)})
                else:
                    pct = _win_pct(player_games.get(int(p["token_id"]), (0, 0)))
                    opp_non_scores.append(pct)
                    opp_non.append({"token_id": int(p["token_id"]), "name": p.get("name"), "global_win_pct": round(pct, 4)})

            champ_pct = _win_pct(champion_games.get(int(token_id), (0, 0)))
            team_support = sum(teammate_scores) / len(teammate_scores) if teammate_scores else 0.5
            opp_support = sum(opp_non_scores) / len(opp_non_scores) if opp_non_scores else 0.5
            opp_champ_pct = sum(opp_champ_scores) / len(opp_champ_scores) if opp_champ_scores else 0.5
            edge_v1 = 0.5 + (champ_pct - opp_champ_pct) * 0.35 + (team_support - opp_support) * 0.15
            team_strength = (champ_pct * 0.65) + (team_support * 0.35)
            opp_strength = (opp_champ_pct * 0.65) + (opp_support * 0.35)
            edge_v2_raw = team_strength - opp_strength
            edge_score = (edge_v2_raw + 1.0) * 50.0

            matches.append(
                {
                    "match_id": match.get("match_id"),
                    "match_date": match.get("match_date"),
                    "opponent_champions": opp_champ,
                    "teammates_nonchampions": teammate_non,
                    "opponent_teammates_nonchampions": opp_non,
                    "edge_score": round(edge_score, 2),
                    "edge_label": "Strong Edge" if edge_score >= 60 else ("Slight Edge" if edge_score >= 52 else ("Close" if edge_score >= 48 else "Underdog")),
                    "components": {
                        "edge_v1_score": round(edge_v1, 4),
                        "champion_win_pct": round(champ_pct, 4),
                        "team_support_win_pct": round(team_support, 4),
                        "opponent_team_support_win_pct": round(opp_support, 4),
                        "opponent_champion_win_pct": round(opp_champ_pct, 4),
                        "normalized_points_component": 0.0,
                        "edge_v2_raw": round(edge_v2_raw, 4),
                        "team_strength": round(team_strength, 4),
                        "opponent_team_strength": round(opp_strength, 4),
                        "global_mean_win_pct": 0.5,
                        "team_player_weights": [],
                        "opponent_player_weights": [],
                    },
                }
            )
        matches.sort(key=lambda m: (str(m.get("match_date")), str(m.get("match_id"))))
        matches = matches[: max(1, int(limit))]
        payload = {
            "champion": {"token_id": int(token_id), "name": found_name} if found_name else None,
            "lookahead_days": lookahead_days,
            "window_start": window_start,
            "window_end": window_end,
            "matches": matches,
            "insufficient_upcoming": len(matches) < max(1, int(limit)),
            "generated_at": utc_now_iso(),
        }
        return payload, FeedMeta(latest_meta.data_generated_at, age, stale)

    def non_champion_next_matches(self, token_id: int, *, limit: int = 10, lookahead_days: int = 2) -> Tuple[Dict[str, Any], FeedMeta]:
        latest, latest_meta = self.get_latest_manifest()
        available_dates = list(latest.get("available_dates") or [])
        today = utc_today()
        today_iso = today.isoformat()
        window_start = today_iso
        window_end = date.fromordinal(today.toordinal() + lookahead_days).isoformat()
        window_dates = [d for d in available_dates if window_start <= d <= window_end]
        payload_rows, part_meta = self._payloads_for_dates(window_dates)
        stale = latest_meta.stale_data or part_meta.stale_data
        age = max(latest_meta.cache_age_seconds, part_meta.cache_age_seconds)

        found_name: Optional[str] = None
        matches: List[Dict[str, Any]] = []
        for payload in payload_rows:
            match = payload.get("match") or {}
            if match.get("state") != "scheduled":
                continue
            players = payload.get("players") or []
            me = None
            for p in players:
                if int(p.get("token_id", -1)) == int(token_id) and not p.get("is_champion"):
                    me = p
                    break
            if me is None:
                continue
            found_name = str(me.get("name") or found_name or f"#{token_id}")
            my_team = me.get("team")
            team_champs = [
                {"token_id": int(p["token_id"]), "name": p.get("name")}
                for p in players
                if p.get("team") == my_team and p.get("is_champion")
            ]
            opp_champs = [
                {"token_id": int(p["token_id"]), "name": p.get("name")}
                for p in players
                if p.get("team") != my_team and p.get("is_champion")
            ]
            matches.append(
                {
                    "match_id": match.get("match_id"),
                    "match_date": match.get("match_date"),
                    "team_champions": team_champs,
                    "opponent_champions": opp_champs,
                }
            )
        matches.sort(key=lambda m: (str(m.get("match_date")), str(m.get("match_id"))))
        matches = matches[: max(1, int(limit))]
        payload = {
            "player": {"token_id": int(token_id), "name": found_name} if found_name else None,
            "lookahead_days": lookahead_days,
            "window_start": window_start,
            "window_end": window_end,
            "matches": matches,
            "insufficient_upcoming": len(matches) < max(1, int(limit)),
            "generated_at": utc_now_iso(),
        }
        return payload, FeedMeta(latest_meta.data_generated_at, age, stale)

    def champion_match_info(self, token_id: int) -> Tuple[Dict[str, Any], FeedMeta]:
        payload_rows, meta = self._all_partitions_payloads()
        found_name: Optional[str] = None
        matches: List[Dict[str, Any]] = []
        combined = {
            "champion": [],
            "team_non_champion": [],
            "opponent_champion": [],
            "opponent_non_champion": [],
        }
        for payload in payload_rows:
            match = payload.get("match") or {}
            if match.get("state") != "scored":
                continue
            players = payload.get("players") or []
            me = None
            for p in players:
                if int(p.get("token_id", -1)) == int(token_id) and p.get("is_champion"):
                    me = p
                    break
            if me is None:
                continue
            found_name = str(me.get("name") or found_name or f"#{token_id}")
            my_team = me.get("team")
            team_champion = [p for p in players if p.get("team") == my_team and p.get("is_champion")]
            team_non = [p for p in players if p.get("team") == my_team and not p.get("is_champion")]
            opp_champion = [p for p in players if p.get("team") != my_team and p.get("is_champion")]
            opp_non = [p for p in players if p.get("team") != my_team and not p.get("is_champion")]
            combined["champion"].extend(team_champion)
            combined["team_non_champion"].extend(team_non)
            combined["opponent_champion"].extend(opp_champion)
            combined["opponent_non_champion"].extend(opp_non)
            team_won = match.get("team_won")
            won = team_won is not None and team_won == my_team

            def _classes(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
                cls = [str(r.get("class")) for r in rows if r.get("class")]
                return {"count": len(rows), "classes": cls, "unique_classes": sorted(set(cls))}

            matches.append(
                {
                    "match_id": match.get("match_id"),
                    "match_date": match.get("match_date"),
                    "result": "W" if won else "L",
                    "win_type": match.get("win_type"),
                    "champion": _classes(team_champion),
                    "team_non_champion": _classes(team_non),
                    "opponent_champion": _classes(opp_champion),
                    "opponent_non_champion": _classes(opp_non),
                }
            )
        matches.sort(key=lambda m: (str(m.get("match_date")), str(m.get("match_id"))), reverse=True)

        def _classes(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
            cls = [str(r.get("class")) for r in rows if r.get("class")]
            return {"count": len(rows), "classes": cls, "unique_classes": sorted(set(cls))}

        payload = {
            "champion": {"token_id": int(token_id), "name": found_name} if found_name else None,
            "matches": matches,
            "combined_classes": {
                "champion": _classes(combined["champion"]),
                "team_non_champion": _classes(combined["team_non_champion"]),
                "opponent_champion": _classes(combined["opponent_champion"]),
                "opponent_non_champion": _classes(combined["opponent_non_champion"]),
            },
            "generated_at": utc_now_iso(),
        }
        return payload, meta
