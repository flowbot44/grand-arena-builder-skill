from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .config import SETTINGS
from .feed_adapter import FeedAdapter, FeedUnavailableError
from .time_utils import utc_today


def _build_adapter() -> FeedAdapter:
    return FeedAdapter(
        base_url=SETTINGS.feed_base_url,
        ttl_seconds=SETTINGS.feed_ttl_seconds,
        timeout_seconds=SETTINGS.feed_http_timeout_seconds,
    )


def _candidate_dates(today_iso: Optional[str], lookahead_days: int) -> List[str]:
    if today_iso:
        start = date.fromisoformat(today_iso)
    else:
        start = utc_today()
    return [(start + timedelta(days=offset)).isoformat() for offset in range(max(0, lookahead_days) + 1)]


def _resolve_champion(
    partitions_by_date: Dict[str, List[Dict[str, Any]]],
    *,
    token_id: Optional[int],
    name: Optional[str],
) -> Tuple[int, str]:
    discovered: Dict[int, str] = {}
    by_name: Dict[str, List[int]] = {}
    for matches in partitions_by_date.values():
        for match in matches:
            for player in match.get("players", []):
                if not player.get("is_champion"):
                    continue
                player_id = int(player["token_id"])
                player_name = str(player.get("name") or "")
                discovered[player_id] = player_name or discovered.get(player_id, "")
                if player_name:
                    by_name.setdefault(player_name.casefold(), []).append(player_id)

    if token_id is not None:
        if token_id not in discovered:
            raise SystemExit(f"Champion token_id {token_id} not found in the requested partitions.")
        return token_id, discovered[token_id]

    if not name:
        raise SystemExit("Pass either --token-id or --name.")

    matches = sorted(set(by_name.get(name.casefold(), [])))
    if not matches:
        raise SystemExit(f"Champion name '{name}' not found in the requested partitions.")
    if len(matches) > 1:
        raise SystemExit(f"Champion name '{name}' is ambiguous across token_ids: {matches}")
    champion_id = matches[0]
    return champion_id, discovered.get(champion_id, name)


def _team_rows(players: List[Dict[str, Any]], team: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for player in players:
        if int(player.get("team", -1)) != team:
            continue
        rows.append(
            {
                "token_id": int(player["token_id"]),
                "name": player.get("name"),
                "class": player.get("class"),
                "is_champion": bool(player.get("is_champion")),
            }
        )
    rows.sort(key=lambda row: (not row["is_champion"], row["token_id"]))
    return rows


def _extract_match_rows(matches: List[Dict[str, Any]], champion_id: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for wrapper in matches:
        players = wrapper.get("players", [])
        me = None
        for player in players:
            if player.get("is_champion") and int(player.get("token_id", -1)) == champion_id:
                me = player
                break
        if me is None:
            continue

        match = wrapper.get("match", {})
        my_team = int(me["team"])
        team_won = match.get("team_won")
        won = team_won in (1, 2) and int(team_won) == my_team
        out.append(
            {
                "match_id": match.get("match_id"),
                "match_date": match.get("match_date"),
                "state": match.get("state"),
                "updated_at": match.get("updated_at"),
                "last_seen_at": match.get("last_seen_at"),
                "team": my_team,
                "team_won": team_won,
                "won": won,
                "win_type": match.get("win_type"),
                "is_bye": bool(match.get("is_bye")),
                "my_team_players": _team_rows(players, my_team),
                "opponent_team_players": _team_rows(players, 1 if my_team == 2 else 2),
            }
        )
    out.sort(key=lambda row: (str(row.get("match_date")), str(row.get("match_id"))))
    return out


def _partition_summary(matches: List[Dict[str, Any]], champion_id: int, champion_name: str) -> Dict[str, Any]:
    counts: Counter[int] = Counter()
    names: Dict[int, str] = {}
    for wrapper in matches:
        for player in wrapper.get("players", []):
            if not player.get("is_champion"):
                continue
            player_id = int(player["token_id"])
            counts[player_id] += 1
            names[player_id] = str(player.get("name") or names.get(player_id) or "")

    values = sorted(counts.values())
    underfilled = [
        {"token_id": player_id, "name": names.get(player_id), "matches": count}
        for player_id, count in sorted(counts.items(), key=lambda item: (item[1], names.get(item[0], "")))
        if count < 30
    ]
    overfilled = [
        {"token_id": player_id, "name": names.get(player_id), "matches": count}
        for player_id, count in sorted(counts.items(), key=lambda item: (-item[1], names.get(item[0], "")))
        if count > 30
    ]
    match_rows = _extract_match_rows(matches, champion_id)
    return {
        "champion": {"token_id": champion_id, "name": champion_name},
        "champion_match_count": len(match_rows),
        "expected_match_count": 30,
        "matches_missing_from_30": max(0, 30 - len(match_rows)),
        "champions_seen": len(counts),
        "min_matches_per_champion": values[0] if values else 0,
        "max_matches_per_champion": values[-1] if values else 0,
        "champions_below_30_count": len(underfilled),
        "champions_above_30_count": len(overfilled),
        "champions_below_30_sample": underfilled[:20],
        "champions_above_30_sample": overfilled[:20],
        "matches": match_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect published feed matches for one champion for today and the lookahead day."
    )
    parser.add_argument("--token-id", type=int, help="Champion token_id to inspect.")
    parser.add_argument("--name", help="Champion name to inspect (case-insensitive exact match).")
    parser.add_argument("--today", help="Override start day in YYYY-MM-DD. Defaults to local today.")
    parser.add_argument(
        "--lookahead-days",
        type=int,
        default=1,
        help="How many future days to inspect from --today. Default: 1.",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path to write the full diagnostic payload as JSON.",
    )
    args = parser.parse_args()

    if args.token_id is None and not args.name:
        parser.error("one of --token-id or --name is required")

    adapter = _build_adapter()
    requested_dates = _candidate_dates(args.today, args.lookahead_days)
    try:
        latest, meta = adapter.get_latest_manifest()
    except FeedUnavailableError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    available = {str(part.get("date")): part for part in latest.get("partitions", [])}
    selected_dates = [day for day in requested_dates if day in available]
    missing_dates = [day for day in requested_dates if day not in available]
    if not selected_dates:
        print(
            f"No requested dates are available in the published feed. requested={requested_dates} "
            f"available={sorted(available)}",
            file=sys.stderr,
        )
        return 1

    partitions_by_date: Dict[str, List[Dict[str, Any]]] = {}
    for day in selected_dates:
        try:
            payload, _ = adapter.get_partition_by_date(day)
        except FeedUnavailableError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        partitions_by_date[day] = payload

    champion_id, champion_name = _resolve_champion(
        partitions_by_date,
        token_id=args.token_id,
        name=args.name,
    )

    days: List[Dict[str, Any]] = []
    for day in selected_dates:
        summary = _partition_summary(partitions_by_date[day], champion_id, champion_name)
        summary["date"] = day
        summary["manifest_match_count"] = int(available[day].get("match_count", 0))
        summary["partition_url"] = str(available[day].get("url", ""))
        days.append(summary)

    payload = {
        "feed_base_url": SETTINGS.feed_base_url,
        "manifest_generated_at_utc": latest.get("generated_at_utc"),
        "manifest_cache_meta": meta.as_body(),
        "requested_dates": requested_dates,
        "selected_dates": selected_dates,
        "missing_dates": missing_dates,
        "champion": {"token_id": champion_id, "name": champion_name},
        "days": days,
    }

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.write("\n")

    print(
        f"Champion {champion_name} ({champion_id}) from {SETTINGS.feed_base_url} "
        f"manifest_generated_at={latest.get('generated_at_utc')}"
    )
    if missing_dates:
        print(f"missing_dates={','.join(missing_dates)}")
    for day in days:
        print(
            f"{day['date']}: champion_matches={day['champion_match_count']}/30 "
            f"manifest_match_count={day['manifest_match_count']} "
            f"champion_range={day['min_matches_per_champion']}..{day['max_matches_per_champion']} "
            f"champions_below_30={day['champions_below_30_count']}"
        )
        for match in day["matches"]:
            print(
                f"  match_id={match['match_id']} state={match['state']} team={match['team']} "
                f"team_won={match['team_won']} won={match['won']} win_type={match['win_type']}"
            )

    if args.json_out:
        print(f"wrote_json={args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
