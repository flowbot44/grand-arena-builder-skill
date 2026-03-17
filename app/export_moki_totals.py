from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .api_client import GrandArenaClient, RateLimiter
from .config import SETTINGS


def _chunks(values: List[int], size: int) -> Iterable[List[int]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _extract_total_stats(moki: Dict[str, Any], match_stats: Dict[str, Any]) -> Dict[str, Any]:
    stats = ((moki.get("gameStats") or {}).get("stats") or {})
    return {
        "mokiId": moki.get("id"),
        "tokenId": moki.get("mokiTokenId"),
        "name": moki.get("name"),
        "class": (moki.get("gameStats") or {}).get("class"),
        "totals": {
            "strength": ((stats.get("strength") or {}).get("total")),
            "speed": ((stats.get("speed") or {}).get("total")),
            "defense": ((stats.get("defense") or {}).get("total")),
            "dexterity": ((stats.get("dexterity") or {}).get("total")),
            "fortitude": ((stats.get("fortitude") or {}).get("total")),
        },
        "matchStats": {
            "matchCount": match_stats.get("matchCount"),
            "wins": match_stats.get("wins"),
            "losses": match_stats.get("losses"),
            "winRate": match_stats.get("winRate"),
            "avgDeposits": match_stats.get("avgDeposits"),
            "avgEliminations": match_stats.get("avgEliminations"),
            "avgWartDistance": match_stats.get("avgWartDistance"),
            "winsByType": match_stats.get("winsByType"),
        },
    }


def fetch_all_moki_totals(client: GrandArenaClient, *, page_limit: int = 100, bulk_limit: int = 100) -> List[Dict[str, Any]]:
    token_ids: List[int] = []
    page = 1
    pages = 1

    while page <= pages:
        payload = client.list_mokis(page=page, limit=page_limit)
        data = payload.get("data") or []
        pagination = payload.get("pagination") or {}
        pages = int(pagination.get("pages") or page)
        page = int(pagination.get("page") or page)

        for row in data:
            token_id = row.get("mokiTokenId")
            if token_id is None:
                continue
            token_ids.append(int(token_id))

        page += 1

    seen: set[int] = set()
    unique_token_ids: List[int] = []
    for token_id in token_ids:
        if token_id in seen:
            continue
        seen.add(token_id)
        unique_token_ids.append(token_id)

    moki_list: List[tuple] = []
    for chunk in _chunks(unique_token_ids, bulk_limit):
        payload = client.get_mokis_bulk(chunk)
        for moki in payload.get("data") or []:
            token_id = moki.get("mokiTokenId")
            moki_list.append((moki, int(token_id) if token_id is not None else None))

    def _fetch_stats(token_id: Optional[int]) -> Dict[str, Any]:
        if token_id is None:
            return {}
        return client.get_moki_stats(token_id).get("data") or {}

    workers = min(SETTINGS.ingest_workers, len(moki_list)) if moki_list else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        stats_list = list(pool.map(_fetch_stats, [tid for _, tid in moki_list]))

    results = [_extract_total_stats(moki, stats) for (moki, _), stats in zip(moki_list, stats_list)]

    results.sort(key=lambda row: (row.get("tokenId") is None, row.get("tokenId")))
    return results


def write_moki_totals_json(out_path: str) -> str:
    rate_limiter = RateLimiter(
        max_per_minute=SETTINGS.request_limit_per_minute,
        min_interval_seconds=SETTINGS.min_request_interval_seconds,
    )
    client = GrandArenaClient(
        base_url=SETTINGS.api_base_url,
        api_key=SETTINGS.api_key,
        rate_limiter=rate_limiter,
        timeout_seconds=SETTINGS.api_timeout_seconds,
        retries=SETTINGS.api_retries,
    )

    rows = fetch_all_moki_totals(client, page_limit=SETTINGS.api_page_limit, bulk_limit=100)
    output = {
        "count": len(rows),
        "data": rows,
    }
    target = Path(out_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        json.dump(output, fh, indent=2, ensure_ascii=True)
    return str(target)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export all Mokis and their total stats from /api/v1/mokis + /api/v1/mokis/bulk"
    )
    parser.add_argument("--out", default="exports/data/moki_totals.json", help="Output JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not SETTINGS.api_key:
        print("Missing API key. Set GRANDARENA_API_KEY.")
        return 2
    out = write_moki_totals_json(args.out)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
