#!/usr/bin/env python3
"""Probe selected Grand Arena GET endpoints."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://api.grandarena.gg"
DEFAULT_PATH = "/api/v1/leaderboards/active"
DEFAULT_OUTFILE = "grandarena_api_response.json"


@dataclass
class AttemptResult:
    auth_style: str
    ok: bool
    status: Optional[int]
    error: Optional[str]
    data: Optional[Any]
    url: str


def parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    val = value.strip().lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError("completed must be true/false")


def make_auth_sets(api_key: str) -> List[Tuple[str, Dict[str, str]]]:
    return [
        ("bearer", {"Authorization": f"Bearer {api_key}"}),
        ("x-api-key", {"x-api-key": api_key}),
        ("X-API-Key", {"X-API-Key": api_key}),
    ]


def request_json(url: str, headers: Dict[str, str], timeout: int = 20) -> Tuple[bool, Optional[int], Optional[Any], Optional[str]]:
    req_headers = {
        "Accept": "application/json",
        "User-Agent": "grandarena-local-api-explorer/1.0",
        **headers,
    }
    req = Request(url=url, headers=req_headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", None)
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return True, status, json.loads(raw), None
            except json.JSONDecodeError:
                return False, status, None, "Response was not valid JSON."
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        body_preview = body[:350].strip() if body else str(exc)
        return False, exc.code, None, f"HTTP {exc.code}: {body_preview}"
    except URLError as exc:
        return False, None, None, f"Network error: {exc.reason}"
    except Exception as exc:  # pragma: no cover
        return False, None, None, f"Unexpected error: {exc}"


def encode_query_params(params: Dict[str, Any]) -> str:
    encoded: Dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            encoded[key] = "true" if value else "false"
        else:
            encoded[key] = value
    return urlencode(encoded)


def validate_200_shape(payload: Any, path: str) -> List[str]:
    errors: List[str] = []
    if not isinstance(payload, dict):
        return ["Top-level payload is not an object."]

    if path in (
        "/api/v1/leaderboards",
        "/api/v1/mokis",
        "/api/v1/mokis/{tokenId}/performances",
        "/api/v1/performances",
        "/api/v1/matches",
    ):
        required_keys = ["data", "pagination"]
    else:
        required_keys = ["data"]
    for key in required_keys:
        if key not in payload:
            errors.append(f"Missing top-level key: {key}")

    list_data_paths = (
        "/api/v1/leaderboards",
        "/api/v1/leaderboards/active",
        "/api/v1/mokis",
        "/api/v1/mokis/{tokenId}/performances",
        "/api/v1/performances",
        "/api/v1/matches",
    )
    data = payload.get("data")
    if path in list_data_paths:
        if data is not None and not isinstance(data, list):
            errors.append("`data` should be a list.")
    else:
        if data is not None and not isinstance(data, dict):
            errors.append("`data` should be an object.")

    if isinstance(data, list) and data:
        sample = data[0]
        if not isinstance(sample, dict):
            errors.append("`data[0]` should be an object.")
        elif path in ("/api/v1/leaderboards", "/api/v1/leaderboards/active"):
            expected = [
                "id",
                "name",
                "description",
                "gameTypes",
                "completed",
                "scoringMethod",
                "startDate",
                "endDate",
                "updatedAt",
            ]
            for k in expected:
                if k not in sample:
                    errors.append(f"`data[0]` missing key: {k}")

    if path in (
        "/api/v1/leaderboards",
        "/api/v1/mokis",
        "/api/v1/mokis/{tokenId}/performances",
        "/api/v1/performances",
        "/api/v1/matches",
    ):
        pagination = payload.get("pagination")
        if pagination is not None and not isinstance(pagination, dict):
            errors.append("`pagination` should be an object.")
        elif isinstance(pagination, dict):
            for k in ["page", "limit", "total", "pages"]:
                if k not in pagination:
                    errors.append(f"`pagination` missing key: {k}")

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Call selected Grand Arena GET endpoints.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"API base URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument("--path", default=DEFAULT_PATH, help=f"Endpoint path (default: {DEFAULT_PATH})")
    parser.add_argument("--api-key", default=os.getenv("GRANDARENA_API_KEY", ""), help="API key; falls back to GRANDARENA_API_KEY env var")
    parser.add_argument("--page", type=int, default=1, help="Query param: page (default: 1)")
    parser.add_argument("--limit", type=int, default=20, help="Query param: limit (default: 20)")
    parser.add_argument("--completed", type=parse_bool, default=None, help="Query param: completed (true/false)")
    parser.add_argument("--is-bye", type=parse_bool, default=None, help="Query param: isBye (true/false)")
    parser.add_argument("--game-type", default=None, help="Query param: gameType (example: mokiMayhem)")
    parser.add_argument("--moki-id", default=None, help="Query param: mokiId (for /api/v1/performances)")
    parser.add_argument("--match-id", default=None, help="Query param: matchId (for /api/v1/performances)")
    parser.add_argument("--win-type", default=None, choices=["gacha", "eliminations", "wart"], help="Query param: winType (for /api/v1/performances)")
    parser.add_argument("--match-date", default=None, help="Query param: matchDate YYYY-MM-DD (for /api/v1/performances and /api/v1/matches)")
    parser.add_argument("--token-id", default=None, help="Path param tokenId (for /api/v1/mokis/{tokenId}/performances)")
    parser.add_argument("--path-match-id", default=None, help="Path param matchId (for /api/v1/matches/{matchId}...)")
    parser.add_argument("--owner-address", default=None, help="Query param: ownerAddress (for /api/v1/mokis)")
    parser.add_argument("--moki-class", default=None, help="Query param: class (for /api/v1/mokis)")
    parser.add_argument("--from-date", default=None, help="Query param: fromDate (ISO datetime)")
    parser.add_argument("--to-date", default=None, help="Query param: toDate (ISO datetime)")
    parser.add_argument(
        "--range-2026-h1",
        action="store_true",
        help="Shortcut: set fromDate=2026-01-01T00:00:00.000Z and toDate=2026-07-01T00:00:00.000Z",
    )
    parser.add_argument("--sort", default=None, help="Query param: sort")
    parser.add_argument("--order", default="desc", choices=["asc", "desc"], help="Query param: order")
    parser.add_argument("--out", default=DEFAULT_OUTFILE, help=f"Output JSON file (default: {DEFAULT_OUTFILE})")
    return parser.parse_args()


def validate_dates(from_date: Optional[str], to_date: Optional[str]) -> Optional[str]:
    for label, value in [("fromDate", from_date), ("toDate", to_date)]:
        if value is None:
            continue
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return f"Invalid {label}. Use ISO format, e.g. 2026-02-20T16:42:00.469Z"
    return None


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("Missing API key. Set GRANDARENA_API_KEY or pass --api-key.")
        return 2

    if args.range_2026_h1:
        if args.from_date is None:
            args.from_date = "2026-01-01T00:00:00.000Z"
        if args.to_date is None:
            args.to_date = "2026-07-01T00:00:00.000Z"

    date_error = validate_dates(args.from_date, args.to_date)
    if date_error:
        print(date_error)
        return 2

    path = args.path
    if path == "/api/v1/mokis/{tokenId}/performances":
        if not args.token_id:
            print("Missing --token-id for /api/v1/mokis/{tokenId}/performances")
            return 2
        path = path.replace("{tokenId}", str(args.token_id))
    if path in ("/api/v1/matches/{matchId}", "/api/v1/matches/{matchId}/stats", "/api/v1/matches/{matchId}/performances"):
        match_id = args.path_match_id or args.match_id
        if not match_id:
            print(f"Missing --path-match-id (or --match-id) for {args.path}")
            return 2
        path = path.replace("{matchId}", str(match_id))

    query_params: Dict[str, Any] = {}
    if args.path == "/api/v1/leaderboards":
        query_params = {
            "page": args.page,
            "limit": args.limit,
            "completed": args.completed,
            "gameType": args.game_type,
            "fromDate": args.from_date,
            "toDate": args.to_date,
            "sort": args.sort or "startDate",
            "order": args.order,
        }
    elif args.path == "/api/v1/mokis":
        query_params = {
            "page": args.page,
            "limit": args.limit,
            "ownerAddress": args.owner_address,
            "class": args.moki_class,
            "sort": args.sort or "tokenId",
            "order": args.order,
        }
    elif args.path == "/api/v1/mokis/{tokenId}/performances":
        query_params = {
            "page": args.page,
            "limit": args.limit,
            "gameType": args.game_type,
            "isBye": args.is_bye,
        }
    elif args.path == "/api/v1/performances":
        query_params = {
            "page": args.page,
            "limit": args.limit,
            "mokiId": args.moki_id,
            "matchId": args.match_id,
            "isBye": args.is_bye,
            "winType": args.win_type,
            "matchDate": args.match_date,
            "sort": args.sort or "updatedAt",
            "order": args.order,
        }
    elif args.path == "/api/v1/matches":
        query_params = {
            "page": args.page,
            "limit": args.limit,
            "gameType": args.game_type,
            "matchDate": args.match_date,
            "sort": args.sort or "updatedAt",
            "order": args.order,
        }
    elif args.path == "/api/v1/matches/{matchId}/performances":
        query_params = {
            "page": args.page,
            "limit": args.limit,
        }
    query = encode_query_params(query_params)
    url = f"{args.base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"

    attempts: List[AttemptResult] = []
    for auth_style, auth_header in make_auth_sets(args.api_key):
        ok, status, data, error = request_json(url, auth_header)
        attempts.append(
            AttemptResult(
                auth_style=auth_style,
                ok=ok,
                status=status,
                error=error,
                data=data,
                url=url,
            )
        )
        if ok:
            break

    winner = attempts[-1]
    shape_errors: List[str] = []
    if winner.ok and winner.status == 200:
        shape_errors = validate_200_shape(winner.data, args.path)

    if winner.ok:
        print(f"Success: HTTP {winner.status} via {winner.auth_style}")
    else:
        print(f"Failed: HTTP {winner.status} via {winner.auth_style}")
        print(f"Error: {winner.error}")

    if shape_errors:
        print("\n200 payload shape checks:")
        for err in shape_errors:
            print(f"- {err}")
    elif winner.ok and winner.status == 200:
        print("200 payload shape matches expected keys.")

    output = {
        "request": {
            "base_url": args.base_url,
            "path": args.path,
            "query": {k: v for k, v in query_params.items() if v is not None},
            "url": url,
        },
        "attempts": [
            {
                "auth_style": a.auth_style,
                "ok": a.ok,
                "status": a.status,
                "error": a.error,
                "url": a.url,
                "data": a.data,
            }
            for a in attempts
        ],
        "selected_result_index": len(attempts) - 1,
        "shape_errors": shape_errors,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=True)
    print(f"Wrote {args.out}")

    return 0 if winner.ok else 1


if __name__ == "__main__":
    sys.exit(main())
