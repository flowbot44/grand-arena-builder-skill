from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class ApiError(RuntimeError):
    pass


@dataclass
class RateLimiter:
    max_per_minute: int = 80
    min_interval_seconds: float = 0.75
    now_fn: Callable[[], float] = time.monotonic
    sleep_fn: Callable[[float], None] = time.sleep

    def __post_init__(self) -> None:
        self._request_times: deque[float] = deque()
        self._last_request_at: Optional[float] = None

    def wait(self) -> None:
        now = self.now_fn()

        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            if elapsed < self.min_interval_seconds:
                self.sleep_fn(self.min_interval_seconds - elapsed)
                now = self.now_fn()

        while self._request_times and now - self._request_times[0] >= 60.0:
            self._request_times.popleft()

        if len(self._request_times) >= self.max_per_minute:
            wait_for = 60.0 - (now - self._request_times[0])
            if wait_for > 0:
                self.sleep_fn(wait_for)
                now = self.now_fn()
            while self._request_times and now - self._request_times[0] >= 60.0:
                self._request_times.popleft()

        self._request_times.append(now)
        self._last_request_at = now


class GrandArenaClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        rate_limiter: Optional[RateLimiter] = None,
        timeout_seconds: int = 20,
        retries: int = 4,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.rate_limiter = rate_limiter or RateLimiter()
        self.timeout_seconds = timeout_seconds
        self.retries = retries

    @staticmethod
    def _is_retryable_http_status(code: int) -> bool:
        return code in {408, 429} or 500 <= code < 600

    def _request_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.api_key:
            raise ApiError("Missing GRANDARENA_API_KEY")

        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        backoff = 1.0
        for attempt in range(self.retries + 1):
            self.rate_limiter.wait()
            req = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                    "User-Agent": "grandarena-local-sync/1.0",
                },
                method="GET",
            )
            try:
                with urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    return json.loads(raw)
            except HTTPError as exc:
                code = exc.code
                if self._is_retryable_http_status(code) and attempt < self.retries:
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
                raise ApiError(f"HTTP {code} for {url}: {body[:300]}") from exc
            except TimeoutError as exc:
                if attempt < self.retries:
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise ApiError(f"Timeout for {url}: {exc}") from exc
            except URLError as exc:
                if attempt < self.retries:
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                raise ApiError(f"Network error for {url}: {exc}") from exc
            except json.JSONDecodeError as exc:
                raise ApiError(f"Invalid JSON response from {url}") from exc

        raise ApiError(f"Request failed after retries: {url}")

    def list_matches(self, match_date: str, page: int, limit: int = 100) -> Dict[str, Any]:
        return self._request_json(
            "/api/v1/matches",
            {
                "page": page,
                "limit": limit,
                "gameType": "mokiMayhem",
                "matchDate": match_date,
                "sort": "updatedAt",
                "order": "asc",
            },
        )

    def list_mokis(self, page: int = 1, limit: int = 100) -> Dict[str, Any]:
        return self._request_json(
            "/api/v1/mokis",
            {
                "page": page,
                "limit": limit,
                "sort": "tokenId",
                "order": "asc",
            },
        )

    def get_mokis_bulk(self, token_ids: List[int]) -> Dict[str, Any]:
        if not token_ids:
            return {"data": []}
        ids_csv = ",".join(str(token_id) for token_id in token_ids)
        return self._request_json("/api/v1/mokis/bulk", {"ids": ids_csv})

    def get_match_stats(self, match_id: str) -> Dict[str, Any]:
        return self._request_json(f"/api/v1/matches/{match_id}/stats")

    def get_match_performances(self, match_id: str, page: int = 1, limit: int = 100) -> Dict[str, Any]:
        return self._request_json(f"/api/v1/matches/{match_id}/performances", {"page": page, "limit": limit})
