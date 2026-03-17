from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    api_base_url: str = os.getenv("GRANDARENA_API_BASE_URL", "https://api.grandarena.gg")
    api_key: str = os.getenv("GRANDARENA_API_KEY", "")
    db_path: str = os.getenv("GRANDARENA_DB_PATH", "grandarena.db")
    champions_path: str = os.getenv("CHAMPIONS_PATH", "champions.json")
    lookbehind_days: int = int(os.getenv("LOOKBEHIND_DAYS", "2"))
    lookahead_days: int = int(os.getenv("LOOKAHEAD_DAYS", "2"))
    request_limit_per_minute: int = int(os.getenv("REQUEST_LIMIT_PER_MINUTE", "80"))
    min_request_interval_seconds: float = float(os.getenv("MIN_REQUEST_INTERVAL_SECONDS", "0.75"))
    api_timeout_seconds: int = int(os.getenv("GRANDARENA_API_TIMEOUT_SECONDS", "30"))
    api_retries: int = int(os.getenv("GRANDARENA_API_RETRIES", "6"))
    api_page_limit: int = int(os.getenv("API_PAGE_LIMIT", "100"))
    ingest_workers: int = int(os.getenv("INGEST_WORKERS", "8"))
    champion_only_matches: bool = _env_bool("CHAMPION_ONLY_MATCHES", True)
    fetch_match_performances: bool = _env_bool("FETCH_MATCH_PERFORMANCES", True)
    backfill_start_default: date = date(2026, 2, 19)
    feed_base_url: str = os.getenv(
        "FEED_BASE_URL",
        "https://flowbot44.github.io/grand-arena-builder-skill/data",
    ).rstrip("/")
    feed_ttl_seconds: int = int(os.getenv("FEED_TTL_SECONDS", "600"))
    feed_http_timeout_seconds: float = float(os.getenv("FEED_HTTP_TIMEOUT_SECONDS", "10"))


SETTINGS = Settings()
