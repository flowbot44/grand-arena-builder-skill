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
    api_page_limit: int = int(os.getenv("API_PAGE_LIMIT", "100"))
    champion_only_matches: bool = _env_bool("CHAMPION_ONLY_MATCHES", True)
    fetch_match_performances: bool = _env_bool("FETCH_MATCH_PERFORMANCES", True)
    backfill_start_default: date = date(2026, 2, 19)


SETTINGS = Settings()
