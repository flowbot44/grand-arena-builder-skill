from __future__ import annotations

from datetime import date, datetime, timezone


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def utc_today_iso() -> str:
    return utc_today().isoformat()
