from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Any, Dict, Optional

from flask import Flask, Response, abort, jsonify, render_template_string, request

from .config import SETTINGS
from .feed_adapter import FeedAdapter, FeedMeta, FeedUnavailableError
from .time_utils import utc_today_iso


def _json_with_meta(payload: Dict[str, Any], meta: Optional[FeedMeta] = None, status_code: int = 200) -> Response:
    if meta is not None:
        payload = dict(payload)
        payload.update(meta.as_body())
    response = Response(json.dumps(payload, default=str), mimetype="application/json", status=status_code)
    if meta is not None:
        for key, value in meta.as_headers().items():
            response.headers[key] = value
    return response

INDEX_TEMPLATE = """
<!doctype html>
<title>Grand Arena Champion Insights</title>
<style>
  body { background: #000; color: #39ff14; font-family: "Courier New", Courier, monospace; margin: 20px; }
  a { color: #7dff7a; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #39ff14; padding: 6px; }
  th { background: #001600; }
</style>
<h1>Grand Arena Champion Insights</h1>
<p><a href="/non-champions">Browse Non-Champions</a></p>
<p>Lookahead: {{ lookahead_days }} day(s), window {{ window_start }} to {{ window_end }}</p>
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Token</th>
      <th>Name</th>
      <th>Win %</th>
      <th>Avg Points</th>
      <th>Next Matches</th>
      <th>Detail</th>
    </tr>
  </thead>
  <tbody>
  {% for row in rows %}
    <tr>
      <td>{{ row.token_id }}</td>
      <td>{{ row.name }}</td>
      <td>{{ row.win_pct }}</td>
      <td>{{ row.avg_points }}</td>
      <td>{{ row.next_count }}</td>
      <td><a href="/champions/{{ row.token_id }}">open</a></td>
    </tr>
  {% endfor %}
  </tbody>
</table>
"""

NON_CHAMPIONS_TEMPLATE = """
<!doctype html>
<title>Grand Arena Non-Champions</title>
<style>
  body { background: #000; color: #39ff14; font-family: "Courier New", Courier, monospace; margin: 20px; }
  a { color: #7dff7a; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #39ff14; padding: 6px; }
  th { background: #001600; }
</style>
<h1>Grand Arena Non-Champions</h1>
<p><a href="/">Back to Champions</a></p>
<p>Page {{ page }} of {{ total_pages }} | Total non-champions: {{ total_items }}</p>
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Token</th>
      <th>Name</th>
      <th>Games</th>
      <th>Win %</th>
      <th>Avg Points (calc)</th>
      <th>Detail</th>
    </tr>
  </thead>
  <tbody>
  {% for row in rows %}
    <tr>
      <td>{{ row.token_id }}</td>
      <td>{{ row.name }}</td>
      <td>{{ row.games }}</td>
      <td>{{ '%.2f'|format(row.win_pct * 100) }}%</td>
      <td>{{ '%.2f'|format(row.avg_points) }}</td>
      <td><a href="/non-champions/{{ row.token_id }}">open</a></td>
    </tr>
  {% endfor %}
  </tbody>
</table>
<p>
  {% if page > 1 %}
    <a href="/non-champions?page={{ page - 1 }}&per_page={{ per_page }}">Prev</a>
  {% endif %}
  {% if page < total_pages %}
    <a href="/non-champions?page={{ page + 1 }}&per_page={{ per_page }}">Next</a>
  {% endif %}
</p>
"""

DETAIL_TEMPLATE = """
<!doctype html>
<title>{{ champion.name }}</title>
<style>
  body { background: #000; color: #39ff14; font-family: "Courier New", Courier, monospace; margin: 20px; }
  a { color: #7dff7a; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #39ff14; padding: 6px; }
  th { background: #001600; }
</style>
<h1>{{ champion.name }} (#{{ champion.token_id }})</h1>
<p>
  <a href="/champions/{{ champion.token_id }}?tab=history">History</a> |
  <a href="/champions/{{ champion.token_id }}?tab=lookahead">Lookahead</a> |
  <a href="/champions/{{ champion.token_id }}?tab=match-info">Match Info</a>
</p>
<p>
  Win %: {{ '%.2f'|format(history.summary.win_pct * 100) }}% |
  Win Type % (of wins): Gacha {{ '%.2f'|format(history.summary.win_type_pct.gacha * 100) }}%,
  Eliminations {{ '%.2f'|format(history.summary.win_type_pct.eliminations * 100) }}%,
  Wart {{ '%.2f'|format(history.summary.win_type_pct.wart * 100) }}%
</p>
<p>
  Avg Points (calc): {{ '%.2f'|format(history.summary.avg_points) }} |
  Avg Deposits: {{ '%.2f'|format(history.summary.avg_deposits) }} |
  Avg Eliminations: {{ '%.2f'|format(history.summary.avg_eliminations) }} |
  Avg Wart: {{ '%.2f'|format(history.summary.avg_wart_distance) }}
</p>
{% if tab == "lookahead" %}
<p>Lookahead: {{ lookahead.lookahead_days }} day(s), window {{ lookahead.window_start }} to {{ lookahead.window_end }}</p>
<p>Insufficient upcoming (<10): {{ lookahead.insufficient_upcoming }}</p>
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Date</th>
      <th>Match ID</th>
      <th>Support</th>
      <th>Opp Champion</th>
      <th>Opp Support</th>
      <th>Edge</th>
    </tr>
  </thead>
  <tbody>
  {% for match in lookahead.matches %}
    <tr>
      <td>{{ match.match_date }}</td>
      <td>{{ match.match_id }}</td>
      <td>{{ '%.3f'|format(match.components.team_support_win_pct) }}</td>
      <td>
        {% for oc in match.opponent_champions %}
          #{{ oc.token_id }} {{ oc.name }} ({{ '%.3f'|format(oc.win_pct) }})
        {% endfor %}
      </td>
      <td>{{ '%.3f'|format(match.components.opponent_team_support_win_pct) }}</td>
      <td>{{ match.edge_label }} ({{ '%.3f'|format(match.edge_score) }})</td>
    </tr>
  {% endfor %}
  </tbody>
</table>
{% elif tab == "match-info" %}
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Date</th>
      <th>Match ID</th>
      <th>Result</th>
      <th>Win Type</th>
      <th>Champion Class</th>
      <th>Team NC Classes</th>
      <th>Opp Champ Classes</th>
      <th>Opp NC Classes</th>
    </tr>
  </thead>
  <tbody>
  {% for row in match_info.matches %}
    <tr>
      <td>{{ row.match_date }}</td>
      <td>{{ row.match_id }}</td>
      <td>{{ row.result }}</td>
      <td>{{ row.win_type }}</td>
      <td>{{ row.champion.classes|join(', ') }}</td>
      <td>{{ row.team_non_champion.classes|join(', ') }}</td>
      <td>{{ row.opponent_champion.classes|join(', ') }}</td>
      <td>{{ row.opponent_non_champion.classes|join(', ') }}</td>
    </tr>
  {% endfor %}
  </tbody>
  <tfoot>
    <tr>
      <th colspan="4">Combined Totals (All Matches)</th>
      <th>{{ match_info.combined_classes.champion.unique_classes|join(', ') }}</th>
      <th>{{ match_info.combined_classes.team_non_champion.unique_classes|join(', ') }}</th>
      <th>{{ match_info.combined_classes.opponent_champion.unique_classes|join(', ') }}</th>
      <th>{{ match_info.combined_classes.opponent_non_champion.unique_classes|join(', ') }}</th>
    </tr>
  </tfoot>
</table>
{% else %}
<p>Scored games: {{ history.totals.games }} | Wins: {{ history.totals.wins }} | Losses: {{ history.totals.losses }}</p>
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Date</th>
      <th>Match ID</th>
      <th>Result</th>
      <th>Win Type</th>
      <th>Points</th>
      <th>Deposits</th>
      <th>Eliminations</th>
      <th>Wart Distance</th>
    </tr>
  </thead>
  <tbody>
  {% for game in history.games %}
    <tr>
      <td>{{ game.match_date }}</td>
      <td>{{ game.match_id }}</td>
      <td>{{ game.result }}</td>
      <td>{{ game.win_type }}</td>
      <td>{{ '%.2f'|format(game.points) }}</td>
      <td>{{ '%.2f'|format(game.deposits) }}</td>
      <td>{{ '%.2f'|format(game.eliminations) }}</td>
      <td>{{ '%.2f'|format(game.wart_distance) }}</td>
    </tr>
  {% endfor %}
  </tbody>
  <tfoot>
    <tr>
      <th colspan="4">Totals</th>
      <th>{{ '%.2f'|format(history.totals.points) }}</th>
      <th>{{ '%.2f'|format(history.totals.deposits) }}</th>
      <th>{{ '%.2f'|format(history.totals.eliminations) }}</th>
      <th>{{ '%.2f'|format(history.totals.wart_distance) }}</th>
    </tr>
  </tfoot>
</table>
{% endif %}
"""

NON_CHAMPION_DETAIL_TEMPLATE = """
<!doctype html>
<title>{{ player.name }}</title>
<style>
  body { background: #000; color: #39ff14; font-family: "Courier New", Courier, monospace; margin: 20px; }
  a { color: #7dff7a; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #39ff14; padding: 6px; }
  th { background: #001600; }
</style>
<h1>{{ player.name }} (#{{ player.token_id }})</h1>
<p>
  <a href="/non-champions">Back to Non-Champions</a> |
  <a href="/non-champions/{{ player.token_id }}?tab=history">History</a> |
  <a href="/non-champions/{{ player.token_id }}?tab=lookahead">Lookahead</a>
</p>
<p>
  Win %: {{ '%.2f'|format(history.summary.win_pct * 100) }}% |
  Win Type % (of wins): Gacha {{ '%.2f'|format(history.summary.win_type_pct.gacha * 100) }}%,
  Eliminations {{ '%.2f'|format(history.summary.win_type_pct.eliminations * 100) }}%,
  Wart {{ '%.2f'|format(history.summary.win_type_pct.wart * 100) }}%
</p>
<p>
  Avg Points (calc): {{ '%.2f'|format(history.summary.avg_points) }} |
  Avg Deposits: {{ '%.2f'|format(history.summary.avg_deposits) }} |
  Avg Eliminations: {{ '%.2f'|format(history.summary.avg_eliminations) }} |
  Avg Wart: {{ '%.2f'|format(history.summary.avg_wart_distance) }}
</p>
{% if tab == "lookahead" %}
<p>Lookahead: {{ lookahead.lookahead_days }} day(s), window {{ lookahead.window_start }} to {{ lookahead.window_end }}</p>
<p>Insufficient upcoming (<10): {{ lookahead.insufficient_upcoming }}</p>
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Date</th>
      <th>Match ID</th>
      <th>Team Champion(s)</th>
      <th>Opp Champion(s)</th>
    </tr>
  </thead>
  <tbody>
  {% for match in lookahead.matches %}
    <tr>
      <td>{{ match.match_date }}</td>
      <td>{{ match.match_id }}</td>
      <td>
        {% for c in match.team_champions %}
          #{{ c.token_id }} {{ c.name }}
        {% endfor %}
      </td>
      <td>
        {% for c in match.opponent_champions %}
          #{{ c.token_id }} {{ c.name }}
        {% endfor %}
      </td>
    </tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<p>Scored games: {{ history.totals.games }} | Wins: {{ history.totals.wins }} | Losses: {{ history.totals.losses }}</p>
<table border="1" cellpadding="6" cellspacing="0">
  <thead>
    <tr>
      <th>Date</th>
      <th>Match ID</th>
      <th>Result</th>
      <th>Win Type</th>
      <th>Points</th>
      <th>Deposits</th>
      <th>Eliminations</th>
      <th>Wart Distance</th>
    </tr>
  </thead>
  <tbody>
  {% for game in history.games %}
    <tr>
      <td>{{ game.match_date }}</td>
      <td>{{ game.match_id }}</td>
      <td>{{ game.result }}</td>
      <td>{{ game.win_type }}</td>
      <td>{{ '%.2f'|format(game.points) }}</td>
      <td>{{ '%.2f'|format(game.deposits) }}</td>
      <td>{{ '%.2f'|format(game.eliminations) }}</td>
      <td>{{ '%.2f'|format(game.wart_distance) }}</td>
    </tr>
  {% endfor %}
  </tbody>
  <tfoot>
    <tr>
      <th colspan="4">Totals</th>
      <th>{{ '%.2f'|format(history.totals.points) }}</th>
      <th>{{ '%.2f'|format(history.totals.deposits) }}</th>
      <th>{{ '%.2f'|format(history.totals.eliminations) }}</th>
      <th>{{ '%.2f'|format(history.totals.wart_distance) }}</th>
    </tr>
  </tfoot>
</table>
{% endif %}
"""


def _json_response(payload: dict) -> Response:
    return _json_with_meta(payload)


def create_app() -> Flask:
    app = Flask(__name__)
    if os.getenv("VERCEL") and not SETTINGS.feed_base_url:
        raise RuntimeError("FEED_BASE_URL is required in Vercel environment")
    feed_adapter = FeedAdapter(
        base_url=SETTINGS.feed_base_url,
        ttl_seconds=SETTINGS.feed_ttl_seconds,
        timeout_seconds=SETTINGS.feed_http_timeout_seconds,
    )
    def _feed_error(exc: FeedUnavailableError) -> Response:
        return _json_with_meta(
            {"error": "feed_unavailable", "message": str(exc), "retry_hint": "retry in 30-60 seconds"},
            status_code=503,
        )

    @app.get("/")
    def index() -> str:
        try:
            feed_payload, _meta = feed_adapter.champion_rows()
            return render_template_string(
                INDEX_TEMPLATE,
                rows=feed_payload["rows"],
                lookahead_days=feed_payload["lookahead_days"],
                window_start=feed_payload["window_start"],
                window_end=feed_payload["window_end"],
            )
        except FeedUnavailableError:
            abort(503)

    @app.get("/champions/<int:token_id>")
    def champion_detail(token_id: int) -> str:
        tab = request.args.get("tab", "history")
        if tab not in {"history", "lookahead", "match-info"}:
            tab = "history"
        try:
            history_payload, _ = feed_adapter.champion_history(token_id)
            lookahead_payload, _ = feed_adapter.champion_next_matches(token_id, limit=10, lookahead_days=SETTINGS.lookahead_days)
            match_info_payload, _ = feed_adapter.champion_match_info(token_id)
        except FeedUnavailableError:
            abort(503)
        if history_payload["champion"] is None:
            abort(404)
        return render_template_string(
            DETAIL_TEMPLATE,
            tab=tab,
            champion=history_payload["champion"],
            history=history_payload,
            lookahead=lookahead_payload,
            match_info=match_info_payload,
        )

    @app.get("/non-champions")
    def non_champions_index() -> str:
        page = max(1, int(request.args.get("page", "1")))
        per_page = int(request.args.get("per_page", "100"))
        per_page = max(10, min(per_page, 500))
        offset = (page - 1) * per_page
        try:
            rows, _meta = feed_adapter.non_champion_rows()
        except FeedUnavailableError:
            abort(503)
        total_items = len(rows)
        total_pages = max(1, (total_items + per_page - 1) // per_page)
        page_rows = rows[offset : offset + per_page]
        return render_template_string(
            NON_CHAMPIONS_TEMPLATE,
            rows=page_rows,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            total_items=total_items,
        )

    @app.get("/non-champions/<int:token_id>")
    def non_champion_detail(token_id: int) -> str:
        tab = request.args.get("tab", "history")
        if tab not in {"history", "lookahead"}:
            tab = "history"
        try:
            history_payload, _ = feed_adapter.non_champion_history(token_id)
            lookahead_payload, _ = feed_adapter.non_champion_next_matches(token_id, limit=10, lookahead_days=SETTINGS.lookahead_days)
        except FeedUnavailableError:
            abort(503)
        if history_payload["player"] is None:
            abort(404)
        return render_template_string(
            NON_CHAMPION_DETAIL_TEMPLATE,
            tab=tab,
            player=history_payload["player"],
            history=history_payload,
            lookahead=lookahead_payload,
        )

    @app.get("/api/champions")
    def champions_json() -> Response:
        try:
            feed_payload, meta = feed_adapter.champion_rows()
            payload = {
                "source": "github_feed",
                "lookahead_days": feed_payload["lookahead_days"],
                "window_start": feed_payload["window_start"],
                "window_end": feed_payload["window_end"],
                "data": [
                    {
                        "token_id": row["token_id"],
                        "name": row["name"],
                        "matches_played": row["matches_played"],
                        "wins": row["wins"],
                        "win_pct": row["win_pct"],
                        "avg_points": row["avg_points"],
                    }
                    for row in feed_payload["rows"]
                ],
            }
            return _json_with_meta(payload, meta)
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    @app.get("/api/non-champions")
    def non_champions_json() -> Response:
        page = max(1, int(request.args.get("page", "1")))
        per_page = int(request.args.get("per_page", "100"))
        per_page = max(10, min(per_page, 500))
        offset = (page - 1) * per_page
        try:
            rows, meta = feed_adapter.non_champion_rows()
            total_items = len(rows)
            page_rows = rows[offset : offset + per_page]
            payload = {
                "source": "github_feed",
                "page": page,
                "per_page": per_page,
                "total_items": total_items,
                "total_pages": max(1, (total_items + per_page - 1) // per_page),
                "data": page_rows,
            }
            return _json_with_meta(payload, meta)
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    @app.get("/api/champions/<int:token_id>/next-matches")
    def champion_next_matches_json(token_id: int) -> Response:
        limit = int(request.args.get("limit", "10"))
        try:
            payload, meta = feed_adapter.champion_next_matches(token_id, limit=limit, lookahead_days=SETTINGS.lookahead_days)
        except FeedUnavailableError as exc:
            return _feed_error(exc)
        if payload["champion"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_with_meta(payload, meta)

    @app.get("/api/champions/<int:token_id>/history")
    def champion_history_json(token_id: int) -> Response:
        try:
            payload, meta = feed_adapter.champion_history(token_id)
            if payload["champion"] is None:
                return jsonify({"error": "not_found"}), 404
            return _json_with_meta(payload, meta)
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    @app.get("/api/champions/<int:token_id>/match-info")
    def champion_match_info_json(token_id: int) -> Response:
        try:
            payload, meta = feed_adapter.champion_match_info(token_id)
        except FeedUnavailableError as exc:
            return _feed_error(exc)
        if payload["champion"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_with_meta(payload, meta)

    @app.get("/api/non-champions/<int:token_id>/history")
    def non_champion_history_json(token_id: int) -> Response:
        try:
            payload, meta = feed_adapter.non_champion_history(token_id)
            if payload["player"] is None:
                return jsonify({"error": "not_found"}), 404
            return _json_with_meta(payload, meta)
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    @app.get("/api/non-champions/<int:token_id>/next-matches")
    def non_champion_next_matches_json(token_id: int) -> Response:
        limit = int(request.args.get("limit", "10"))
        try:
            payload, meta = feed_adapter.non_champion_next_matches(token_id, limit=limit, lookahead_days=SETTINGS.lookahead_days)
        except FeedUnavailableError as exc:
            return _feed_error(exc)
        if payload["player"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_with_meta(payload, meta)

    @app.get("/api/cumulative/current-totals")
    def cumulative_current_totals_json() -> Response:
        try:
            rows, meta = feed_adapter.get_current_totals()
            return _json_with_meta({"source": "github_feed", "data": rows}, meta)
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    @app.get("/api/moki-totals")
    def moki_totals_json() -> Response:
        try:
            payload, meta = feed_adapter.get_moki_totals()
            return _json_with_meta(
                {
                    "source": "github_feed",
                    "count": payload.get("count", len(payload.get("data") or [])),
                    "data": payload.get("data", []),
                },
                meta,
            )
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    @app.get("/api/system/status")
    def system_status() -> Response:
        try:
            latest, latest_meta = feed_adapter.get_latest_manifest()
            status, status_meta = feed_adapter.get_status()
            available_dates = latest.get("available_dates", [])
            meta = FeedMeta(
                data_generated_at=status.get("generated_at_utc", latest.get("generated_at_utc")),
                cache_age_seconds=max(latest_meta.cache_age_seconds, status_meta.cache_age_seconds),
                stale_data=latest_meta.stale_data or status_meta.stale_data,
            )
            payload = {
                "source": "github_feed",
                "feed_base_url": SETTINGS.feed_base_url,
                "generated_at_utc": status.get("generated_at_utc", latest.get("generated_at_utc")),
                "window_days": latest.get("window_days", status.get("window_days", 7)),
                "lookahead_days": latest.get("lookahead_days", status.get("lookahead_days", SETTINGS.lookahead_days)),
                "window_start": status.get(
                    "window_start",
                    available_dates[0] if available_dates else utc_today_iso(),
                ),
                "window_end": status.get(
                    "window_end",
                    available_dates[-1] if available_dates else utc_today_iso(),
                ),
                "cumulative_window_end": status.get(
                    "cumulative_window_end",
                    available_dates[-1] if available_dates else utc_today_iso(),
                ),
                "active_window_start": status.get(
                    "active_window_start",
                    status.get("window_start", available_dates[0] if available_dates else utc_today_iso()),
                ),
                "active_window_end": status.get(
                    "active_window_end",
                    status.get("window_end", available_dates[-1] if available_dates else utc_today_iso()),
                ),
                "archive_window_start": status.get(
                    "archive_window_start",
                    available_dates[0] if available_dates else utc_today_iso(),
                ),
                "archive_window_end": status.get(
                    "archive_window_end",
                    available_dates[-1] if available_dates else utc_today_iso(),
                ),
                "raw_dates": status.get("raw_dates", available_dates),
                "latest_ingestion_run": status.get("latest_ingestion_run"),
                "latest_run": status.get("latest_ingestion_run"),
            }
            return _json_with_meta(payload, meta)
        except FeedUnavailableError as exc:
            return _feed_error(exc)

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local Flask app for champion matchup insights")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app = create_app()
    app.run(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
