from __future__ import annotations

import argparse
import json
from datetime import date

from flask import Flask, Response, abort, g, jsonify, render_template_string, request

from .analytics import (
    build_champion_match_info,
    build_champion_history,
    build_champion_next_matches,
    build_non_champion_history,
    build_non_champion_next_matches,
)
from .config import SETTINGS
from .db import get_connection, init_db

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
    return Response(json.dumps(payload, default=str), mimetype="application/json")


def create_app(db_path: str = SETTINGS.db_path) -> Flask:
    app = Flask(__name__)
    bootstrap_conn = get_connection(db_path)
    init_db(bootstrap_conn)
    bootstrap_conn.close()

    def get_db():
        conn = g.get("db_conn")
        if conn is None:
            conn = get_connection(db_path)
            g.db_conn = conn
        return conn

    @app.teardown_appcontext
    def close_db(_exc):
        conn = g.pop("db_conn", None)
        if conn is not None:
            conn.close()

    @app.get("/")
    def index() -> str:
        conn = get_db()
        today = date.today()
        window_start = today.isoformat()
        window_end = date.fromordinal(today.toordinal() + SETTINGS.lookahead_days).isoformat()

        rows = conn.execute(
            """
            SELECT
                c.token_id,
                c.name,
                ROUND(COALESCE(cm.win_pct, 0.0), 4) AS win_pct,
                ROUND(COALESCE(cm.avg_points, 0.0), 4) AS avg_points,
                (
                    SELECT COUNT(*)
                    FROM matches m
                    JOIN match_players mp ON mp.match_id = m.match_id
                    WHERE mp.token_id = c.token_id
                      AND m.state = 'scheduled'
                      AND m.match_date >= ?
                      AND m.match_date <= ?
                ) AS next_count
            FROM champions c
            LEFT JOIN champion_metrics cm ON cm.token_id = c.token_id
            ORDER BY win_pct DESC, c.token_id ASC
            """,
            (window_start, window_end),
        ).fetchall()
        return render_template_string(
            INDEX_TEMPLATE,
            rows=rows,
            lookahead_days=SETTINGS.lookahead_days,
            window_start=window_start,
            window_end=window_end,
        )

    @app.get("/champions/<int:token_id>")
    def champion_detail(token_id: int) -> str:
        conn = get_db()
        tab = request.args.get("tab", "history")
        if tab not in {"history", "lookahead", "match-info"}:
            tab = "history"

        history_payload = build_champion_history(conn, token_id)
        lookahead_payload = build_champion_next_matches(conn, token_id, limit=10, lookahead_days=SETTINGS.lookahead_days)
        match_info_payload = build_champion_match_info(conn, token_id)
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
        conn = get_db()
        page = max(1, int(request.args.get("page", "1")))
        per_page = int(request.args.get("per_page", "100"))
        per_page = max(10, min(per_page, 500))
        offset = (page - 1) * per_page

        total_items = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM (
              SELECT token_id
              FROM match_players
              WHERE is_champion = 0
              GROUP BY token_id
            )
            """
        ).fetchone()["c"]

        rows = conn.execute(
            """
            WITH perf AS (
                SELECT
                    mp.token_id,
                    MAX(mp.name) AS name,
                    COUNT(*) AS games,
                    SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS wins,
                    AVG(COALESCE(p.deposits, msp.deposits, 0)) AS avg_deposits,
                    AVG(COALESCE(p.eliminations, msp.eliminations, 0)) AS avg_eliminations,
                    AVG(COALESCE(p.wart_distance, msp.wart_distance, 0)) AS avg_wart
                FROM match_players mp
                JOIN matches m ON m.match_id = mp.match_id
                LEFT JOIN match_stats_players msp ON msp.match_id = m.match_id AND msp.token_id = mp.token_id
                LEFT JOIN (
                    SELECT
                        match_id,
                        token_id,
                        AVG(deposits) AS deposits,
                        AVG(eliminations) AS eliminations,
                        AVG(wart_distance) AS wart_distance
                    FROM performances
                    GROUP BY match_id, token_id
                ) p ON p.match_id = m.match_id AND p.token_id = mp.token_id
                WHERE mp.is_champion = 0
                  AND m.state = 'scored'
                GROUP BY mp.token_id
            )
            SELECT
                token_id,
                name,
                games,
                CASE WHEN games > 0 THEN CAST(wins AS REAL) / games ELSE 0.0 END AS win_pct,
                ((avg_deposits * 50.0) + (avg_eliminations * 80.0) + (CAST(avg_wart / 80.0 AS INTEGER) * 45.0) + (CASE WHEN games > 0 THEN (CAST(wins AS REAL) / games) * 300.0 ELSE 0.0 END)) AS avg_points
            FROM perf
            ORDER BY games DESC, token_id ASC
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        ).fetchall()

        total_pages = max(1, (total_items + per_page - 1) // per_page)
        return render_template_string(
            NON_CHAMPIONS_TEMPLATE,
            rows=rows,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            total_items=total_items,
        )

    @app.get("/non-champions/<int:token_id>")
    def non_champion_detail(token_id: int) -> str:
        conn = get_db()
        tab = request.args.get("tab", "history")
        if tab not in {"history", "lookahead"}:
            tab = "history"
        history_payload = build_non_champion_history(conn, token_id)
        lookahead_payload = build_non_champion_next_matches(conn, token_id, limit=10, lookahead_days=SETTINGS.lookahead_days)
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
        conn = get_db()
        rows = conn.execute(
            """
            SELECT
                c.token_id,
                c.name,
                COALESCE(cm.matches_played, 0) AS matches_played,
                COALESCE(cm.wins, 0) AS wins,
                COALESCE(cm.win_pct, 0.0) AS win_pct,
                COALESCE(cm.avg_points, 0.0) AS avg_points
            FROM champions c
            LEFT JOIN champion_metrics cm ON cm.token_id = c.token_id
            ORDER BY win_pct DESC, c.token_id ASC
            """
        ).fetchall()

        payload = {
            "lookahead_days": SETTINGS.lookahead_days,
            "window_start": date.today().isoformat(),
            "window_end": date.fromordinal(date.today().toordinal() + SETTINGS.lookahead_days).isoformat(),
            "data": [dict(r) for r in rows],
        }
        return _json_response(payload)

    @app.get("/api/non-champions")
    def non_champions_json() -> Response:
        conn = get_db()
        page = max(1, int(request.args.get("page", "1")))
        per_page = int(request.args.get("per_page", "100"))
        per_page = max(10, min(per_page, 500))
        offset = (page - 1) * per_page
        total_items = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM (
              SELECT token_id
              FROM match_players
              WHERE is_champion = 0
              GROUP BY token_id
            )
            """
        ).fetchone()["c"]
        rows = conn.execute(
            """
            SELECT token_id, MAX(name) AS name
            FROM match_players
            WHERE is_champion = 0
            GROUP BY token_id
            ORDER BY token_id ASC
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        ).fetchall()
        payload = {
            "page": page,
            "per_page": per_page,
            "total_items": total_items,
            "total_pages": max(1, (total_items + per_page - 1) // per_page),
            "data": [dict(r) for r in rows],
        }
        return _json_response(payload)

    @app.get("/api/champions/<int:token_id>/next-matches")
    def champion_next_matches_json(token_id: int) -> Response:
        conn = get_db()
        limit = int(request.args.get("limit", "10"))
        payload = build_champion_next_matches(conn, token_id, limit=limit, lookahead_days=SETTINGS.lookahead_days)
        if payload["champion"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_response(payload)

    @app.get("/api/champions/<int:token_id>/history")
    def champion_history_json(token_id: int) -> Response:
        conn = get_db()
        payload = build_champion_history(conn, token_id)
        if payload["champion"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_response(payload)

    @app.get("/api/champions/<int:token_id>/match-info")
    def champion_match_info_json(token_id: int) -> Response:
        conn = get_db()
        payload = build_champion_match_info(conn, token_id)
        if payload["champion"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_response(payload)

    @app.get("/api/non-champions/<int:token_id>/history")
    def non_champion_history_json(token_id: int) -> Response:
        conn = get_db()
        payload = build_non_champion_history(conn, token_id)
        if payload["player"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_response(payload)

    @app.get("/api/non-champions/<int:token_id>/next-matches")
    def non_champion_next_matches_json(token_id: int) -> Response:
        conn = get_db()
        limit = int(request.args.get("limit", "10"))
        payload = build_non_champion_next_matches(conn, token_id, limit=limit, lookahead_days=SETTINGS.lookahead_days)
        if payload["player"] is None:
            return jsonify({"error": "not_found"}), 404
        return _json_response(payload)

    @app.get("/api/system/status")
    def system_status() -> Response:
        conn = get_db()
        run = conn.execute(
            """
            SELECT run_id, started_at, finished_at, status
            FROM ingestion_runs
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        payload = {
            "lookahead_days": SETTINGS.lookahead_days,
            "latest_run": dict(run) if run else None,
        }
        return _json_response(payload)

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local Flask app for champion matchup insights")
    parser.add_argument("--db", default=SETTINGS.db_path, help="SQLite DB path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app = create_app(db_path=args.db)
    app.run(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
