from __future__ import annotations

import sqlite3
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .time_utils import utc_today


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def recompute_champion_metrics(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM champion_metrics")
    conn.execute(
        """
        INSERT INTO champion_metrics (
            token_id,
            matches_played,
            wins,
            win_pct,
            avg_points,
            avg_eliminations,
            avg_deposits,
            avg_wart_distance,
            updated_at
        )
        SELECT
            mp.token_id,
            COUNT(*) AS matches_played,
            SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS wins,
            CASE WHEN COUNT(*) > 0 THEN CAST(SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS REAL) / COUNT(*) ELSE NULL END AS win_pct,
            AVG(msp.points) AS avg_points,
            AVG(p.eliminations) AS avg_eliminations,
            AVG(p.deposits) AS avg_deposits,
            AVG(p.wart_distance) AS avg_wart_distance,
            ? AS updated_at
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id
        LEFT JOIN match_stats_players msp ON msp.match_id = mp.match_id AND msp.token_id = mp.token_id
        LEFT JOIN (
            SELECT
                match_id,
                token_id,
                AVG(eliminations) AS eliminations,
                AVG(deposits) AS deposits,
                AVG(wart_distance) AS wart_distance
            FROM performances
            GROUP BY match_id, token_id
        ) p ON p.match_id = mp.match_id AND p.token_id = mp.token_id
        WHERE mp.is_champion = 1
          AND m.state = 'scored'
          AND m.is_bye = 0
        GROUP BY mp.token_id
        """,
        (utc_now_iso(),),
    )
    conn.commit()


def _global_non_champion_win_pct(conn: sqlite3.Connection, token_id: int) -> Optional[float]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS matches_played,
            SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS wins
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE mp.token_id = ?
          AND mp.is_champion = 0
          AND m.state = 'scored'
          AND m.is_bye = 0
        """,
        (token_id,),
    ).fetchone()
    if not row or row["matches_played"] == 0:
        return None
    return float(row["wins"] or 0) / float(row["matches_played"])


def _global_player_win_pct(conn: sqlite3.Connection) -> float:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS matches_played,
            SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS wins
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.state = 'scored'
          AND m.is_bye = 0
        """
    ).fetchone()
    if not row or row["matches_played"] == 0:
        return 0.5
    return float(row["wins"] or 0) / float(row["matches_played"])


def _player_win_stats(conn: sqlite3.Connection, token_id: int) -> tuple[int, int]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS matches_played,
            SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS wins
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE mp.token_id = ?
          AND m.state = 'scored'
          AND m.is_bye = 0
        """,
        (token_id,),
    ).fetchone()
    if not row:
        return (0, 0)
    return (int(row["matches_played"] or 0), int(row["wins"] or 0))


def _smooth_win_pct(wins: int, games: int, global_mean: float, k: int = 20) -> float:
    return (float(wins) + (k * global_mean)) / (float(games) + k)


def _label_from_edge_v2(score_0_100: float) -> str:
    if score_0_100 >= 58.0:
        return "Strong Edge"
    if score_0_100 >= 53.0:
        return "Slight Edge"
    if score_0_100 >= 47.0:
        return "Neutral"
    return "Tough"


def _champion_metric(conn: sqlite3.Connection, token_id: int) -> Dict[str, float]:
    row = conn.execute(
        """
        SELECT win_pct, avg_points, avg_eliminations, avg_deposits, avg_wart_distance
        FROM champion_metrics
        WHERE token_id = ?
        """,
        (token_id,),
    ).fetchone()
    if not row:
        return {
            "win_pct": 0.5,
            "avg_points": 0.0,
            "avg_eliminations": 0.0,
            "avg_deposits": 0.0,
            "avg_wart_distance": 0.0,
        }
    return {
        "win_pct": float(row["win_pct"] or 0.5),
        "avg_points": float(row["avg_points"] or 0.0),
        "avg_eliminations": float(row["avg_eliminations"] or 0.0),
        "avg_deposits": float(row["avg_deposits"] or 0.0),
        "avg_wart_distance": float(row["avg_wart_distance"] or 0.0),
    }


def _points_bounds(conn: sqlite3.Connection) -> tuple[float, float]:
    row = conn.execute("SELECT MIN(avg_points) AS min_points, MAX(avg_points) AS max_points FROM champion_metrics").fetchone()
    if not row:
        return (0.0, 1.0)
    min_points = float(row["min_points"] or 0.0)
    max_points = float(row["max_points"] or 1.0)
    if max_points <= min_points:
        max_points = min_points + 1.0
    return (min_points, max_points)


def _label_from_score(score: float) -> str:
    if score >= 0.65:
        return "Strong Edge"
    if score >= 0.55:
        return "Slight Edge"
    if score >= 0.45:
        return "Neutral"
    return "Tough"


def build_champion_next_matches(
    conn: sqlite3.Connection,
    token_id: int,
    *,
    limit: int = 10,
    today: Optional[date] = None,
    lookahead_days: int = 2,
) -> Dict[str, Any]:
    today = today or utc_today()
    window_start = today.isoformat()
    window_end = (today + timedelta(days=lookahead_days)).isoformat()

    champion = conn.execute("SELECT token_id, name FROM champions WHERE token_id = ?", (token_id,)).fetchone()
    if not champion:
        return {
            "champion": None,
            "lookahead_days": lookahead_days,
            "window_start": window_start,
            "window_end": window_end,
            "matches": [],
            "insufficient_upcoming": True,
            "generated_at": utc_now_iso(),
        }

    min_points, max_points = _points_bounds(conn)
    champ_metric = _champion_metric(conn, token_id)
    points_norm = (champ_metric["avg_points"] - min_points) / (max_points - min_points)
    global_mean_win_pct = _global_player_win_pct(conn)
    player_stats_cache: Dict[int, tuple[int, int]] = {}

    def get_player_smoothed_win_pct(player_token_id: int) -> float:
        if player_token_id not in player_stats_cache:
            player_stats_cache[player_token_id] = _player_win_stats(conn, player_token_id)
        games, wins = player_stats_cache[player_token_id]
        return _smooth_win_pct(wins=wins, games=games, global_mean=global_mean_win_pct, k=20)

    rows = conn.execute(
        """
        SELECT m.match_id, m.match_date, m.updated_at, mp.team
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        WHERE mp.token_id = ?
          AND m.state = 'scheduled'
          AND m.match_date >= ?
          AND m.match_date <= ?
        ORDER BY m.match_date ASC, m.updated_at ASC
        LIMIT ?
        """,
        (token_id, window_start, window_end, limit),
    ).fetchall()

    out_matches: List[Dict[str, Any]] = []
    for row in rows:
        match_id = row["match_id"]
        champion_team = row["team"]

        opponent_champ_rows = conn.execute(
            """
            SELECT token_id, name
            FROM match_players
            WHERE match_id = ? AND team != ? AND is_champion = 1
            ORDER BY token_id
            """,
            (match_id, champion_team),
        ).fetchall()

        teammate_rows = conn.execute(
            """
            SELECT token_id, name
            FROM match_players
            WHERE match_id = ? AND team = ? AND is_champion = 0
            ORDER BY token_id
            """,
            (match_id, champion_team),
        ).fetchall()

        opponent_scores: List[float] = []
        opponent_champions = []
        for oc in opponent_champ_rows:
            metric = _champion_metric(conn, int(oc["token_id"]))
            opponent_scores.append(metric["win_pct"])
            opponent_champions.append(
                {
                    "token_id": int(oc["token_id"]),
                    "name": oc["name"],
                    "win_pct": metric["win_pct"],
                }
            )

        teammate_scores: List[float] = []
        teammate_non_champions = []
        for tm in teammate_rows:
            win_pct = _global_non_champion_win_pct(conn, int(tm["token_id"]))
            teammate_non_champions.append(
                {
                    "token_id": int(tm["token_id"]),
                    "name": tm["name"],
                    "global_win_pct": win_pct,
                }
            )
            if win_pct is not None:
                teammate_scores.append(win_pct)

        opponent_teammate_rows = conn.execute(
            """
            SELECT token_id, name
            FROM match_players
            WHERE match_id = ? AND team != ? AND is_champion = 0
            ORDER BY token_id
            """,
            (match_id, champion_team),
        ).fetchall()

        opponent_teammate_scores: List[float] = []
        opponent_teammates_non_champions = []
        for tm in opponent_teammate_rows:
            win_pct = _global_non_champion_win_pct(conn, int(tm["token_id"]))
            opponent_teammates_non_champions.append(
                {
                    "token_id": int(tm["token_id"]),
                    "name": tm["name"],
                    "global_win_pct": win_pct,
                }
            )
            if win_pct is not None:
                opponent_teammate_scores.append(win_pct)

        team_support = sum(teammate_scores) / len(teammate_scores) if teammate_scores else 0.5
        opponent_team_support = (
            sum(opponent_teammate_scores) / len(opponent_teammate_scores)
            if opponent_teammate_scores
            else 0.5
        )
        opp_win = sum(opponent_scores) / len(opponent_scores) if opponent_scores else 0.5

        edge_score_v1 = (
            0.35 * champ_metric["win_pct"]
            + 0.25 * team_support
            - 0.25 * opp_win
            + 0.15 * points_norm
        )

        team_rows = conn.execute(
            """
            SELECT token_id, name, is_champion
            FROM match_players
            WHERE match_id = ? AND team = ?
            ORDER BY is_champion DESC, token_id ASC
            """,
            (match_id, champion_team),
        ).fetchall()
        opp_team_rows = conn.execute(
            """
            SELECT token_id, name, is_champion
            FROM match_players
            WHERE match_id = ? AND team != ?
            ORDER BY is_champion DESC, token_id ASC
            """,
            (match_id, champion_team),
        ).fetchall()

        def weighted_team_strength(players: List[sqlite3.Row]) -> tuple[float, List[Dict[str, Any]]]:
            weighted_rows: List[Dict[str, Any]] = []
            weight_sum = 0.0
            for p in players:
                base_weight = 0.65 if int(p["is_champion"]) == 1 else 0.175
                smoothed = get_player_smoothed_win_pct(int(p["token_id"]))
                weighted_rows.append(
                    {
                        "token_id": int(p["token_id"]),
                        "name": p["name"],
                        "is_champion": bool(int(p["is_champion"])),
                        "base_weight": base_weight,
                        "smoothed_win_pct": round(smoothed, 4),
                    }
                )
                weight_sum += base_weight
            if weight_sum <= 0:
                return (0.5, weighted_rows)

            strength = 0.0
            for item in weighted_rows:
                normalized_weight = item["base_weight"] / weight_sum
                contribution = normalized_weight * item["smoothed_win_pct"]
                item["weight"] = round(normalized_weight, 4)
                item["contribution"] = round(contribution, 4)
                strength += contribution
            return (strength, weighted_rows)

        team_strength, team_weighted_players = weighted_team_strength(team_rows)
        opp_team_strength, opp_team_weighted_players = weighted_team_strength(opp_team_rows)
        edge_v2_raw = team_strength - opp_team_strength
        edge_v2_score = (edge_v2_raw + 1.0) * 50.0

        out_matches.append(
            {
                "match_id": match_id,
                "match_date": row["match_date"],
                "opponent_champions": opponent_champions,
                "teammates_nonchampions": teammate_non_champions,
                "opponent_teammates_nonchampions": opponent_teammates_non_champions,
                "edge_score": round(edge_v2_score, 2),
                "edge_label": _label_from_edge_v2(edge_v2_score),
                "components": {
                    "edge_v1_score": round(edge_score_v1, 4),
                    "champion_win_pct": round(champ_metric["win_pct"], 4),
                    "team_support_win_pct": round(team_support, 4),
                    "opponent_team_support_win_pct": round(opponent_team_support, 4),
                    "opponent_champion_win_pct": round(opp_win, 4),
                    "normalized_points_component": round(points_norm, 4),
                    "edge_v2_raw": round(edge_v2_raw, 4),
                    "team_strength": round(team_strength, 4),
                    "opponent_team_strength": round(opp_team_strength, 4),
                    "global_mean_win_pct": round(global_mean_win_pct, 4),
                    "team_player_weights": team_weighted_players,
                    "opponent_player_weights": opp_team_weighted_players,
                },
            }
        )

    return {
        "champion": {"token_id": int(champion["token_id"]), "name": champion["name"]},
        "lookahead_days": lookahead_days,
        "window_start": window_start,
        "window_end": window_end,
        "matches": out_matches,
        "insufficient_upcoming": len(out_matches) < limit,
        "generated_at": utc_now_iso(),
    }


def build_champion_history(conn: sqlite3.Connection, token_id: int) -> Dict[str, Any]:
    champion = conn.execute("SELECT token_id, name FROM champions WHERE token_id = ?", (token_id,)).fetchone()
    if not champion:
        return {
            "champion": None,
            "games": [],
            "totals": {
                "games": 0,
                "wins": 0,
                "losses": 0,
                "points": 0.0,
                "deposits": 0.0,
                "eliminations": 0.0,
                "wart_distance": 0.0,
            },
            "generated_at": utc_now_iso(),
        }

    rows = conn.execute(
        """
        SELECT
            m.match_id,
            m.match_date,
            m.state,
            m.team_won,
            mp.team,
            m.win_type,
            COALESCE(p.deposits, msp.deposits, 0) AS deposits,
            COALESCE(p.eliminations, msp.eliminations, 0) AS eliminations,
            COALESCE(p.wart_distance, msp.wart_distance, 0) AS wart_distance
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        LEFT JOIN match_stats_players msp
            ON msp.match_id = m.match_id AND msp.token_id = mp.token_id
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
        WHERE mp.token_id = ?
          AND m.state = 'scored'
        ORDER BY m.match_date DESC, m.updated_at DESC
        """,
        (token_id,),
    ).fetchall()

    games: List[Dict[str, Any]] = []
    total_wins = 0
    total_losses = 0
    total_points = 0.0
    total_deposits = 0.0
    total_elims = 0.0
    total_wart = 0.0
    win_type_counts = {
        "gacha": 0,
        "eliminations": 0,
        "wart": 0,
    }

    for row in rows:
        won = row["team_won"] == row["team"] if row["team_won"] is not None else None
        if won is True:
            total_wins += 1
        elif won is False:
            total_losses += 1

        deposits = float(row["deposits"] or 0.0)
        eliminations = float(row["eliminations"] or 0.0)
        wart_distance = float(row["wart_distance"] or 0.0)
        wart_points = math.floor(wart_distance / 80.0) * 45.0
        points = (deposits * 50.0) + (eliminations * 80.0) + wart_points + (300.0 if won else 0.0)

        total_points += points
        total_deposits += deposits
        total_elims += eliminations
        total_wart += wart_distance
        if won is True and row["win_type"] in win_type_counts:
            win_type_counts[str(row["win_type"])] += 1

        games.append(
            {
                "match_id": row["match_id"],
                "match_date": row["match_date"],
                "result": "W" if won is True else ("L" if won is False else "-"),
                "win_type": row["win_type"],
                "points": round(points, 2),
                "deposits": round(deposits, 2),
                "eliminations": round(eliminations, 2),
                "wart_distance": round(wart_distance, 2),
            }
        )

    games_count = len(games)
    win_pct = (float(total_wins) / float(games_count)) if games_count > 0 else 0.0
    avg_points = (total_points / games_count) if games_count > 0 else 0.0
    avg_deposits = (total_deposits / games_count) if games_count > 0 else 0.0
    avg_eliminations = (total_elims / games_count) if games_count > 0 else 0.0
    avg_wart = (total_wart / games_count) if games_count > 0 else 0.0
    win_total = float(total_wins) if total_wins > 0 else 1.0

    return {
        "champion": {"token_id": int(champion["token_id"]), "name": champion["name"]},
        "games": games,
        "summary": {
            "win_pct": round(win_pct, 4),
            "avg_points": round(avg_points, 2),
            "avg_deposits": round(avg_deposits, 2),
            "avg_eliminations": round(avg_eliminations, 2),
            "avg_wart_distance": round(avg_wart, 2),
            "win_type_pct": {
                "gacha": round(float(win_type_counts["gacha"]) / win_total, 4),
                "eliminations": round(float(win_type_counts["eliminations"]) / win_total, 4),
                "wart": round(float(win_type_counts["wart"]) / win_total, 4),
            },
        },
        "totals": {
            "games": games_count,
            "wins": total_wins,
            "losses": total_losses,
            "points": round(total_points, 2),
            "deposits": round(total_deposits, 2),
            "eliminations": round(total_elims, 2),
            "wart_distance": round(total_wart, 2),
        },
        "generated_at": utc_now_iso(),
    }


def build_non_champion_history(conn: sqlite3.Connection, token_id: int) -> Dict[str, Any]:
    name_row = conn.execute(
        """
        SELECT name
        FROM match_players
        WHERE token_id = ? AND is_champion = 0
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (token_id,),
    ).fetchone()
    if not name_row:
        return {
            "player": None,
            "games": [],
            "totals": {
                "games": 0,
                "wins": 0,
                "losses": 0,
                "points": 0.0,
                "deposits": 0.0,
                "eliminations": 0.0,
                "wart_distance": 0.0,
            },
            "generated_at": utc_now_iso(),
        }

    rows = conn.execute(
        """
        SELECT
            m.match_id,
            m.match_date,
            m.team_won,
            mp.team,
            m.win_type,
            COALESCE(p.deposits, msp.deposits, 0) AS deposits,
            COALESCE(p.eliminations, msp.eliminations, 0) AS eliminations,
            COALESCE(p.wart_distance, msp.wart_distance, 0) AS wart_distance
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        LEFT JOIN match_stats_players msp
            ON msp.match_id = m.match_id AND msp.token_id = mp.token_id
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
        WHERE mp.token_id = ?
          AND mp.is_champion = 0
          AND m.state = 'scored'
        ORDER BY m.match_date DESC, m.updated_at DESC
        """,
        (token_id,),
    ).fetchall()

    games: List[Dict[str, Any]] = []
    total_wins = 0
    total_losses = 0
    total_points = 0.0
    total_deposits = 0.0
    total_elims = 0.0
    total_wart = 0.0
    win_type_counts = {"gacha": 0, "eliminations": 0, "wart": 0}

    for row in rows:
        won = row["team_won"] == row["team"] if row["team_won"] is not None else None
        if won is True:
            total_wins += 1
        elif won is False:
            total_losses += 1

        deposits = float(row["deposits"] or 0.0)
        eliminations = float(row["eliminations"] or 0.0)
        wart_distance = float(row["wart_distance"] or 0.0)
        wart_points = math.floor(wart_distance / 80.0) * 45.0
        points = (deposits * 50.0) + (eliminations * 80.0) + wart_points + (300.0 if won else 0.0)

        total_points += points
        total_deposits += deposits
        total_elims += eliminations
        total_wart += wart_distance
        if won is True and row["win_type"] in win_type_counts:
            win_type_counts[str(row["win_type"])] += 1

        games.append(
            {
                "match_id": row["match_id"],
                "match_date": row["match_date"],
                "result": "W" if won is True else ("L" if won is False else "-"),
                "win_type": row["win_type"],
                "points": round(points, 2),
                "deposits": round(deposits, 2),
                "eliminations": round(eliminations, 2),
                "wart_distance": round(wart_distance, 2),
            }
        )

    games_count = len(games)
    win_pct = (float(total_wins) / float(games_count)) if games_count > 0 else 0.0
    avg_points = (total_points / games_count) if games_count > 0 else 0.0
    avg_deposits = (total_deposits / games_count) if games_count > 0 else 0.0
    avg_eliminations = (total_elims / games_count) if games_count > 0 else 0.0
    avg_wart = (total_wart / games_count) if games_count > 0 else 0.0
    win_total = float(total_wins) if total_wins > 0 else 1.0

    return {
        "player": {"token_id": int(token_id), "name": name_row["name"]},
        "games": games,
        "summary": {
            "win_pct": round(win_pct, 4),
            "avg_points": round(avg_points, 2),
            "avg_deposits": round(avg_deposits, 2),
            "avg_eliminations": round(avg_eliminations, 2),
            "avg_wart_distance": round(avg_wart, 2),
            "win_type_pct": {
                "gacha": round(float(win_type_counts["gacha"]) / win_total, 4),
                "eliminations": round(float(win_type_counts["eliminations"]) / win_total, 4),
                "wart": round(float(win_type_counts["wart"]) / win_total, 4),
            },
        },
        "totals": {
            "games": games_count,
            "wins": total_wins,
            "losses": total_losses,
            "points": round(total_points, 2),
            "deposits": round(total_deposits, 2),
            "eliminations": round(total_elims, 2),
            "wart_distance": round(total_wart, 2),
        },
        "generated_at": utc_now_iso(),
    }


def build_non_champion_next_matches(
    conn: sqlite3.Connection,
    token_id: int,
    *,
    limit: int = 10,
    today: Optional[date] = None,
    lookahead_days: int = 2,
) -> Dict[str, Any]:
    today = today or utc_today()
    window_start = today.isoformat()
    window_end = (today + timedelta(days=lookahead_days)).isoformat()

    name_row = conn.execute(
        """
        SELECT name
        FROM match_players
        WHERE token_id = ? AND is_champion = 0
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (token_id,),
    ).fetchone()
    if not name_row:
        return {
            "player": None,
            "lookahead_days": lookahead_days,
            "window_start": window_start,
            "window_end": window_end,
            "matches": [],
            "insufficient_upcoming": True,
            "generated_at": utc_now_iso(),
        }

    rows = conn.execute(
        """
        SELECT m.match_id, m.match_date, m.updated_at, mp.team
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        WHERE mp.token_id = ?
          AND mp.is_champion = 0
          AND m.state = 'scheduled'
          AND m.match_date >= ?
          AND m.match_date <= ?
        ORDER BY m.match_date ASC, m.updated_at ASC
        LIMIT ?
        """,
        (token_id, window_start, window_end, limit),
    ).fetchall()

    out_matches: List[Dict[str, Any]] = []
    for row in rows:
        team = row["team"]
        team_champs = conn.execute(
            """
            SELECT token_id, name
            FROM match_players
            WHERE match_id = ? AND team = ? AND is_champion = 1
            ORDER BY token_id
            """,
            (row["match_id"], team),
        ).fetchall()
        opp_champs = conn.execute(
            """
            SELECT token_id, name
            FROM match_players
            WHERE match_id = ? AND team != ? AND is_champion = 1
            ORDER BY token_id
            """,
            (row["match_id"], team),
        ).fetchall()
        out_matches.append(
            {
                "match_id": row["match_id"],
                "match_date": row["match_date"],
                "team_champions": [{"token_id": int(x["token_id"]), "name": x["name"]} for x in team_champs],
                "opponent_champions": [{"token_id": int(x["token_id"]), "name": x["name"]} for x in opp_champs],
            }
        )

    return {
        "player": {"token_id": int(token_id), "name": name_row["name"]},
        "lookahead_days": lookahead_days,
        "window_start": window_start,
        "window_end": window_end,
        "matches": out_matches,
        "insufficient_upcoming": len(out_matches) < limit,
        "generated_at": utc_now_iso(),
    }


def _cohort_stats(rows: List[sqlite3.Row]) -> Dict[str, float]:
    count = len(rows)
    total_deposits = sum(float(r["deposits"] or 0.0) for r in rows)
    total_eliminations = sum(float(r["eliminations"] or 0.0) for r in rows)
    total_wart = sum(float(r["wart_distance"] or 0.0) for r in rows)
    if count == 0:
        return {
            "count": 0,
            "total_deposits": 0.0,
            "total_eliminations": 0.0,
            "total_wart_distance": 0.0,
            "avg_deposits": 0.0,
            "avg_eliminations": 0.0,
            "avg_wart_distance": 0.0,
        }
    return {
        "count": count,
        "total_deposits": round(total_deposits, 2),
        "total_eliminations": round(total_eliminations, 2),
        "total_wart_distance": round(total_wart, 2),
        "avg_deposits": round(total_deposits / count, 2),
        "avg_eliminations": round(total_eliminations / count, 2),
        "avg_wart_distance": round(total_wart / count, 2),
    }


def _cohort_classes(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    class_names = [str(r["class"]) for r in rows if r["class"] is not None]
    return {
        "count": len(rows),
        "classes": class_names,
        "unique_classes": sorted(set(class_names)),
    }


def build_champion_match_info(conn: sqlite3.Connection, token_id: int) -> Dict[str, Any]:
    champion = conn.execute("SELECT token_id, name FROM champions WHERE token_id = ?", (token_id,)).fetchone()
    if not champion:
        return {
            "champion": None,
            "matches": [],
            "combined_classes": {
                "champion": _cohort_classes([]),
                "team_non_champion": _cohort_classes([]),
                "opponent_champion": _cohort_classes([]),
                "opponent_non_champion": _cohort_classes([]),
            },
            "generated_at": utc_now_iso(),
        }

    matches = conn.execute(
        """
        SELECT m.match_id, m.match_date, m.team_won, m.win_type, mp.team AS champion_team
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        WHERE mp.token_id = ?
          AND mp.is_champion = 1
          AND m.state = 'scored'
        ORDER BY m.match_date DESC, m.updated_at DESC
        """,
        (token_id,),
    ).fetchall()

    out: List[Dict[str, Any]] = []
    combined_champion_rows: List[sqlite3.Row] = []
    combined_team_non_champion: List[sqlite3.Row] = []
    combined_opponent_champion: List[sqlite3.Row] = []
    combined_opponent_non_champion: List[sqlite3.Row] = []
    for m in matches:
        player_rows = conn.execute(
            """
            SELECT
                mp.team,
                mp.is_champion,
                mp.class,
                COALESCE(p.deposits, msp.deposits, 0) AS deposits,
                COALESCE(p.eliminations, msp.eliminations, 0) AS eliminations,
                COALESCE(p.wart_distance, msp.wart_distance, 0) AS wart_distance
            FROM match_players mp
            LEFT JOIN match_stats_players msp
                ON msp.match_id = mp.match_id AND msp.token_id = mp.token_id
            LEFT JOIN (
                SELECT
                    match_id,
                    token_id,
                    AVG(deposits) AS deposits,
                    AVG(eliminations) AS eliminations,
                    AVG(wart_distance) AS wart_distance
                FROM performances
                GROUP BY match_id, token_id
            ) p ON p.match_id = mp.match_id AND p.token_id = mp.token_id
            WHERE mp.match_id = ?
            """,
            (m["match_id"],),
        ).fetchall()

        champ_team = int(m["champion_team"])
        team_champion_rows = [r for r in player_rows if int(r["team"]) == champ_team and int(r["is_champion"]) == 1]
        teammate_non_champs = [r for r in player_rows if int(r["team"]) == champ_team and int(r["is_champion"]) == 0]
        opp_champs = [r for r in player_rows if int(r["team"]) != champ_team and int(r["is_champion"]) == 1]
        opp_non_champs = [r for r in player_rows if int(r["team"]) != champ_team and int(r["is_champion"]) == 0]
        combined_champion_rows.extend(team_champion_rows)
        combined_team_non_champion.extend(teammate_non_champs)
        combined_opponent_champion.extend(opp_champs)
        combined_opponent_non_champion.extend(opp_non_champs)

        won = m["team_won"] == champ_team if m["team_won"] is not None else None
        out.append(
            {
                "match_id": m["match_id"],
                "match_date": m["match_date"],
                "result": "W" if won is True else ("L" if won is False else "-"),
                "win_type": m["win_type"],
                "champion": _cohort_classes(team_champion_rows),
                "team_non_champion": _cohort_classes(teammate_non_champs),
                "opponent_champion": _cohort_classes(opp_champs),
                "opponent_non_champion": _cohort_classes(opp_non_champs),
            }
        )

    return {
        "champion": {"token_id": int(champion["token_id"]), "name": champion["name"]},
        "matches": out,
        "combined_classes": {
            "champion": _cohort_classes(combined_champion_rows),
            "team_non_champion": _cohort_classes(combined_team_non_champion),
            "opponent_champion": _cohort_classes(combined_opponent_champion),
            "opponent_non_champion": _cohort_classes(combined_opponent_non_champion),
        },
        "generated_at": utc_now_iso(),
    }
