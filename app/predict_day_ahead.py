from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from .config import SETTINGS
from .db import get_connection


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_hhmm_utc(value: str) -> int:
    try:
        hh_str, mm_str = value.split(":")
        hh = int(hh_str)
        mm = int(mm_str)
    except (ValueError, AttributeError) as exc:
        raise argparse.ArgumentTypeError("Expected HH:MM format for UTC time.") from exc
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise argparse.ArgumentTypeError("UTC time must be between 00:00 and 23:59.")
    return hh * 60 + mm


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def logit(p: float) -> float:
    p = min(0.999, max(0.001, p))
    return math.log(p / (1.0 - p))


def smooth_wr(wins: int, games: int, prior: float = 0.5, k: float = 20.0) -> float:
    return (float(wins) + (k * prior)) / (float(games) + k)


def strength(games: int, full_conf_at: int) -> float:
    if games <= 0:
        return 0.0
    return min(1.0, float(games) / float(full_conf_at))


@dataclass
class TeamSnapshot:
    team: int
    champion_token_id: Optional[int]
    champion_name: Optional[str]
    champion_class: Optional[str]
    comp: str


def build_team_comp(classes: List[str]) -> str:
    return "|".join(sorted(classes))


def load_class_stats(conn) -> Dict[str, Tuple[int, int]]:
    rows = conn.execute(
        """
        SELECT
            mp.class AS class_name,
            COUNT(*) AS games,
            SUM(CASE WHEN m.team_won = mp.team THEN 1 ELSE 0 END) AS wins
        FROM matches m
        JOIN match_players mp ON mp.match_id = m.match_id
        WHERE m.state = 'scored'
          AND m.is_bye = 0
          AND m.team_won IN (1, 2)
          AND mp.is_champion = 1
          AND mp.class IS NOT NULL
          AND mp.class != ''
        GROUP BY mp.class
        """
    ).fetchall()
    return {str(r["class_name"]): (int(r["games"]), int(r["wins"] or 0)) for r in rows}


def load_class_vs_class_stats(conn) -> Dict[Tuple[str, str], Tuple[int, int]]:
    rows = conn.execute(
        """
        WITH champs AS (
            SELECT match_id, team, class
            FROM match_players
            WHERE is_champion = 1
              AND class IS NOT NULL
              AND class != ''
        )
        SELECT
            a.class AS self_class,
            b.class AS opp_class,
            COUNT(*) AS games,
            SUM(CASE WHEN m.team_won = a.team THEN 1 ELSE 0 END) AS wins
        FROM matches m
        JOIN champs a ON a.match_id = m.match_id
        JOIN champs b ON b.match_id = m.match_id AND b.team != a.team
        WHERE m.state = 'scored'
          AND m.is_bye = 0
          AND m.team_won IN (1, 2)
        GROUP BY a.class, b.class
        """
    ).fetchall()
    return {(str(r["self_class"]), str(r["opp_class"])): (int(r["games"]), int(r["wins"] or 0)) for r in rows}


def load_comp_stats(conn) -> Dict[str, Tuple[int, int]]:
    rows = conn.execute(
        """
        WITH team_comps AS (
            SELECT
                m.match_id,
                p.team,
                (
                    SELECT group_concat(class_name, '|')
                    FROM (
                        SELECT p2.class AS class_name
                        FROM match_players p2
                        WHERE p2.match_id = p.match_id
                          AND p2.team = p.team
                          AND p2.class IS NOT NULL
                          AND p2.class != ''
                        ORDER BY p2.class
                    )
                ) AS comp,
                CASE WHEN m.team_won = p.team THEN 1 ELSE 0 END AS win
            FROM matches m
            JOIN (SELECT DISTINCT match_id, team FROM match_players) p ON p.match_id = m.match_id
            WHERE m.state = 'scored'
              AND m.is_bye = 0
              AND m.team_won IN (1, 2)
        )
        SELECT comp, COUNT(*) AS games, SUM(win) AS wins
        FROM team_comps
        WHERE comp IS NOT NULL
        GROUP BY comp
        """
    ).fetchall()
    return {str(r["comp"]): (int(r["games"]), int(r["wins"] or 0)) for r in rows}


def load_scheduled_matches(conn, from_date: date, to_date: date) -> List[dict]:
    rows = conn.execute(
        """
        SELECT match_id, match_date
        FROM matches
        WHERE state = 'scheduled'
          AND is_bye = 0
          AND match_date >= ?
          AND match_date <= ?
        ORDER BY match_date ASC, updated_at ASC
        """,
        (from_date.isoformat(), to_date.isoformat()),
    ).fetchall()

    out: List[dict] = []
    for row in rows:
        match_id = str(row["match_id"])
        players = conn.execute(
            """
            SELECT team, token_id, name, class, is_champion
            FROM match_players
            WHERE match_id = ?
            ORDER BY team, is_champion DESC, token_id ASC
            """,
            (match_id,),
        ).fetchall()
        if not players:
            continue

        team_classes: Dict[int, List[str]] = {}
        team_champs: Dict[int, dict] = {}
        for p in players:
            team = int(p["team"])
            cls = str(p["class"] or "").strip()
            if cls:
                team_classes.setdefault(team, []).append(cls)
            if int(p["is_champion"] or 0) == 1 and team not in team_champs:
                team_champs[team] = {
                    "token_id": int(p["token_id"] or 0),
                    "name": str(p["name"] or ""),
                    "class": cls if cls else None,
                }

        teams = sorted({int(p["team"]) for p in players})
        if len(teams) != 2:
            continue
        if teams[0] not in team_champs or teams[1] not in team_champs:
            # Always keep this predictor champion-only.
            continue

        snapshots: Dict[int, TeamSnapshot] = {}
        for team in teams:
            champ = team_champs.get(team, {})
            snapshots[team] = TeamSnapshot(
                team=team,
                champion_token_id=champ.get("token_id"),
                champion_name=champ.get("name"),
                champion_class=champ.get("class"),
                comp=build_team_comp(team_classes.get(team, [])),
            )

        out.append(
            {
                "match_id": match_id,
                "match_date": str(row["match_date"]),
                "team_1": snapshots[teams[0]],
                "team_2": snapshots[teams[1]],
            }
        )

    return out


def predict_match(
    team_1: TeamSnapshot,
    team_2: TeamSnapshot,
    class_stats: Dict[str, Tuple[int, int]],
    cvc_stats: Dict[Tuple[str, str], Tuple[int, int]],
    comp_stats: Dict[str, Tuple[int, int]],
) -> dict:
    class_prior_k = 60.0
    cvc_prior_k = 60.0
    comp_prior_k = 80.0

    comp_1_games, comp_1_wins = comp_stats.get(team_1.comp, (0, 0))
    comp_2_games, comp_2_wins = comp_stats.get(team_2.comp, (0, 0))
    comp_1_wr = smooth_wr(comp_1_wins, comp_1_games, k=comp_prior_k)
    comp_2_wr = smooth_wr(comp_2_wins, comp_2_games, k=comp_prior_k)
    comp_delta = logit(comp_1_wr) - logit(comp_2_wr)
    comp_conf = min(strength(comp_1_games, 300), strength(comp_2_games, 300))

    class_delta = 0.0
    class_conf = 0.0
    if team_1.champion_class and team_2.champion_class:
        c1_games, c1_wins = class_stats.get(team_1.champion_class, (0, 0))
        c2_games, c2_wins = class_stats.get(team_2.champion_class, (0, 0))
        c1_wr = smooth_wr(c1_wins, c1_games, k=class_prior_k)
        c2_wr = smooth_wr(c2_wins, c2_games, k=class_prior_k)
        class_delta = logit(c1_wr) - logit(c2_wr)
        class_conf = min(strength(c1_games, 500), strength(c2_games, 500))

    cvc_delta = 0.0
    cvc_conf = 0.0
    if team_1.champion_class and team_2.champion_class:
        cvc_games, cvc_wins = cvc_stats.get((team_1.champion_class, team_2.champion_class), (0, 0))
        cvc_wr = smooth_wr(cvc_wins, cvc_games, k=cvc_prior_k)
        cvc_delta = logit(cvc_wr)
        cvc_conf = strength(cvc_games, 300)

    log_odds = (0.60 * comp_conf * comp_delta) + (0.25 * class_conf * class_delta) + (0.45 * cvc_conf * cvc_delta)
    team_1_prob = sigmoid(log_odds)
    team_2_prob = 1.0 - team_1_prob

    predicted_winner_team = team_1.team if team_1_prob >= team_2_prob else team_2.team
    confidence = max(team_1_prob, team_2_prob)

    return {
        "predicted_winner_team": predicted_winner_team,
        "team_1_win_probability": round(team_1_prob, 4),
        "team_2_win_probability": round(team_2_prob, 4),
        "confidence": round(confidence, 4),
        "signals": {
            "composition": {
                "team_1_comp": team_1.comp,
                "team_2_comp": team_2.comp,
                "team_1_games": comp_1_games,
                "team_2_games": comp_2_games,
                "team_1_smoothed_wr": round(comp_1_wr, 4),
                "team_2_smoothed_wr": round(comp_2_wr, 4),
                "confidence": round(comp_conf, 4),
            },
            "champion_class": {
                "team_1_class": team_1.champion_class,
                "team_2_class": team_2.champion_class,
                "confidence": round(class_conf, 4),
            },
            "class_vs_class": {
                "games": cvc_stats.get((team_1.champion_class or "", team_2.champion_class or ""), (0, 0))[0],
                "confidence": round(cvc_conf, 4),
            },
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Predict scheduled Grand Arena matchups using class and team-structure priors.")
    parser.add_argument("--db-path", default=SETTINGS.db_path, help="Path to SQLite DB (default: %(default)s)")
    parser.add_argument(
        "--date",
        type=parse_date,
        default=(date.today() + timedelta(days=1)),
        help="Target date to score (YYYY-MM-DD). Default: tomorrow.",
    )
    parser.add_argument("--days", type=int, default=1, help="Number of days to include starting from --date.")
    parser.add_argument("--min-confidence", type=float, default=0.0, help="Only include predictions with confidence >= this value (0-1).")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to print/write after filtering (0 = no limit).")
    parser.add_argument("--summary-only", action="store_true", help="Print only summary counts and skip per-match lines.")
    parser.add_argument(
        "--start-time-utc",
        type=parse_hhmm_utc,
        default=None,
        help="Contest start time in UTC (HH:MM). Requires --num-matches and --days 1.",
    )
    parser.add_argument(
        "--num-matches",
        type=int,
        default=0,
        help="Take the next N scheduled matches from --start-time-utc (contest slice mode).",
    )
    parser.add_argument(
        "--favorable-threshold",
        type=float,
        default=0.5,
        help="Probability threshold used to count a matchup as favorable for the predicted winner champion.",
    )
    parser.add_argument("--json-out", help="Optional path to write predictions as JSON.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.min_confidence < 0.0 or args.min_confidence > 1.0:
        raise SystemExit("--min-confidence must be between 0 and 1")
    if args.limit < 0:
        raise SystemExit("--limit must be >= 0")
    if args.num_matches < 0:
        raise SystemExit("--num-matches must be >= 0")
    if args.favorable_threshold < 0.0 or args.favorable_threshold > 1.0:
        raise SystemExit("--favorable-threshold must be between 0 and 1")
    if (args.start_time_utc is None) != (args.num_matches == 0):
        raise SystemExit("Use --start-time-utc and --num-matches together.")
    if args.num_matches > 0 and args.days != 1:
        raise SystemExit("Contest slice mode currently requires --days 1.")

    from_date = args.date
    to_date = args.date + timedelta(days=args.days - 1)

    conn = get_connection(args.db_path)
    class_stats = load_class_stats(conn)
    cvc_stats = load_class_vs_class_stats(conn)
    comp_stats = load_comp_stats(conn)
    matches = load_scheduled_matches(conn, from_date=from_date, to_date=to_date)

    predictions = []
    for m in matches:
        team_1 = m["team_1"]
        team_2 = m["team_2"]
        pred = predict_match(team_1, team_2, class_stats, cvc_stats, comp_stats)
        predictions.append(
            {
                "match_id": m["match_id"],
                "match_date": m["match_date"],
                "team_1": {
                    "team": team_1.team,
                    "champion_token_id": team_1.champion_token_id,
                    "champion_name": team_1.champion_name,
                    "champion_class": team_1.champion_class,
                    "composition": team_1.comp,
                },
                "team_2": {
                    "team": team_2.team,
                    "champion_token_id": team_2.champion_token_id,
                    "champion_name": team_2.champion_name,
                    "champion_class": team_2.champion_class,
                    "composition": team_2.comp,
                },
                "prediction": pred,
            }
        )

    contest_slice_info: Optional[dict] = None
    if args.num_matches > 0:
        total = len(predictions)
        if total == 0:
            predictions = []
            contest_slice_info = {
                "enabled": True,
                "start_time_utc_minutes": args.start_time_utc,
                "start_index": 0,
                "requested_num_matches": args.num_matches,
                "selected_num_matches": 0,
                "total_matches_for_day": 0,
                "note": "No scheduled matches for this date.",
            }
        else:
            day_fraction = float(args.start_time_utc) / float(24 * 60)
            start_idx = int(math.floor(day_fraction * total))
            start_idx = max(0, min(total - 1, start_idx))
            end_idx = min(total, start_idx + args.num_matches)
            predictions = predictions[start_idx:end_idx]
            contest_slice_info = {
                "enabled": True,
                "start_time_utc_minutes": args.start_time_utc,
                "start_index": start_idx,
                "requested_num_matches": args.num_matches,
                "selected_num_matches": len(predictions),
                "total_matches_for_day": total,
                "note": (
                    "Scheduled match start times are not stored in DB; "
                    "time is mapped proportionally across the day's match order."
                ),
            }

    filtered = [row for row in predictions if row["prediction"]["confidence"] >= args.min_confidence]
    if args.limit > 0:
        filtered = filtered[: args.limit]

    favorable_by_champion: Dict[str, int] = {}
    expected_score_by_champion: Dict[str, float] = {}
    appearance_count_by_champion: Dict[str, int] = {}
    for row in filtered:
        prediction = row["prediction"]
        t1_name = row["team_1"]["champion_name"] or "Unknown Champion"
        t2_name = row["team_2"]["champion_name"] or "Unknown Champion"
        t1_prob = float(prediction["team_1_win_probability"])
        t2_prob = float(prediction["team_2_win_probability"])

        appearance_count_by_champion[t1_name] = appearance_count_by_champion.get(t1_name, 0) + 1
        appearance_count_by_champion[t2_name] = appearance_count_by_champion.get(t2_name, 0) + 1
        expected_score_by_champion[t1_name] = expected_score_by_champion.get(t1_name, 0.0) + t1_prob
        expected_score_by_champion[t2_name] = expected_score_by_champion.get(t2_name, 0.0) + t2_prob

        if t1_prob >= args.favorable_threshold:
            favorable_by_champion[t1_name] = favorable_by_champion.get(t1_name, 0) + 1
        if t2_prob >= args.favorable_threshold:
            favorable_by_champion[t2_name] = favorable_by_champion.get(t2_name, 0) + 1

    champions_gt_3 = {k: v for k, v in favorable_by_champion.items() if v > 3}
    top_expected_performers = [
        {
            "champion_name": champ_name,
            "expected_wins": round(expected_wins, 4),
            "matches": appearance_count_by_champion.get(champ_name, 0),
            "favorable_matchups": favorable_by_champion.get(champ_name, 0),
        }
        for champ_name, expected_wins in expected_score_by_champion.items()
    ]
    top_expected_performers.sort(
        key=lambda row: (-row["expected_wins"], -row["favorable_matchups"], row["champion_name"])
    )

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "db_path": args.db_path,
        "window_start": from_date.isoformat(),
        "window_end": to_date.isoformat(),
        "match_count": len(filtered),
        "contest_slice": contest_slice_info,
        "favorable_threshold": args.favorable_threshold,
        "champion_favorable_counts": favorable_by_champion,
        "champions_with_more_than_3_favorable_matchups": champions_gt_3,
        "champions_with_more_than_3_favorable_matchups_count": len(champions_gt_3),
        "top_expected_performers": top_expected_performers,
        "predictions": filtered,
    }

    print(f"Window: {payload['window_start']} -> {payload['window_end']}")
    print(f"Scheduled matches scored: {payload['match_count']}")
    if contest_slice_info is not None:
        start_minutes = int(contest_slice_info["start_time_utc_minutes"])
        hh = start_minutes // 60
        mm = start_minutes % 60
        print(
            "Contest slice: "
            f"{hh:02d}:{mm:02d} UTC, "
            f"start_index={contest_slice_info['start_index']}, "
            f"requested={contest_slice_info['requested_num_matches']}, "
            f"selected={contest_slice_info['selected_num_matches']}, "
            f"total_day_matches={contest_slice_info['total_matches_for_day']}"
        )
        print(f"Note: {contest_slice_info['note']}")
    if not args.summary_only:
        print()
        for row in filtered:
            p = row["prediction"]
            t1_name = row["team_1"]["champion_name"] or "Unknown Champion"
            t2_name = row["team_2"]["champion_name"] or "Unknown Champion"
            print(
                f"{row['match_date']}  {row['match_id']}  "
                f"T{p['predicted_winner_team']} ({max(p['team_1_win_probability'], p['team_2_win_probability']):.1%})  "
                f"T1={p['team_1_win_probability']:.1%}  T2={p['team_2_win_probability']:.1%}  "
                f"T1Champ={t1_name}({row['team_1']['champion_class']}) vs "
                f"T2Champ={t2_name}({row['team_2']['champion_class']})"
            )

    print()
    print(f"Champion names with >3 favorable matchups (threshold {args.favorable_threshold:.2f}): {len(champions_gt_3)}")
    if champions_gt_3:
        for champ_name, cnt in sorted(champions_gt_3.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  champion_name={champ_name} favorable_matchups={cnt}")

    if top_expected_performers:
        print()
        print("Top expected performers in selected period:")
        for row in top_expected_performers[:10]:
            print(
                f"  champion_name={row['champion_name']} "
                f"expected_wins={row['expected_wins']:.3f} "
                f"matches={row['matches']} "
                f"favorable_matchups={row['favorable_matchups']}"
            )

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print()
        print(f"Wrote JSON predictions to {args.json_out}")


if __name__ == "__main__":
    main()
