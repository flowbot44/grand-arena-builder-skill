from __future__ import annotations

import sqlite3
from contextlib import contextmanager


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS champions (
            token_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            traits_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            game_type TEXT NOT NULL,
            match_date TEXT NOT NULL,
            state TEXT NOT NULL,
            is_bye INTEGER NOT NULL,
            team_won INTEGER,
            win_type TEXT,
            scoring_method TEXT,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS match_players (
            match_id TEXT NOT NULL,
            moki_id TEXT NOT NULL,
            token_id INTEGER NOT NULL,
            team INTEGER NOT NULL,
            name TEXT,
            class TEXT,
            image_url TEXT,
            is_champion INTEGER NOT NULL,
            PRIMARY KEY (match_id, moki_id)
        );

        CREATE TABLE IF NOT EXISTS match_stats_players (
            match_id TEXT NOT NULL,
            token_id INTEGER NOT NULL,
            team INTEGER NOT NULL,
            won INTEGER NOT NULL,
            points REAL,
            eliminations REAL,
            deposits REAL,
            wart_distance REAL,
            PRIMARY KEY (match_id, token_id)
        );

        CREATE TABLE IF NOT EXISTS performances (
            performance_id TEXT PRIMARY KEY,
            match_id TEXT NOT NULL,
            moki_id TEXT NOT NULL,
            token_id INTEGER,
            match_date TEXT NOT NULL,
            is_bye INTEGER NOT NULL,
            win_type TEXT,
            deposits REAL,
            eliminations REAL,
            wart_distance REAL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            details_json TEXT
        );

        CREATE TABLE IF NOT EXISTS api_cursors (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS champion_metrics (
            token_id INTEGER PRIMARY KEY,
            matches_played INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            win_pct REAL,
            avg_points REAL,
            avg_eliminations REAL,
            avg_deposits REAL,
            avg_wart_distance REAL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_matches_date_state ON matches(match_date, state);
        CREATE INDEX IF NOT EXISTS idx_match_players_token_match ON match_players(token_id, match_id);
        CREATE INDEX IF NOT EXISTS idx_match_players_match_team_champ ON match_players(match_id, team, is_champion);
        CREATE INDEX IF NOT EXISTS idx_performances_token_date ON performances(token_id, match_date);
        CREATE INDEX IF NOT EXISTS idx_performances_match_token ON performances(match_id, token_id);
        CREATE INDEX IF NOT EXISTS idx_match_stats_token_match ON match_stats_players(token_id, match_id);
        CREATE INDEX IF NOT EXISTS idx_matches_updated_at ON matches(updated_at);
        """
    )
    try:
        conn.execute("ALTER TABLE matches ADD COLUMN scoring_method TEXT")
    except Exception:
        pass  # column already exists
    conn.commit()
