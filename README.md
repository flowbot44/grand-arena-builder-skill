# Grand Arena Builder Skill

Utilities and skill files for generating Moki Grand Arena lineup recommendations from local champion stats and scheme cards.

## What this repo contains

- `moki-lineup-generator/SKILL.md`: Skill definition for lineup generation.
- `moki-lineup-generator/scripts/generate_lineup.py`: Builds best lineups per scheme card (including trait-based and stat-based scheme bonuses) and writes results to `moki_lineups.md`.
- `champions.json`: Champion roster and traits.
- `game.csv`: Performance metrics used for scoring.
- `schemes.json`: Available scheme cards.
- `moki_lineups.md`: Example/generated lineup report.
- `update_champions.py`: Optional script to refresh champion traits from the marketplace GraphQL API.

## Requirements

- Python 3.9+
- For `update_champions.py`: `requests` package

Install dependencies:

```bash
python -m pip install requests
```

## Generate a lineup

Run from the project root:

```bash
python moki-lineup-generator/scripts/generate_lineup.py
```

The script reads:

- `champions.json`
- `game.csv`
- `schemes.json`

What it does:

- Computes base champion score from `winrate`, `avg elims`, `avg balls`, and `avg wart`.
- Applies additional scheme-specific stat bonuses for supported non-trait schemes:
  `Aggressive Specialization`, `Collective Specialization`, `Victory Lap`, `Taking a Dive`, `Gacha Gouging`, `Cage Match`.
- Applies trait-based optimization and bonuses (`+25` per matching champion, based on lineup composition) for supported trait schemes, including:
  `Shapeshifting` (matches `Tongue out`, `Tanuki mask`, `Kitsune Mask`, `Cat Mask`) and other trait schemes like `Divine Intervention`, `Midnight Strike`, `Malicious Intent`, etc.
- Selects the best 4-champion lineup for each supported scheme.
- Marks unsupported schemes in the output.
- Sorts all scheme lineups by total lineup score.
- Writes full report to `moki_lineups.md`.

Important notes:

- Champion traits in `champions.json` are used by trait-based schemes.
- The script prints `Successfully created moki_lineups.md` when generation succeeds.

## Refresh champion trait data (optional)

`update_champions.py` pulls latest traits using the Sky Mavis GraphQL endpoint.

1. Set your API key:

```bash
export MOKI_API_KEY="your_api_key"
```

2. Ensure `query.txt` exists in repo root with JSON shape:

```json
{
  "query": "<your GraphQL query string>"
}
```

3. Run:

```bash
python update_champions.py
```

Output is written to `champions_updated.json`.

## Explore Grand Arena leaderboard endpoint

Use `explore_grandarena_api.py` to call `GET /api/v1/leaderboards`.

1. Set your API key:

```bash
export GRANDARENA_API_KEY="your_api_key"
```

2. Run the explorer (defaults shown in the docs UI):

```bash
python explore_grandarena_api.py
```

3. Optional query params:

```bash
python explore_grandarena_api.py \
  --page 1 \
  --limit 20 \
  --game-type mokiMayhem \
  --sort startDate \
  --order desc \
  --out grandarena_leaderboards_response.json
```

The script:

- Calls only `GET /api/v1/leaderboards`.
- Supports query params:
  `page`, `limit`, `completed`, `gameType`, `fromDate`, `toDate`, `sort`, `order`.
- Tries common auth header styles (`Authorization: Bearer`, `x-api-key`, `X-API-Key`).
- Validates the 200 response shape against expected keys.
- Writes full request/response attempts to `grandarena_leaderboards_response.json`.

You can also query a single match (and related stats/performances), for example:

```bash
python explore_grandarena_api.py \
  --path /api/v1/matches/{matchId} \
  --path-match-id 6996ea499e2bde4f324cfae9 \
  --out match_6996ea499e2bde4f324cfae9.json
```

```bash
python explore_grandarena_api.py \
  --path /api/v1/matches/{matchId}/stats \
  --path-match-id 6996ea499e2bde4f324cfae9 \
  --out match_6996ea499e2bde4f324cfae9_stats.json
```

```bash
python explore_grandarena_api.py \
  --path /api/v1/matches/{matchId}/performances \
  --path-match-id 6996ea499e2bde4f324cfae9 \
  --page 1 --limit 100 \
  --out match_6996ea499e2bde4f324cfae9_performances.json
```

## Grand Arena local ingest + matchup website (v1)

This repo now includes a local data platform in `app/`:

- `python -m app.ingest backfill`: backfill and enrich match data.
- `python -m app.ingest hourly`: hourly rolling sync (today-2 to today+2).
- `python -m app.ingest enrich-only`: only enrich already stored scored matches missing stats/perfs.
- `python -m app.serve`: local Flask website/API for champion matchup edges.

### Runtime assumptions

- Free-tier safe request pacing: `80 req/min` with `0.75s` minimum interval.
- Retry on `429` and `5xx` with exponential backoff (`1s, 2s, 4s, 8s`).
- Backfill default start: `2026-02-19`.
- Lookahead window for upcoming matches: `2` days.

### Environment variables

```bash
export GRANDARENA_API_KEY="your_api_key"
export GRANDARENA_DB_PATH="grandarena.db" # optional
```

Optional tuning:

```bash
export REQUEST_LIMIT_PER_MINUTE=80
export MIN_REQUEST_INTERVAL_SECONDS=0.75
export LOOKBEHIND_DAYS=2
export LOOKAHEAD_DAYS=2
export CHAMPION_ONLY_MATCHES=true
export FETCH_MATCH_PERFORMANCES=true
```

Efficiency toggles:

- `CHAMPION_ONLY_MATCHES=true` (default): only store/enrich matches that include at least one token from `champions.json`.
- `FETCH_MATCH_PERFORMANCES=false`: skip `/matches/{id}/performances` enrichment and use stats-only enrichment for lower API usage.

### Backfill season to today

```bash
python -m app.ingest backfill --from 2026-02-19 --to "$(date +%F)"
```

### Run hourly sync window manually

```bash
python -m app.ingest hourly
```

### Efficient enrich-only pass (if matches are already in DB)

```bash
python -m app.ingest enrich-only --from 2026-02-19 --to 2026-02-23 --max-matches 1000
```

This mode skips `/api/v1/matches` pagination and only calls:
- `/api/v1/matches/{id}/stats`
- `/api/v1/matches/{id}/performances` (if `FETCH_MATCH_PERFORMANCES=true`)

### Run local website/API

```bash
python -m app.serve --host 127.0.0.1 --port 5000
```

### Predict day-ahead match winners

Use class-matchup and team-composition priors from scored matches to predict scheduled matches.

```bash
python -m app.predict_day_ahead --date "$(date -v+1d +%F)"
```

Optional JSON export:

```bash
python -m app.predict_day_ahead --date 2026-02-25 --days 1 --json-out predictions_2026-02-25.json
```

High-confidence shortlist:

```bash
python -m app.predict_day_ahead --date 2026-02-25 --min-confidence 0.60 --limit 50
```

Include champion names in each line (default) and print how many champion names have more than 3 favorable matchups:

```bash
python -m app.predict_day_ahead --date 2026-02-25 --favorable-threshold 0.50
```

Summary-only output (uses champion names, not IDs):

```bash
python -m app.predict_day_ahead --date 2026-02-25 --summary-only --favorable-threshold 0.50
```

Contest window mode (example: starts 08:00 UTC, evaluate next 5 matches):

```bash
python -m app.predict_day_ahead --date 2026-02-25 --start-time-utc 08:00 --num-matches 5 --summary-only
```

Note: the current DB stores `match_date` as date-only (no exact scheduled start timestamp), so `--start-time-utc` is mapped proportionally across the day’s match order.

### Website features (current)

- Terminal-style UI (black background, neon green text).
- Champions index: ranked list with quick links.
- Champion detail tabs:
  - `history`: scored match history + totals + calculated points.
  - `lookahead`: next matches, support metrics, opponent info, edge label/score.
  - `match-info`: per-match result/win type + class breakdown by group.
- Non-champions index with pagination (default `100` per page, up to `500`).
- Non-champion detail tabs:
  - `history`
  - `lookahead`

Key routes:

- `GET /`
- `GET /champions/<token_id>?tab=history`
- `GET /champions/<token_id>?tab=lookahead`
- `GET /champions/<token_id>?tab=match-info`
- `GET /non-champions?page=1&per_page=100`
- `GET /non-champions/<token_id>?tab=history`
- `GET /non-champions/<token_id>?tab=lookahead`
- `GET /api/champions`
- `GET /api/champions/<token_id>/history`
- `GET /api/champions/<token_id>/next-matches?limit=10`
- `GET /api/champions/<token_id>/match-info`
- `GET /api/non-champions?page=1&per_page=100`
- `GET /api/non-champions/<token_id>/history`
- `GET /api/non-champions/<token_id>/next-matches?limit=10`
- `GET /api/system/status`
- `GET /api/cumulative/current-totals`
- `GET /api/moki-totals`

Response metadata includes:

- `lookahead_days`
- `window_start`
- `window_end`
- `insufficient_upcoming` (for sparse upcoming schedules)
- `data_generated_at` (feed manifest generation timestamp)
- `cache_age_seconds` (in-memory cache age)
- `stale_data` (`true` when serving stale cache after feed refresh failure)

### What is stored in the SQLite database

Database file:

- default: `grandarena.db`
- override: `GRANDARENA_DB_PATH`

Tables:

- `champions`: champion roster from `champions.json` (`token_id`, `name`, `traits_json`, `updated_at`).
- `matches`: match-level metadata and state (`match_id`, `match_date`, `state`, `team_won`, `win_type`, timestamps).
- `match_players`: players per match (`token_id`, `moki_id`, `team`, `class`, `is_champion`).
- `match_stats_players`: per-match stats payload rows (`points`, `deposits`, `eliminations`, `wart_distance`, `won`).
- `performances`: per-performance rows from `/matches/{id}/performances`.
- `ingestion_runs`: run history and run details JSON.
- `api_cursors`: per-date watermarks/cursors used by ingest.
- `champion_metrics`: recomputed champion summary metrics for website queries.

Notes:

- `CHAMPION_ONLY_MATCHES=true` keeps storage focused on matches that include at least one champion.
- Calculated points shown on pages use custom scoring logic in app analytics, not raw API points.

## Public GitHub feed for Vercel/FastAPI consumers

This repo can publish a static JSON/GZIP feed to GitHub Pages so a Vercel-hosted app can read data without shipping `grandarena.db`.

Expected public URLs (replace `<user>`/`<repo>`):

- `https://<user>.github.io/<repo>/data/latest.json`
- `https://<user>.github.io/<repo>/data/status.json`
- `https://<user>.github.io/<repo>/data/partitions/raw_matches_YYYY-MM-DD.json.gz`
- `https://<user>.github.io/<repo>/data/cumulative/latest.json`
- `https://<user>.github.io/<repo>/data/cumulative/current_totals.json.gz`
- `https://<user>.github.io/<repo>/data/cumulative/daily_totals_YYYY-MM-DD.json.gz`
- `https://<user>.github.io/<repo>/data/moki_totals.json`

### Scheduled publish flow

Workflow file: `.github/workflows/publish-feed.yml`

- Triggered every 4 hours (`cron: "0 */4 * * *"`, UTC) and via manual dispatch.
- Restores the last DB state artifact (`state/grandarena.db.gz`) when available.
- Optionally seeds the DB from `SEED_DB_URL` if no prior DB artifact exists.
- Runs:
  - `python -m app.ingest hourly --db state/grandarena.db`
  - `python -m app.maintenance prune --db state/grandarena.db --keep-days 7`
  - `python -m app.export_moki_totals --out exports/data/moki_totals.json`
  - `python -m app.export_feed --db state/grandarena.db --out exports/data --days 7 --lookahead-days 1 --mutable-days-back 1 --mutable-days-forward 1 --cumulative-mutable-days-back 1`
- Verifies `exports/data/cumulative/current_totals.json.gz` is non-empty before deploy.
- Publishes `exports/` to GitHub Pages.
- Uploads updated `state/grandarena.db.gz` for the next run.

Required secret:

- `GRANDARENA_API_KEY`
- `SEED_DB_URL` (optional, only used when no DB artifact is available)

### 7-day rolling raw partitions

`app.maintenance` keeps only a rolling 7-day window in DB by deleting match-linked rows older than the cutoff date:

- `performances`
- `match_stats_players`
- `match_players`
- `matches`

Raw partition exports are generated per day:

- `data/partitions/raw_matches_YYYY-MM-DD.json.gz`

Publish behavior for raw partitions:

- Days older than `today-1` inside the 7-day window are reused from prior exports when available.
- Only `yesterday`, `today`, and `today+1` are refreshed each run.

These files are designed for website/API consumers that need recent raw match/player/stats/performance data without reprocessing all days every run.

### Cumulative daily totals dataset (tokenId keyed)

`app.export_feed` also emits cumulative totals keyed by `token_id` (with `moki_id` metadata when available):

- `games_played_cum`
- `wins_cum`
- `points_cum`
- `eliminations_cum`
- `deposits_cum`
- `wart_distance_cum`
- `as_of_date`

Publish behavior for cumulative files:

- Older cumulative daily files are reused when available.
- Only `yesterday` and `today` cumulative files are recomputed each run.
- `cumulative/current_totals.json.gz` should represent the latest cumulative snapshot and must be non-empty for deploy to proceed.

Points formula:

- `points = deposits*50 + eliminations*80 + floor(wart_distance/80)*45 + (won ? 300 : 0)`

### Vercel integration expectation

For a Vercel-hosted FastAPI/static frontend app:

- Read from the GitHub Pages URLs over HTTPS.
- Add short TTL caching (for example 5-15 minutes) in the backend adapter.
- Keep existing frontend endpoint contracts stable while swapping the backend data source from local SQLite to feed files.

Feed adapter environment variables:

- `FEED_BASE_URL` (default: `https://flowbot44.github.io/grand-arena-builder-skill/data`)
- `FEED_TTL_SECONDS` (default: `600`)
- `FEED_HTTP_TIMEOUT_SECONDS` (default: `10`)

Operational behavior:

- In Vercel (`VERCEL` env set), the app expects `FEED_BASE_URL` to be configured.
- On feed timeout/parse/fetch errors, stale cache is served when available.
- If feed fetch fails and no cache exists, API returns `503` with retry guidance.
