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

Key routes:

- `GET /`
- `GET /champions/<token_id>`
- `GET /non-champions?page=1&per_page=100`
- `GET /non-champions/<token_id>?tab=history`
- `GET /non-champions/<token_id>?tab=lookahead`
- `GET /api/champions`
- `GET /api/champions/<token_id>/next-matches?limit=10`
- `GET /api/non-champions?page=1&per_page=100`
- `GET /api/non-champions/<token_id>/history`
- `GET /api/non-champions/<token_id>/next-matches?limit=10`
- `GET /api/system/status`

Response metadata includes:

- `lookahead_days`
- `window_start`
- `window_end`
- `insufficient_upcoming` (for sparse upcoming schedules)
