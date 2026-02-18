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
