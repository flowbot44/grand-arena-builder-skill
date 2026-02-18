# Grand Arena Builder Skill

Utilities and skill files for generating Moki Grand Arena lineup recommendations from local champion stats and scheme cards.

## What this repo contains

- `moki-lineup-generator/SKILL.md`: Skill definition for lineup generation.
- `moki-lineup-generator/scripts/generate_lineup.py`: Scores champions from `game.csv`, picks the top 4 by score, and pairs them with a random scheme card from `schemes.json`.
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

Run from the script directory (important for relative file paths):

```bash
cd moki-lineup-generator/scripts
python generate_lineup.py
```

The script reads:

- `../../champions.json`
- `../../game.csv`
- `../../schemes.json`

What it does:

- Computes a per-champion score from `winrate`, `avg elims`, `avg balls`, and `avg wart`.
- Sorts all champions by computed score (descending).
- Selects the top 4 champions.
- Selects 1 random scheme card.
- Prints the lineup to stdout.

Important notes:

- `champions.json` is currently loaded only for existence checking and is not used in the scoring logic.
- Output is currently console-only; this script does not write `moki_lineups.md`.

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
