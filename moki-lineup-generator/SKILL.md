---
name: moki-lineup-generator
description: Generates a daily Moki Grand Arena lineup made up of 4 moki champions and 1 scheme card.
---

# Moki Lineup Generator

This skill generates a daily Moki Grand Arena lineup.

## How to use

To generate a lineup, run the following command from the root of the `moki` project:

```bash
python moki-lineup-generator/scripts/generate_lineup.py
```

The script will read the `champions.json`, `game.csv`, and `schemes.json` files, calculate the best lineup for each scheme card, and save the output to a file named `moki_lineups.md`.
