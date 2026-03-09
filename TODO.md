# TODO

- [x] Use UTC consistently for all day-boundary logic.
- [x] Align published feed lookahead with the app lookahead window.
- [x] Keep 30 days of gzip feed data while pruning the live DB to `today-2` through `today+2`.
- [x] Skip `seed_champions()` when `champions.json` content has not changed.
- [x] Enable SQLite write-oriented pragmas for the ingestion workload.
- [x] Batch `match_players`, `match_stats_players`, and `performances` writes with `executemany()`.
- [ ] Reduce per-match read-before-write work in `_upsert_match()` further, likely with a more selective upsert strategy.
- [ ] Precompute lightweight support win-rate aggregates for feed lookahead instead of scanning all scored partitions per request.
- [ ] Revisit `recompute_champion_metrics()` if DB growth resumes; current rebuild is acceptable for the trimmed DB window but still full-refresh.
