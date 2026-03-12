# Elo pipeline diagnostics

Use these to see where match/row counts drop between raw data and `core.player_elo_history`.

## Row-count funnel (run in psql or your SQL client)

```sql
-- 1. Raw matches
SELECT 'raw_fivb_matches' AS stage, COUNT(*) AS cnt FROM raw.raw_fivb_matches
UNION ALL
SELECT 'raw with winner', COUNT(*) FROM raw.raw_fivb_matches WHERE winner_team_id IS NOT NULL
UNION ALL
-- 2. In elo_match_feed (winner + both teams with non-null player IDs in dim_team_tournaments)
SELECT 'elo_match_feed', COUNT(*) FROM mart.elo_match_feed
UNION ALL
SELECT 'feed with played_at', COUNT(*) FROM mart.elo_match_feed WHERE played_at IS NOT NULL
UNION ALL
SELECT 'feed with played_at or start_date', COUNT(*) FROM mart.elo_match_feed
  WHERE played_at IS NOT NULL OR tournament_start_date IS NOT NULL
UNION ALL
-- 3. History rows (4 per match)
SELECT 'player_elo_history', COUNT(*) FROM core.player_elo_history;
```

- **raw with winner** &lt; **raw_fivb_matches**: many matches have no winner (e.g. walkover, incomplete).
- **elo_match_feed** &lt; **raw with winner**: matches dropped because team1 or team2 is missing from `dim_team_tournaments` for that tournament, or either team has null `player_a_id` / `player_b_id` (from `raw_fivb_teams`).
- **player_elo_history** = 4 × (matches processed): each match adds 4 rows (one per player). If history is still low, the script may be skipping matches with no usable date (both `played_at` and `tournament_start_date` null).

## Why the feed can be smaller than raw matches

1. **Team–player linkage**  
   `elo_match_feed` requires both teams to exist in `dim_team_tournaments` (from `raw_fivb_teams`) with non-null `player_a_id` and `player_b_id`. If the VIS API omits player IDs for some teams or tournaments, those matches are excluded.

2. **played_at**  
   Matches with null `played_at` are now included by using `tournament_start_date` for ordering and `as_of_date` when `played_at` is null. Matches with both null are still skipped.

3. **Winner**  
   Only matches with `winner_team_id` set are used (no draws in beach volleyball).
