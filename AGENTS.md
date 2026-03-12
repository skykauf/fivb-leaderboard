# Agent guidelines

Guidance for AI and human contributors working in this repo.

## Database: no migrations

**Do not introduce or rely on table migrations** (e.g. `ALTER TABLE` to add/remove/rename columns, or migration frameworks).

- Prefer **re-initializing the database** when schema or ETL changes require it:
  - Drop and recreate objects (e.g. `etl.db.drop_all_schemas`) or use `./postgres/restart_postgres.sh` for a full reset.
  - Re-run ETL and dbt so tables and views are created from current code.
- If you change raw/staging table structure (columns in `etl/db.py`, ETL normalizers, or dbt models), document that a **full re-init** is required (e.g. in the PR or commit message); do not add migration scripts.

This keeps the codebase simple and avoids maintaining a migration history or dealing with backward compatibility on the DB layer.

## dbt first, Python where needed

- **Prefer dbt** for testing, transformations, and as much logic as possible: use dbt models, tests, and macros. Run and validate with `dbt run`, `dbt test`, etc.
- **Use Python** for external interfaces (e.g. VIS API client, loading raw data into Postgres) and for essential modeling that doesn’t fit dbt (e.g. Elo or other algorithms that need iterative or stateful computation). Keep Python focused on ingestion and specialized logic; do the rest in dbt.

## Fail loudly

- **Prefer explicit failures over silent bad state.** When preconditions or order are wrong (e.g. missing table, wrong run order), raise errors or exit with a clear message rather than continuing and writing inconsistent or stale data. Avoid “best effort” behavior that hides problems; fail fast so they can be fixed.

## Match date and time ordering

**`played_at` is almost never populated** in the source data. For all time ordering, "as of" semantics (e.g. rankings at match time, Elo as_of_date), and recency use:

- **`match_date`** — canonical date for a match: `COALESCE(played_at, round_start_date, tournament_start_date)`. Exposed in `fct_matches`, `match_mart`, and `elo_match_feed`. Prefer this for ordering and filters.
- When `match_date` is not available, use **`COALESCE(played_at::date, round_start_date, tournament_start_date)`** (e.g. in ad-hoc queries against tables that don't yet have `match_date`).

dbt models use `match_date` for ranking joins, Elo feed ordering, and mart columns; the Elo script uses `match_date` from the feed for `as_of_date`.

## Database lookups (Postgres / MCP)

Use **psql** or the **Postgres MCP** (if configured in `.cursor/mcp.json`) to run ad‑hoc queries. Connection is typically `postgresql://fivb_leaderboard:yourpassword@localhost:5432/fivb_leaderboard` (or from `.env` / `DATABASE_URL`).

### Key schemas and views

| Schema   | Purpose |
|----------|---------|
| `raw`    | Raw ingested tables: `raw_fivb_matches`, `raw_fivb_team_rankings`, etc. |
| `staging` | dbt staging: `stg_fivb_players`, `stg_fivb_matches`, etc. |
| `core`   | dbt core: `fct_matches`, `fct_team_rankings`, `fct_tournament_standings`, `dim_team_tournaments`, `dim_tournaments` |
| `mart`   | dbt marts: `match_mart`, `player_elo_latest`, `player_elo_history` |

### Common lookups

**1. Find player_id by name**

```sql
SELECT player_id, full_name FROM staging.stg_fivb_players
WHERE full_name ILIKE '%SearchName%';
```

**2. All career matches for a player (full details, recent → oldest)**

Use `match_mart` and order by `match_date` (see "Match date and time ordering" above):

```sql
SELECT match_id, match_date, tournament_name, season, round_name,
  team1_player_a_name || ' / ' || team1_player_b_name AS team1,
  team2_player_a_name || ' / ' || team2_player_b_name AS team2,
  score_sets,
  CASE WHEN winner_team_id = team1_id THEN team1_player_a_name || ' / ' || team1_player_b_name
       ELSE team2_player_a_name || ' / ' || team2_player_b_name END AS winner
FROM mart.match_mart
WHERE <player_id> IN (team1_player_a_id, team1_player_b_id, team2_player_a_id, team2_player_b_id)
ORDER BY match_date DESC NULLS LAST, match_id DESC;
```

**3. Player Elo history (one row per match)**

```sql
SELECT player_id, player_name, as_of_date, match_id, tournament_name, tournament_season, elo_rating
FROM mart.player_elo_history
WHERE player_id = <player_id>
ORDER BY as_of_date DESC NULLS LAST, match_id DESC;
```

**4. Top Elo rankings (all players or by country/gender)**

```sql
SELECT player_id, player_name, player_country_code, gender,
  round(elo_rating::numeric, 1) AS elo_rating, matches_played, wins, losses,
  last_match_played_at, last_match_tournament_name
FROM mart.player_elo_latest
WHERE 1=1
  -- AND player_country_code = 'USA'
  -- AND gender = '0'  -- men (often stored as '0' in this schema)
ORDER BY elo_rating DESC
LIMIT 30;
```

**5. Potential partners: same country (and optional similar Elo), ordered by Elo**

```sql
SELECT player_id, player_name, round(elo_rating::numeric, 1) AS elo_rating,
  matches_played, wins, losses, last_match_played_at, last_match_tournament_name
FROM mart.player_elo_latest
WHERE player_country_code = 'USA' AND gender = '0'
  AND player_id != <exclude_player_id>
  AND last_match_played_at >= '2024-01-01'
  -- AND elo_rating BETWEEN 1487 AND 1647  -- optional: similar Elo band
ORDER BY elo_rating DESC;
```

Replace `<exclude_player_id>` with the target player’s ID (from lookup 1).
