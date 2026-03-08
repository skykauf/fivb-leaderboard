# Elo Model Plan: Player Ratings from H2H Match Results

This document plans a **player-level Elo rating system** computed **de novo** using **only head-to-head match results**, built on top of the existing dbt models.

---

## 1. Goals

- **All players’ Elo rankings across time** — one rating per player, updated after each match they play.
- **De novo** — no use of FIVB official rankings or points; only match outcomes.
- **H2H only** — input is match result data: who played whom, when, and who won.
- **On top of dbt** — consume existing fact/dimension models and expose new marts for Elo.

---

## 2. Current dbt Stack (Relevant Pieces)

| Layer        | Model                 | Role for Elo |
|-------------|------------------------|--------------|
| Staging     | `stg_fivb_matches`     | Match grain: `match_id`, `played_at`, `team1_id`, `team2_id`, `winner_team_id` |
| Staging     | `stg_fivb_players`     | Player dimension: `player_id`, `full_name`, `gender` |
| Core        | `fct_matches`          | Matches + tournament/round context, `is_winner_team1` |
| Core        | `dim_team_tournaments` | Links teams to players: `team_id`, `tournament_id`, `player_a_id`, `player_b_id` |
| Core        | `dim_tournaments`      | `tournament_id`, `gender` (men/women) |
| Mart        | `match_mart`           | One row per match with team and player IDs/names |

Matches are **team vs team** (beach: 2 players per team). Each match has a clear winner (`winner_team_id` / `is_winner_team1`). We have `played_at` for ordering.

---

## 3. Elo Design Choices

### 3.1 Grain: Player, not team

- **Player Elo**: each **player** has one rating per gender at any point in time.
- Matches are still team vs team; we derive player updates from each match (see below).

### 3.2 Gender

- Compute **separate Elo series per gender** (tournament gender from `dim_tournaments`). No cross-gender matches in the data.

### 3.3 Team strength from player Elo

- For a match, **team Elo** = average of the two players’ current Elos:
  - `team1_elo = (player_a_elo + player_b_elo) / 2`
  - Same for team2.
- Expected score (probability team1 wins):
  - `E1 = 1 / (1 + 10^((team2_elo - team1_elo) / 400))`
- Actual: 1 if team1 wins, 0 if team2 wins (no draws in beach volleyball).

### 3.4 Updating player Elo after a match

- Result: `S1 = 1` if team1 wins, else `0`. `S2 = 1 - S1`.
- Delta for team1: `K * (S1 - E1)`; for team2: `K * (S2 - E2)`.
- **Split delta evenly** between the two players on each team:
  - Team1’s players each get: `(K/2) * (S1 - E1)`.
  - Team2’s players each get: `(K/2) * (S2 - E2)` (negative when they lose).
- **K-factor**: fixed (e.g. 32) or optionally vary by tournament tier (e.g. higher K for majors). Start with a single K for simplicity.
- **Initial Elo**: same for all players before first match (e.g. 1500).

### 3.5 Time ordering

- Process matches in **chronological order** (`played_at`) **per gender**. Each match updates the four players’ Elos; those updated values are used for the next match involving any of them.

---

## 4. Where the Elo algorithm runs

Elo is **stateful**: each match update depends on the current Elos of four players, which were set by previous matches. dbt models are stateless, so the rating loop does not run inside dbt.

**Approach**: A **Python** script (or job) reads **`elo_match_feed`** from the warehouse, runs the Elo algorithm in memory (one pass per gender, chronological order), and writes the result to a table (**`player_elo_history`**). dbt then **sources** that table and builds marts on top of it. So the pipeline is: dbt builds the feed → Python computes Elo → dbt exposes history and latest ratings.

---

## 5. Proposed dbt Models (On Top of Existing)

### 5.1 New mart: Elo match feed (input to Elo)

**Purpose**: One row per completed match with all information needed to run the Elo algorithm: no rankings, no standings—only H2H and time.

**Suggested name**: `mart/fivb/elo_match_feed.sql` (or `elo_input_mart`).

**Columns** (conceptual):

| Column                 | Type     | Source / logic |
|------------------------|----------|----------------|
| `match_id`             | PK       | `fct_matches` |
| `played_at`            | timestamp| `fct_matches.played_at` — sort key |
| `tournament_gender`    | text     | `dim_tournaments.gender` — separate Elo series |
| `team1_player_a_id`    | int      | `dim_team_tournaments` for team1 |
| `team1_player_b_id`    | int      | same |
| `team2_player_a_id`    | int      | `dim_team_tournaments` for team2 |
| `team2_player_b_id`    | int      | same |
| `is_winner_team1`      | boolean  | from `fct_matches` |

**Filters**:

- `winner_team_id is not null` (completed matches with a winner).
- Optionally: exclude certain phases or result types if you add them later.

**Dependencies**: `fct_matches`, `dim_tournaments`, `dim_team_tournaments` (all existing). No use of `fct_team_rankings` or standings.

**Materialization**: View is enough; the feed is a thin slice of existing models.

---

### 5.2 Elo output (ratings over time)

**Purpose**: The result of the Python Elo run, written to a table so dbt can build marts on top.

The **Python** script reads `elo_match_feed` (from the warehouse), sorts by `tournament_gender` and `played_at`, and for each gender maintains a dict of `player_id → current_elo` (initial 1500). For each match it computes team Elos, expected score, deltas; updates all four players; and appends four rows to the output. It then writes the result to a table (e.g. in `core` or `mart`). dbt **sources** this table and downstream marts `ref()` it or select from the source.

The **schema** of the Elo output table should look like:

**Table**: `player_elo_history` (or `player_elo_snapshots`)

| Column       | Type     | Description |
|--------------|----------|-------------|
| `player_id`  | int      | FK to players |
| `gender`     | text     | Men / Women (matches tournament_gender) |
| `as_of_date` | date     | Date of the match that produced this rating (or `played_at::date`) |
| `match_id`   | int      | Last match that updated this player’s rating (optional but useful for debugging) |
| `elo_rating` | numeric  | Player’s Elo after that match |

- One row per **player per match played** (i.e. after each match, append four rows—one per player). “Across time” = many rows per player, ordered by `as_of_date`.
- **Latest rating** = per (player_id, gender) take the row with max(as_of_date).

---

### 5.3 dbt models on top of Elo output

Once the `player_elo_history` table exists (populated by the Python job), add dbt models that sit on top of it:

1. **`mart/fivb/player_elo_history`**  
   - If the table lives in raw/core, this mart can be a view that selects from the source/ref and adds player names, etc.  
   - Exposes: `player_id`, `player_name`, `gender`, `as_of_date`, `match_id`, `elo_rating`, and any other dimensions you want.

2. **`mart/fivb/player_elo_latest`**  
   - One row per (player_id, gender): latest `elo_rating` and `as_of_date`.  
   - Useful for “current” Elo and for joining to match_mart (e.g. “team strength” at match time can be joined from history).

3. **Optional: match_mart enrichment**  
   - Join match_mart to player Elo history (as of `played_at`) to get “team1_avg_elo”, “team2_avg_elo” for analytics, still using only H2H-derived Elo.

---

## 6. Implementation Order

1. **Add `elo_match_feed`** in dbt (view on `fct_matches` + `dim_tournaments` + `dim_team_tournaments`), filtered to completed matches, with the columns in 5.1.
2. **Implement the Elo calculator in Python**:
   - Connect to the warehouse (same profile as dbt, or read from a CSV/Parquet export of `elo_match_feed`).
   - Read `elo_match_feed`, sort by `tournament_gender` then `played_at`.
   - For each gender, maintain `player_id → current_elo` (default 1500). For each match: compute team Elos, expected score, deltas; update all four players; append four rows to the history list.
   - Write the result to a table `player_elo_history` (e.g. in schema `core` or `mart`).
3. **Register the Elo table in dbt** as a **source** (e.g. in `models/.../sources.yml`) so it’s documented and versioned with the project.
4. **Add marts** that select from that source: `player_elo_history` (with player names) and `player_elo_latest` (latest rating per player per gender). Optionally enrich match_mart with team avg Elo at match time.

---

## 7. Summary

- **Input**: H2H-only match feed from existing dbt models → new **`elo_match_feed`** mart (dbt view).
- **Computation**: Stateful Elo loop in **Python** on top of `elo_match_feed`; script writes **`player_elo_history`** to the warehouse.
- **Output**: dbt **sources** that table and builds marts for **player Elo across time** and **latest Elo**, with no dependency on FIVB rankings.

The Elo model is Pythonic on top of the dbt-built feed; dbt then owns the feed and the downstream marts.
