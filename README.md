## FIVB Beach Pro Tour Leaderboard

This project defines a **semantic data model** for professional beach volleyball players, teams, tournaments, and rankings on the FIVB Beach Pro Tour, implemented with **dbt** on **Postgres**.

- **Warehouse**: Postgres
- **Transformation**: dbt (with `dbt-postgres`)
- **Goal**: Provide clean dimensions and fact tables for leaderboards, rankings, and performance analytics.

### Prerequisites (system setup)

Use **Python 3.11 or 3.12**; dbt is not yet compatible with Python 3.14+. The project declares `requires-python = ">=3.11,<3.13"` in `pyproject.toml`, so `pip install -e .` will fail on unsupported versions.

**macOS (Homebrew):**

```bash
brew install postgresql@16 python@3.12
brew services start postgresql@16
```

Ensure `psql` and your chosen Python are on `PATH`. If needed, add to `~/.zshrc`:

```bash
export PATH="/opt/homebrew/opt/postgresql@16/bin:/opt/homebrew/opt/python@3.12/libexec/bin:$PATH"
```

**Linux (apt):** `sudo apt install postgresql postgresql-client python3.12 python3.12-venv`

**Create the database, user, and schemas:** from the project root, run `./postgres/setup.sh`. This creates the database and user, plus the `raw` and `analytics` schemas with the right grants so the ETL and dbt can create tables and views. Use the same `DB_USER` / `DB_PASSWORD` in `.env` and in `~/.dbt/profiles.yml`.

### 1. Setup

**Create a virtualenv with Python 3.12 and install the project (dependencies are in `pyproject.toml`; requires Python 3.11–3.12):**

```bash
python3.12 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

On macOS with Homebrew Python 3.12:

```bash
/opt/homebrew/opt/python@3.12/bin/python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Copy `.env.example` to `.env` and set `DATABASE_URL`. Use the **same** database user and password in your dbt profile (`~/.dbt/profiles.yml`) so ETL and dbt share one database and permissions.

### 2. Configure dbt profile

dbt looks for profiles in `~/.dbt/profiles.yml`. Add an entry like:

```yaml
fivb_leaderboard:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: your_db_user
      password: your_db_password
      port: 5432
      dbname: fivb_leaderboard
      schema: analytics
      threads: 4
      keepalives_idle: 0
```

The `profile` name (`fivb_leaderboard`) must match `dbt_project.yml`; `user` and `password` should match `DATABASE_URL` in `.env`.

### 3. Run the pipeline (first time)

With the venv activated and `.env` plus dbt profile set:

```bash
python -m etl.load_raw    # load raw data from FIVB VIS into raw.*
dbt debug                 # optional: verify dbt can connect
dbt run                   # build staging and core models
```

Run `./postgres/setup.sh` first if the database or schemas don’t exist yet.

### 4. Raw FIVB data (VIS Web Service)

The ETL pulls data from the **FIVB VIS Web Service** (data API), not the web portal:

- **Docs**: [FIVB VIS Web Service](https://www.fivb.org/VisSDK/VisWebService/)
- **Endpoint**: `POST https://www.fivb.org/Vis2009/XmlRequest.asmx` with an XML request body (request types: `GetBeachTournamentList`, `GetBeachMatchList`, `GetBeachTeamList`, `GetBeachTournamentRanking`, etc.).
- **Auth**: None required for public data.

The **`etl/vis_client.py`** module implements these requests (JSON responses, no auth). **`etl/load_raw.py`** uses it and writes into the **`raw`** schema:

- `raw_fivb_tournaments`
- `raw_fivb_teams`
- `raw_fivb_matches`
- `raw_fivb_results` (tournament finishing positions from [GetBeachTournamentRanking](https://www.fivb.org/VisSDK/VisWebService/#GetBeachTournamentRanking.html))
- `raw_fivb_players` (player details from VIS GetPlayerList, single request)

Run the ETL (loads tournaments, then teams and players in single requests, then matches and results per tournament):

```bash
python -m etl.load_raw
```

Optional env vars (all optional; unset = no limit) for faster, smaller test runs:

| Env var | Effect |
|---------|--------|
| `LIMIT_TOURNAMENTS` | Max tournaments to process for matches/results (None = all from list). |
| `LIMIT_MATCHES_PER_TOURNAMENT` | Max matches to store per tournament. |
| `LIMIT_RESULTS_PER_TOURNAMENT` | Max result rows to store per tournament (per phase). |
| `ETL_PARALLEL` | Set to `0`, `false`, or `no` to run per-tournament loads sequentially; default is parallel. Use to compare performance. |
| `ETL_MAX_WORKERS` | When parallel, number of threads for per-tournament loads (default 4). Ignored if `ETL_PARALLEL` is off. |
| `TRUNCATE_RAW` | Set to `1`, `true`, or `yes` to truncate all raw tables before loading (use once when migrating from append-only, or when you want a full replace). |

Raw tables use **upsert** (insert or update by primary key), so re-runs refresh existing rows instead of appending duplicates. Staging models also deduplicate by business key. If you see "Raw table has duplicate keys", run once with `TRUNCATE_RAW=1` to clear and re-add primary keys.

**Test pipeline config** — A small, repeatable test run (ETL + dbt in ~3–5 min) is defined in **`env.test.example`**. Use it in one of two ways:

1. **Source the config and run manually** (from project root, with venv activated and `.env` set):

   ```bash
   set -a && source env.test.example && set +a
   python -m etl.load_raw
   dbt build
   ```

2. **Run the script** (sources `env.test.example` if present, else uses the same defaults):

   ```bash
   chmod +x scripts/run_test_pipeline.sh   # once
   ./scripts/run_test_pipeline.sh
   ```

You can copy `env.test.example` to `env.test` and edit limits if you want a different test size; the script will use `env.test.example` (or inline defaults) and does not require a separate copy.

### 5. Semantic layer models (dbt)

The dbt models in `models/` transform the raw tables into a **semantic warehouse model**:

- **Staging models** in `models/staging/fivb/`:
  - `stg_fivb_players.sql`
  - `stg_fivb_teams.sql`
  - `stg_fivb_tournaments.sql`
  - `stg_fivb_matches.sql`
  - `stg_fivb_rankings.sql`

- **Core models** in `models/core/`:
  - `dim_player.sql`
  - `dim_team.sql`
  - `dim_tournament.sql`
  - `fact_match.sql`
  - `fact_result.sql`
  - `fact_ranking_snapshot.sql`

These implement the ontology for:

- Players
- Teams/partnerships
- Tournaments
- Matches
- Tournament results
- Ranking snapshots (world/olympic/seasonal, etc.)

### 6. Testing

**VIS client (live API):**

```bash
pytest tests/test_vis_client.py -v
```

**dbt tests** (run after `dbt run`): the project uses `dbt_utils.at_least_one` on key columns of each core model so that `dbt test` fails if a core table is empty. Install the package and run:

```bash
dbt deps
dbt test
```

See `packages.yml` for the dbt_utils dependency.

### 7. Running dbt

Initialize and run dbt models:

```bash
dbt debug        # verify connection
dbt deps         # if using packages
dbt run          # build all models
dbt test         # run tests defined in schema YAML
```

To run just the core semantic layer:

```bash
dbt run --select core
```

### 8. Streamlit app

A minimal web app queries the Postgres core models and displays them with Plotly Express:

```bash
source .venv/bin/activate
streamlit run streamlit_app.py
```

Uses `DATABASE_URL` from `.env`. In the sidebar, pick a core model (e.g. `dim_tournament`, `fact_ranking_snapshot`) and optional row limit; the app shows a data table and a default chart (e.g. tournaments by country, rank vs points).

### 9. Next steps

- Extend models with more granular stats (per set, per point) if VIS exposes them.
- Layer BI or an application API on top of the dimensional/fact models for leaderboards and player scouting.

### 10. Optional: dbt MCP server

This repo includes a barebones [dbt MCP](https://github.com/dbt-labs/dbt-mcp) config so Cursor (or other MCP clients) can use dbt tools (run, compile, lineage, model details, etc.).

1. **Install [uv](https://docs.astral.sh/uv/getting-started/installation/)** (provides `uvx`, used to run `dbt-mcp`).
2. **Edit `.cursor/mcp.json`**: replace `<REPO_ROOT>` with the full path to this repo and `<PATH_TO_DBT_EXE>` with your dbt executable path:
   - macOS/Linux: `which dbt` → e.g. `/opt/homebrew/bin/dbt`
   - Windows: `where dbt` → e.g. `C:/Python312/Scripts/dbt.exe`
3. Restart Cursor or reload MCP so it picks up the config.

This setup is **CLI-only** (no dbt Cloud). For dbt platform (Discovery API, Semantic Layer, jobs), see [dbt docs](https://docs.getdbt.com/docs/dbt-ai/integrate-mcp-cursor).

