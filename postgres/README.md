## Postgres quick-setup

This folder contains helpers to spin up a **local Postgres database** for the FIVB leaderboard project.

### 1. Start Postgres (macOS/Homebrew example)

If you don't have Postgres:

```bash
brew install postgresql
brew services start postgresql
```

### 2. Run the setup script

From the project root:

```bash
chmod +x postgres/setup.sh
./postgres/setup.sh
```

Defaults (override via env vars if needed):

- **DB_NAME**: `fivb_leaderboard`
- **DB_USER**: `fivb_username`
- **DB_PASSWORD**: `fivb_password`

Example with overrides:

```bash
DB_NAME=fivb_leaderboard \
DB_USER=fivb_username \
DB_PASSWORD=supersecret \
./postgres/setup.sh
```

The script will:

- Create the database if it does not exist.
- Create the user if it does not exist.
- Grant the user all privileges on the database.
- Create the **`raw`** and **`analytics`** schemas and grant the user full access so the ETL can create/load raw tables and dbt can build models in `analytics`.
- Print out example `DATABASE_URL` and dbt profile configuration.

Run this **before** the first `python -m etl.load_raw` and `dbt run` so table creation and model runs succeed without permission errors.

### 3. Wire to ETL and dbt

Create a `.env` file at the project root (only `DATABASE_URL` is required for the ETL):

```bash
DATABASE_URL=postgresql+psycopg2://fivb_username:fivb_password@localhost:5432/fivb_leaderboard
```

Then (from project root, with Python 3.11 or 3.12):

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .

python -m etl.load_raw   # populate raw tables
dbt run                  # build semantic models
```

Use the same `DB_USER` and `DB_PASSWORD` in `.env` and in `~/.dbt/profiles.yml`.

