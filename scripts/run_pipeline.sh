#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f ".venv/bin/activate" ]]; then
  echo "Missing .venv. Create it with: python3.12 -m venv .venv && source .venv/bin/activate && pip install -e ."
  exit 1
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

if [[ ! -f ".env" ]]; then
  echo "Missing .env. Copy .env.example to .env and set DATABASE_URL (and DB_PASSWORD if using ./postgres/setup.sh)."
  exit 1
fi

set -a
# shellcheck disable=SC1091
source ".env"
set +a

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$ROOT/.dbt}"

if command -v pg_isready >/dev/null 2>&1; then
  PGHOST="${PGHOST:-localhost}"
  PGPORT="${PGPORT:-5432}"
  pg_isready -h "$PGHOST" -p "$PGPORT" >/dev/null 2>&1 || {
    echo "Postgres not ready at ${PGHOST}:${PGPORT}. Start it (brew services start postgresql@14) then retry."
    exit 1
  }
fi

python -m etl.load_raw

# Ensure core.player_elo_history exists so dbt elo marts can build even before first compute.
python scripts/elo_compute.py --init-only

dbt run

python scripts/elo_compute.py

echo "Pipeline complete."
