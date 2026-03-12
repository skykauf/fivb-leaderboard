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

# Pipeline always does incremental load (upsert); never truncate raw tables.
unset TRUNCATE_RAW

# Optional: skip pulling data from VIS (only run dbt + elo compute on existing raw data).
SKIP_FETCH=0
for arg in "$@"; do
  case "$arg" in
    --no-fetch) SKIP_FETCH=1 ;;
    --help|-h)
      echo "Usage: $0 [--no-fetch]"
      echo "  --no-fetch  Skip VIS/ETL step; only run dbt and elo compute on existing raw data."
      exit 0
      ;;
  esac
done

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$ROOT/.dbt}"

if command -v pg_isready >/dev/null 2>&1; then
  PGHOST="${PGHOST:-localhost}"
  PGPORT="${PGPORT:-5432}"
  pg_isready -h "$PGHOST" -p "$PGPORT" >/dev/null 2>&1 || {
    echo "Postgres not ready at ${PGHOST}:${PGPORT}. Start it (brew services start postgresql@14) then retry."
    exit 1
  }
fi

if [[ "$SKIP_FETCH" -eq 0 ]]; then
  python -m etl.load_raw
else
  echo "Skipping VIS fetch (--no-fetch). Using existing raw data."
fi

# Ensure core.player_elo_history exists so dbt elo marts can build even before first compute.
python scripts/elo_compute.py --init-only

dbt run

python scripts/elo_compute.py

echo "Pipeline complete."
