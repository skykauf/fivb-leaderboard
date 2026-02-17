#!/usr/bin/env bash
set -euo pipefail

# Quick Postgres setup for the FIVB leaderboard project.
# This script creates:
#   - a database (default: fivb_leaderboard)
#   - a user (default: fivb_username) with a password
#   - grants the user privileges on the database
#
# You can override defaults with environment variables:
#   DB_NAME, DB_USER, DB_PASSWORD
#
# Example:
#   DB_NAME=fivb_leaderboard DB_USER=fivb_username DB_PASSWORD=supersecret ./postgres/setup.sh
#
# Requires:
#   - `psql` available on PATH
#   - the current OS/psql user has permission to create dbs/users.

DB_NAME="${DB_NAME:-fivb_leaderboard}"
DB_USER="${DB_USER:-fivb_username}"
DB_PASSWORD="${DB_PASSWORD:-fivb_password}"

echo "Creating Postgres database and user for FIVB leaderboard..."
echo "  DB_NAME=${DB_NAME}"
echo "  DB_USER=${DB_USER}"

# Connect explicitly to the built‑in 'postgres' database so we don't depend
# on a per-user database existing.

# Create database if it does not exist
if ! psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
  echo "Creating database '${DB_NAME}'..."
  psql -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${DB_NAME}\";"
else
  echo "Database '${DB_NAME}' already exists."
fi

# Create user if it does not exist
if ! psql -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
  echo "Creating user '${DB_USER}'..."
  psql -d postgres -v ON_ERROR_STOP=1 -c "CREATE USER \"${DB_USER}\" WITH PASSWORD '${DB_PASSWORD}';"
else
  echo "User '${DB_USER}' already exists."
fi

echo "Granting privileges on database '${DB_NAME}' to '${DB_USER}'..."
psql -d postgres -v ON_ERROR_STOP=1 -c "GRANT ALL PRIVILEGES ON DATABASE \"${DB_NAME}\" TO \"${DB_USER}\";"

echo "Creating schemas (raw, analytics) and granting to '${DB_USER}'..."
psql -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "
  CREATE SCHEMA IF NOT EXISTS raw;
  CREATE SCHEMA IF NOT EXISTS analytics;
  GRANT ALL ON SCHEMA raw TO \"${DB_USER}\";
  GRANT ALL ON SCHEMA analytics TO \"${DB_USER}\";
  GRANT CREATE ON SCHEMA raw TO \"${DB_USER}\";
  GRANT CREATE ON SCHEMA analytics TO \"${DB_USER}\";
  GRANT ALL ON ALL TABLES IN SCHEMA raw TO \"${DB_USER}\";
  GRANT ALL ON ALL TABLES IN SCHEMA analytics TO \"${DB_USER}\";
  ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO \"${DB_USER}\";
  ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO \"${DB_USER}\";
"

cat <<EOF

Postgres setup complete.

Next steps:

1) Set your DATABASE_URL (for Python ETL) – e.g. in .env at project root:

   DATABASE_URL=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@localhost:5432/${DB_NAME}

2) Add/confirm this dbt profile in ~/.dbt/profiles.yml:

   fivb_leaderboard:
     target: dev
     outputs:
       dev:
         type: postgres
         host: localhost
         user: ${DB_USER}
         password: ${DB_PASSWORD}
         port: 5432
         dbname: ${DB_NAME}
         schema: analytics
         threads: 4

3) Run:

   pip install -e .
   python -m etl.load_raw
   dbt run

EOF

