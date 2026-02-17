#!/usr/bin/env bash
#
# Create the Postgres database, user, and schemas for the FIVB leaderboard ETL and dbt.
# Run from project root. Requires Postgres client (psql) and a superuser connection.
#
# Usage:
#   ./postgres/setup.sh
#
# Optional env vars (set before running or export):
#   DB_NAME     database name (default: fivb_leaderboard)
#   DB_USER     database user for ETL/dbt (default: fivb_leaderboard)
#   DB_PASSWORD password for DB_USER (required unless user already exists)
#   PGHOST      Postgres host (default: localhost)
#   PGPORT      Postgres port (default: 5432)
#   PGUSER      Superuser for creating DB/role (default: current OS user or postgres)
#
# After running, set DATABASE_URL in .env to:
#   postgresql+psycopg2://DB_USER:DB_PASSWORD@PGHOST:PGPORT/DB_NAME
# and use the same user/password in ~/.dbt/profiles.yml.

set -e

DB_NAME="${DB_NAME:-fivb_leaderboard}"
DB_USER="${DB_USER:-fivb_leaderboard}"
DB_PASSWORD="${DB_PASSWORD:?Set DB_PASSWORD (e.g. export DB_PASSWORD=yourpassword)}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
# Superuser: default to $PGUSER if set, else try postgres, else current user
if [[ -z "${PGUSER}" ]]; then
  if command -v whoami &>/dev/null; then
    PGUSER="$(whoami)"
  else
    PGUSER="postgres"
  fi
fi

echo "Creating database and user (superuser: $PGUSER) ..."

# Create role (idempotent)
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 <<EOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$DB_USER') THEN
    CREATE ROLE $DB_USER WITH LOGIN PASSWORD '$DB_PASSWORD';
  ELSE
    ALTER ROLE $DB_USER WITH PASSWORD '$DB_PASSWORD';
  END IF;
END
\$\$;
EOF

# Create database (must be outside transaction; ignore error if exists)
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $DB_NAME OWNER $DB_USER" 2>/dev/null || true
if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1; then
  echo "Failed to create database $DB_NAME"
  exit 1
fi

# Ensure app user can create schemas (for ensure_raw_tables)
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 -c "GRANT CREATE ON DATABASE $DB_NAME TO $DB_USER" 2>/dev/null || true

# Create schemas and grants in the target database
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<EOF
-- Schemas used by ETL (raw) and dbt (staging, core, mart)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- Grant usage and create so ETL/dbt can create tables and views as DB_USER
GRANT USAGE, CREATE ON SCHEMA raw TO $DB_USER;
GRANT USAGE, CREATE ON SCHEMA staging TO $DB_USER;
GRANT USAGE, CREATE ON SCHEMA core TO $DB_USER;
GRANT USAGE, CREATE ON SCHEMA mart TO $DB_USER;
EOF

echo "Done. Database '$DB_NAME' and user '$DB_USER' are ready."
echo "Set in .env: DATABASE_URL=postgresql+psycopg2://$DB_USER:****@$PGHOST:$PGPORT/$DB_NAME"
echo "Use the same user and password in ~/.dbt/profiles.yml."
