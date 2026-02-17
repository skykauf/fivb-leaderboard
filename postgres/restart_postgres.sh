#!/usr/bin/env bash
#
# Drop the FIVB leaderboard database and role, then run setup.sh for a clean slate.
# Use this to fix permission issues or start over without leftover state.
#
# Run from project root:
#   DB_PASSWORD=yourpassword ./postgres/restart_postgres.sh
#
# Uses the same env vars as postgres/setup.sh (DB_NAME, DB_USER, PGHOST, PGPORT, PGUSER).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DB_NAME="${DB_NAME:-fivb_leaderboard}"
DB_USER="${DB_USER:-fivb_leaderboard}"
DB_PASSWORD="${DB_PASSWORD:?Set DB_PASSWORD (e.g. export DB_PASSWORD=yourpassword)}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
if [[ -z "${PGUSER}" ]]; then
  PGUSER="$(whoami 2>/dev/null)" || PGUSER="postgres"
fi

echo "Stopping connections to database '$DB_NAME' ..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 <<EOF 2>/dev/null || true
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();
EOF

echo "Dropping database '$DB_NAME' and role '$DB_USER' ..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -v ON_ERROR_STOP=1 <<EOF
DROP DATABASE IF EXISTS $DB_NAME;
DROP ROLE IF EXISTS $DB_USER;
EOF

echo "Creating fresh database and schemas (running setup.sh) ..."
cd "$ROOT_DIR"
DB_NAME="$DB_NAME" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" PGHOST="$PGHOST" PGPORT="$PGPORT" PGUSER="$PGUSER" "$SCRIPT_DIR/setup.sh"

echo ""
echo "Restart complete. Use DATABASE_URL=postgresql+psycopg2://$DB_USER:****@$PGHOST:$PGPORT/$DB_NAME in .env"
