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
