from __future__ import annotations

import json
from typing import Any, Iterable, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import get_db_config


def get_engine() -> Engine:
    cfg = get_db_config()
    return create_engine(cfg.url, future=True)


def drop_all_schemas(engine: Engine) -> None:
    """
    Drop all tables and views in project schemas (raw + dbt staging/core/mart) so the database can start anew.
    Drops objects inside the schemas rather than the schemas themselves, so schema-owner privileges are not required.
    """
    schemas = ("raw", "staging", "core", "mart")
    # Order: drop views and materialized views first, then tables (CASCADE removes dependents)
    type_order = ("VIEW", "MATERIALIZED VIEW", "BASE TABLE", "FOREIGN TABLE")
    with engine.begin() as conn:
        for schema in schemas:
            rows = conn.execute(
                text("""
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW', 'FOREIGN TABLE')
                """),
                {"schema": schema},
            ).fetchall()
            for table_type in type_order:
                for (table_name, ttype) in rows:
                    if ttype != table_type:
                        continue
                    quoted = f'"{schema}"."{table_name}"'
                    if ttype == "VIEW":
                        conn.execute(text(f"DROP VIEW IF EXISTS {quoted} CASCADE"))
                    elif ttype == "MATERIALIZED VIEW":
                        conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {quoted} CASCADE"))
                    else:
                        conn.execute(text(f"DROP TABLE IF EXISTS {quoted} CASCADE"))


def ensure_raw_tables(engine: Engine) -> None:
    """
    Create raw schema and base tables if they don't exist.

    These are intentionally close to the JSON structure and can be refined
    once the exact VIS payloads are known.
    """
    ddl_statements = [
        """
        create schema if not exists raw;
        """,
        """
        create table if not exists raw.raw_fivb_players (
            player_id        bigint primary key,
            first_name       text,
            last_name        text,
            full_name        text,
            gender           text,
            birth_date       date,
            height_cm        integer,
            country_code     text,
            profile_url      text,
            payload          jsonb,
            ingested_at      timestamptz default now()
        );
        """,
        """
        create table if not exists raw.raw_fivb_teams (
            team_id          bigint,
            tournament_id    bigint,
            player_a_id      bigint,
            player_b_id      bigint,
            country_code     text,
            status           text,
            valid_from       date,
            valid_to         date,
            payload          jsonb,
            ingested_at      timestamptz default now(),
            primary key (team_id)
        );
        """,
        """
        create table if not exists raw.raw_fivb_tournaments (
            tournament_id    bigint primary key,
            name             text,
            season           text,
            tier             text,
            start_date       date,
            end_date         date,
            city             text,
            country_code     text,
            country_name     text,
            gender           text,
            status           text,
            timezone         text,
            payload          jsonb,
            ingested_at      timestamptz default now()
        );
        """,
        """
        create table if not exists raw.raw_fivb_matches (
            match_id         bigint primary key,
            tournament_id    bigint,
            phase            text,
            round            text,
            team1_id         bigint,
            team2_id         bigint,
            winner_team_id   bigint,
            score_sets       text,
            duration_minutes integer,
            played_at        timestamptz,
            result_type      text,
            status           text,
            payload          jsonb,
            ingested_at      timestamptz default now()
        );
        """,
        """
        create table if not exists raw.raw_fivb_rankings (
            ranking_type     text,
            snapshot_date    date,
            player_id        bigint,
            rank             integer,
            points           integer,
            payload          jsonb,
            ingested_at      timestamptz default now(),
            primary key (ranking_type, snapshot_date, player_id)
        );
        """,
        """
        create table if not exists raw.raw_fivb_results (
            tournament_id    bigint,
            team_id          bigint,
            finishing_pos    integer,
            points           integer,
            prize_money      numeric,
            ingested_at      timestamptz default now(),
            primary key (tournament_id, team_id)
        );
        """,
    ]

    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))
        # Migrate season to text if it was created as integer (VIS sends e.g. "1991-92")
        try:
            conn.execute(
                text(
                    "ALTER TABLE raw.raw_fivb_tournaments ALTER COLUMN season TYPE text USING season::text"
                )
            )
        except Exception:
            pass
    # Add primary keys for existing tables (each in its own transaction so one failure doesn't abort the rest).
    for alter in [
        "ALTER TABLE raw.raw_fivb_players ADD PRIMARY KEY (player_id)",
        "ALTER TABLE raw.raw_fivb_teams ADD PRIMARY KEY (team_id)",
        "ALTER TABLE raw.raw_fivb_tournaments ADD PRIMARY KEY (tournament_id)",
        "ALTER TABLE raw.raw_fivb_matches ADD PRIMARY KEY (match_id)",
        "ALTER TABLE raw.raw_fivb_rankings ADD PRIMARY KEY (ranking_type, snapshot_date, player_id)",
        "ALTER TABLE raw.raw_fivb_results ADD PRIMARY KEY (tournament_id, team_id)",
    ]:
        try:
            with engine.begin() as conn:
                conn.execute(text(alter))
        except Exception as e:
            orig = getattr(e, "orig", None)
            pgcode = getattr(orig, "pgcode", None) if orig else None
            if pgcode == "42P16":
                pass  # already has primary key
            elif pgcode == "23505":
                raise RuntimeError(
                    "Raw table has duplicate keys; run with TRUNCATE_RAW=1 once, then re-run."
                ) from e
            else:
                raise


def truncate_raw_tables(engine: Engine) -> None:
    """Truncate all raw tables so the next load is a full refresh (no duplicate buildup)."""
    tables = [
        "raw.raw_fivb_results",
        "raw.raw_fivb_rankings",
        "raw.raw_fivb_matches",
        "raw.raw_fivb_teams",
        "raw.raw_fivb_tournaments",
        "raw.raw_fivb_players",
    ]
    table_list = ", ".join(tables)
    with engine.begin() as conn:
        try:
            conn.execute(text(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE"))
        except Exception as e:
            orig = getattr(e, "orig", None)
            if orig is not None and getattr(orig, "pgcode", None) == "42P01":
                pass  # undefined_table - tables not created yet
            else:
                raise


def _serialize_for_db(value: Any) -> Any:
    """Serialize dict/list to JSON string for JSONB columns; pass through others."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def get_distinct_team_player_ids(engine: Engine) -> list[int]:
    """Return distinct non-null player IDs from raw.raw_fivb_teams (player_a_id, player_b_id)."""
    q = text(
        """
        SELECT DISTINCT id FROM (
            SELECT player_a_id AS id FROM raw.raw_fivb_teams WHERE player_a_id IS NOT NULL
            UNION
            SELECT player_b_id AS id FROM raw.raw_fivb_teams WHERE player_b_id IS NOT NULL
        ) u
        WHERE id IS NOT NULL
        ORDER BY id
    """
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(q)]


def bulk_insert(
    engine: Engine,
    table: str,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    """
    Simple helper to bulk insert JSON-like dicts into a table.
    Dict/list values are serialized to JSON for JSONB columns.
    """
    rows = list(rows)
    if not rows:
        return

    columns = sorted(rows[0].keys())
    col_list = ", ".join(columns)
    param_list = ", ".join(f":{c}" for c in columns)
    sql = text(f"insert into {table} ({col_list}) values ({param_list})")

    serialized = [{k: _serialize_for_db(v) for k, v in row.items()} for row in rows]

    with engine.begin() as conn:
        conn.execute(sql, serialized)


# Conflict targets for raw tables (for upsert). Columns that form the primary key.
RAW_CONFLICT_COLUMNS: dict[str, tuple[str, ...]] = {
    "raw.raw_fivb_players": ("player_id",),
    "raw.raw_fivb_teams": ("team_id",),
    "raw.raw_fivb_tournaments": ("tournament_id",),
    "raw.raw_fivb_matches": ("match_id",),
    "raw.raw_fivb_rankings": ("ranking_type", "snapshot_date", "player_id"),
    "raw.raw_fivb_results": ("tournament_id", "team_id"),
}


def bulk_upsert(
    engine: Engine,
    table: str,
    rows: Iterable[Mapping[str, Any]],
    conflict_columns: tuple[str, ...],
) -> None:
    """
    Insert or update rows (upsert) by conflict_columns. Uses ON CONFLICT DO UPDATE.
    Table must have a unique constraint or primary key on conflict_columns.
    """
    rows = list(rows)
    if not rows:
        return

    columns = sorted(rows[0].keys())
    conflict_set = set(conflict_columns)
    update_columns = [c for c in columns if c not in conflict_set]
    if not update_columns:
        return

    col_list = ", ".join(columns)
    param_list = ", ".join(f":{c}" for c in columns)
    conflict_list = ", ".join(conflict_columns)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
    # Refresh ingested_at on conflict so we know when the row was last updated
    set_clause = set_clause + ", ingested_at = now()"
    sql = text(
        f"INSERT INTO {table} ({col_list}) VALUES ({param_list}) "
        f"ON CONFLICT ({conflict_list}) DO UPDATE SET {set_clause}"
    )

    serialized = [{k: _serialize_for_db(v) for k, v in row.items()} for row in rows]

    with engine.begin() as conn:
        conn.execute(sql, serialized)
