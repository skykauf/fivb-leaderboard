#!/usr/bin/env python3
"""
Compute player Elo ratings from H2H match results and write to core.player_elo_history.

Reads from the dbt-built view mart.elo_match_feed (run `dbt run` first so the view exists).
Uses the same DB as dbt/ETL: set DATABASE_URL (or .env) before running.

  python scripts/elo_compute.py

Or from project root:
  python -m scripts.elo_compute
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

# Project root on path so we can import etl
if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from sqlalchemy import text

from etl.db import get_engine
from etl.config import get_db_config

INITIAL_ELO = 1500.0
K = 32.0


def expected_score(elo_a: float, elo_b: float) -> float:
    """Probability that side A wins: 1 / (1 + 10^((elo_b - elo_a)/400))."""
    return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))


def run_elo(engine) -> list[dict]:
    """Read mart.elo_match_feed, compute Elo per gender over time, return list of history rows."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                select match_id, played_at, tournament_gender,
                       team1_player_a_id, team1_player_b_id,
                       team2_player_a_id, team2_player_b_id,
                       is_winner_team1
                from mart.elo_match_feed
                order by tournament_gender, played_at
            """)
        ).fetchall()

    history: list[dict] = []
    # current_elo[gender][player_id] = elo
    current: dict[str, dict[int, float]] = {}

    for r in rows:
        match_id, played_at, gender, t1_pa, t1_pb, t2_pa, t2_pb, is_winner_team1 = r
        if played_at is None:
            continue  # skip matches without a played_at (can't order or set as_of_date)
        if gender not in current:
            current[gender] = {}

        def elo(pid: int) -> float:
            return current[gender].get(pid, INITIAL_ELO)

        team1_elo = (elo(t1_pa) + elo(t1_pb)) / 2.0
        team2_elo = (elo(t2_pa) + elo(t2_pb)) / 2.0
        e1 = expected_score(team1_elo, team2_elo)
        s1 = 1.0 if is_winner_team1 else 0.0
        delta_team1 = K * (s1 - e1)
        delta_team2 = -delta_team1
        half = 0.5
        current[gender][t1_pa] = elo(t1_pa) + half * delta_team1
        current[gender][t1_pb] = elo(t1_pb) + half * delta_team1
        current[gender][t2_pa] = elo(t2_pa) + half * delta_team2
        current[gender][t2_pb] = elo(t2_pb) + half * delta_team2

        as_of = played_at.date() if hasattr(played_at, "date") else date.fromisoformat(str(played_at)[:10])
        for pid in (t1_pa, t1_pb, t2_pa, t2_pb):
            history.append({
                "player_id": pid,
                "gender": gender,
                "as_of_date": as_of,
                "match_id": match_id,
                "elo_rating": round(current[gender][pid], 2),
            })

    return history


def ensure_table(engine) -> None:
    """Create core schema and player_elo_history table if they do not exist."""
    ddl = """
    create schema if not exists core;
    create table if not exists core.player_elo_history (
        player_id   bigint not null,
        gender      text not null,
        as_of_date  date not null,
        match_id    bigint not null,
        elo_rating numeric not null,
        primary key (player_id, gender, match_id)
    );
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))


def write_history(engine, history: list[dict]) -> None:
    """Truncate core.player_elo_history and insert new rows."""
    with engine.begin() as conn:
        conn.execute(text("truncate table core.player_elo_history"))
    if not history:
        return
    columns = ["player_id", "gender", "as_of_date", "match_id", "elo_rating"]
    # Use raw insert in batches to avoid adding pandas/sqlalchemy bulk dependency
    with engine.begin() as conn:
        for i in range(0, len(history), 5000):
            batch = history[i : i + 5000]
            placeholders = []
            params = {}
            for j, row in enumerate(batch):
                placeholders.append(
                    f"(:p{j}_0, :p{j}_1, :p{j}_2, :p{j}_3, :p{j}_4)"
                )
                params[f"p{j}_0"] = row["player_id"]
                params[f"p{j}_1"] = row["gender"]
                params[f"p{j}_2"] = row["as_of_date"]
                params[f"p{j}_3"] = row["match_id"]
                params[f"p{j}_4"] = row["elo_rating"]
            sql = (
                "insert into core.player_elo_history (player_id, gender, as_of_date, match_id, elo_rating) "
                "values " + ", ".join(placeholders)
            )
            conn.execute(text(sql), params)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Compute player Elo from mart.elo_match_feed and write core.player_elo_history.")
    parser.add_argument("--init-only", action="store_true", help="Only create core schema and table; do not read feed or write history (use before first dbt run so elo marts can be built).")
    args = parser.parse_args()

    get_db_config()  # raise early if DATABASE_URL missing
    engine = get_engine()
    ensure_table(engine)
    if args.init_only:
        print("Created core.player_elo_history (empty). Run dbt run, then run this script without --init-only to populate.")
        return
    history = run_elo(engine)
    write_history(engine, history)
    print(f"Wrote {len(history)} player-elo rows to core.player_elo_history.")


if __name__ == "__main__":
    main()
