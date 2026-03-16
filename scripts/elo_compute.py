#!/usr/bin/env python3
"""
Compute player Elo ratings from H2H match results.

Writes two tables:
  - core.player_elo_history: standard Elo (all matches count equally).
  - core.player_elo_round_weighted_history: round-weighted Elo (finals/semis count more than pool).

Reads from the dbt-built view mart.elo_match_feed (run `dbt run` first). Uses the same DB as dbt/ETL: set DATABASE_URL (or .env) before running.

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
from tqdm import tqdm

from etl.db import get_engine
from etl.config import get_db_config

INITIAL_ELO = 1500.0
K = 32.0

# Round weights for round-weighted Elo: later/knockout rounds count more than pool play.
ROUND_WEIGHT_FINAL = 2.0
ROUND_WEIGHT_SEMI = 1.5
ROUND_WEIGHT_QUARTER = 1.25
ROUND_WEIGHT_RO16 = 1.1
ROUND_WEIGHT_POOL = 0.75
ROUND_WEIGHT_DEFAULT = 1.0


def round_weight(
    round_phase: str | None,
    round_name: str | None,
    is_final: bool | None,
    is_pool_phase: bool | None,
) -> float:
    """Return multiplier for this match's round (finals > semi > quarter > ... > pool)."""
    if is_final:
        return ROUND_WEIGHT_FINAL
    combined = " ".join(
        str(x).lower() for x in (round_phase or "", round_name or "") if x
    )
    if "semi" in combined or "final four" in combined:
        return ROUND_WEIGHT_SEMI
    if "quarter" in combined or "quarterfinal" in combined:
        return ROUND_WEIGHT_QUARTER
    if "round of 16" in combined or "ro16" in combined or "16th" in combined:
        return ROUND_WEIGHT_RO16
    if is_pool_phase:
        return ROUND_WEIGHT_POOL
    return ROUND_WEIGHT_DEFAULT


def expected_score(elo_a: float, elo_b: float) -> float:
    """Probability that side A wins: 1 / (1 + 10^((elo_b - elo_a)/400))."""
    return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))


def _to_date(val) -> date | None:
    """Convert tournament_start_date (or similar) to date for as_of_date."""
    if val is None:
        return None
    if hasattr(val, "date"):
        return val.date()
    s = str(val)[:10]
    if not s or s == "None":
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def run_elo(engine) -> tuple[list[dict], list[dict]]:
    """Read mart.elo_match_feed, compute standard and round-weighted Elo per gender over time.
    Returns (history_standard, history_round_weighted).
    Uses match_date for ordering and as_of_date. Round-weighted Elo uses K * round_weight per match."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                select match_id, match_date, tournament_gender,
                       team1_player_a_id, team1_player_b_id,
                       team2_player_a_id, team2_player_b_id,
                       is_winner_team1,
                       round_phase, round_name, is_final, is_pool_phase
                from mart.elo_match_feed
                where match_date is not null
                order by tournament_gender, match_date, match_id
            """)
        ).fetchall()

    history: list[dict] = []
    history_rw: list[dict] = []
    current: dict[str, dict[int, float]] = {}
    current_rw: dict[str, dict[int, float]] = {}

    for r in tqdm(rows, desc="Elo compute", unit="match"):
        (
            match_id,
            match_date,
            gender,
            t1_pa,
            t1_pb,
            t2_pa,
            t2_pb,
            is_winner_team1,
            round_phase,
            round_name,
            is_final,
            is_pool_phase,
        ) = r
        as_of = _to_date(match_date)
        if as_of is None:
            continue
        if gender not in current:
            current[gender] = {}
            current_rw[gender] = {}

        def elo(pid: int) -> float:
            return current[gender].get(pid, INITIAL_ELO)

        def elo_rw(pid: int) -> float:
            return current_rw[gender].get(pid, INITIAL_ELO)

        team1_elo = (elo(t1_pa) + elo(t1_pb)) / 2.0
        team2_elo = (elo(t2_pa) + elo(t2_pb)) / 2.0
        e1 = expected_score(team1_elo, team2_elo)
        s1 = 1.0 if is_winner_team1 else 0.0

        # Standard Elo
        delta_team1 = K * (s1 - e1)
        delta_team2 = -delta_team1
        half = 0.5
        current[gender][t1_pa] = elo(t1_pa) + half * delta_team1
        current[gender][t1_pb] = elo(t1_pb) + half * delta_team1
        current[gender][t2_pa] = elo(t2_pa) + half * delta_team2
        current[gender][t2_pb] = elo(t2_pb) + half * delta_team2

        # Round-weighted Elo: same formula with K * weight
        w = round_weight(round_phase, round_name, is_final, is_pool_phase)
        team1_elo_rw = (elo_rw(t1_pa) + elo_rw(t1_pb)) / 2.0
        team2_elo_rw = (elo_rw(t2_pa) + elo_rw(t2_pb)) / 2.0
        e1_rw = expected_score(team1_elo_rw, team2_elo_rw)
        delta_team1_rw = K * w * (s1 - e1_rw)
        delta_team2_rw = -delta_team1_rw
        current_rw[gender][t1_pa] = elo_rw(t1_pa) + half * delta_team1_rw
        current_rw[gender][t1_pb] = elo_rw(t1_pb) + half * delta_team1_rw
        current_rw[gender][t2_pa] = elo_rw(t2_pa) + half * delta_team2_rw
        current_rw[gender][t2_pb] = elo_rw(t2_pb) + half * delta_team2_rw

        for pid in (t1_pa, t1_pb, t2_pa, t2_pb):
            history.append({
                "player_id": pid,
                "gender": gender,
                "as_of_date": as_of,
                "match_id": match_id,
                "elo_rating": round(current[gender][pid], 2),
            })
            history_rw.append({
                "player_id": pid,
                "gender": gender,
                "as_of_date": as_of,
                "match_id": match_id,
                "elo_rating": round(current_rw[gender][pid], 2),
            })

    return history, history_rw


def ensure_table(engine) -> None:
    """Create core schema and Elo history tables (standard + round-weighted) if they do not exist."""
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
    create table if not exists core.player_elo_round_weighted_history (
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


def _write_elo_history(
    engine,
    history: list[dict],
    table: str,
    desc: str = "Write history",
) -> int:
    """Truncate table and insert history rows. Dedupes by (player_id, gender, match_id).
    Returns the number of rows written (after dedupe)."""
    with engine.begin() as conn:
        conn.execute(text(f"truncate table {table}"))
    if not history:
        return 0
    seen: dict[tuple[int, str, int], dict] = {}
    for row in history:
        key = (row["player_id"], row["gender"], row["match_id"])
        seen[key] = row
    history = list(seen.values())
    batch_size = 1000
    num_batches = (len(history) + batch_size - 1) // batch_size
    with engine.begin() as conn:
        for i in tqdm(
            range(0, len(history), batch_size),
            total=num_batches,
            desc=desc,
            unit="batch",
        ):
            batch = history[i : i + batch_size]
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
                f"insert into {table} (player_id, gender, as_of_date, match_id, elo_rating) "
                "values " + ", ".join(placeholders) + " "
                "on conflict (player_id, gender, match_id) do update set as_of_date = excluded.as_of_date, elo_rating = excluded.elo_rating"
            )
            conn.execute(text(sql), params)
    return len(history)


def write_history(engine, history: list[dict]) -> int:
    """Truncate core.player_elo_history and insert new rows. See _write_elo_history."""
    return _write_elo_history(
        engine, history, "core.player_elo_history", desc="Write history"
    )


def write_round_weighted_history(engine, history_rw: list[dict]) -> int:
    """Truncate core.player_elo_round_weighted_history and insert new rows."""
    return _write_elo_history(
        engine,
        history_rw,
        "core.player_elo_round_weighted_history",
        desc="Write round-weighted history",
    )


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Compute player Elo from mart.elo_match_feed and write core.player_elo_history.")
    parser.add_argument("--init-only", action="store_true", help="Only create core schema and table; do not read feed or write history (use before first dbt run so elo marts can be built).")
    args = parser.parse_args()

    get_db_config()  # raise early if DATABASE_URL missing
    engine = get_engine()
    ensure_table(engine)
    if args.init_only:
        print("Created core.player_elo_history and core.player_elo_round_weighted_history (empty). Run dbt run, then run this script without --init-only to populate.")
        return
    print("Reading mart.elo_match_feed…")
    history, history_rw = run_elo(engine)
    print(f"Computed {len(history)} standard and {len(history_rw)} round-weighted history rows.")
    written = write_history(engine, history)
    written_rw = write_round_weighted_history(engine, history_rw)
    print(f"Wrote {written} rows to core.player_elo_history, {written_rw} to core.player_elo_round_weighted_history.")


if __name__ == "__main__":
    main()
