"""
Raw ingestion from the FIVB VIS Web Service (Volleyball competitions – Beach).

Pipeline follows the endpoints listed under:
  https://www.fivb.org/VisSDK/VisWebService/#Volleyball%20competitions.html
and GetBeachTournamentRanking: https://www.fivb.org/VisSDK/VisWebService/#GetBeachTournamentRanking.html

Endpoint order used:
  1. GetBeachTournamentList  -> raw_fivb_tournaments
  2. GetBeachTeamList (single request, all teams) -> raw_fivb_teams
  3. GetPlayerList (single request, all players) -> raw_fivb_players
  4. Per tournament: GetBeachMatchList, GetBeachTournamentRanking -> raw_fivb_matches, raw_fivb_results
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .db import (
    bulk_upsert,
    ensure_raw_tables,
    get_engine,
    RAW_CONFLICT_COLUMNS,
    truncate_raw_tables,
)
from .vis_client import (
    fetch_beach_matches_for_tournament,
    fetch_beach_teams,
    fetch_beach_tournament_ranking,
    fetch_beach_tournaments,
    fetch_player_list,
    TOURNAMENT_SEASON,
)

# VIS request types (Volleyball competitions – Beach); order matches run_full_ingestion.
VIS_GET_BEACH_TOURNAMENT_LIST = "GetBeachTournamentList"
VIS_GET_BEACH_TEAM_LIST = "GetBeachTeamList"
VIS_GET_BEACH_MATCH_LIST = "GetBeachMatchList"
VIS_GET_BEACH_TOURNAMENT_RANKING = "GetBeachTournamentRanking"
VIS_GET_PLAYER_LIST = "GetPlayerList"


@dataclass(frozen=True)
class IngestionLimits:
    """Optional caps for per-tournament retrieval and parallelism. Tournaments, teams, and players are loaded in full."""

    tournaments: int | None = (
        None  # max tournaments to process for matches/results (None = all from list)
    )
    matches_per_tournament: int | None = None
    results_per_tournament: int | None = None
    max_workers: int = 4  # threads for per-tournament matches+results (1 = sequential)

    @classmethod
    def from_env(cls) -> "IngestionLimits":
        import os

        def _int(key: str) -> int | None:
            v = os.environ.get(key)
            return int(v) if v not in (None, "") else None

        def _parallel_enabled() -> bool:
            v = os.environ.get("ETL_PARALLEL", "").strip().lower()
            if v in ("0", "false", "no", "off"):
                return False
            if v in ("1", "true", "yes", "on") or v == "":
                return True
            return True  # default on for any other value

        workers = _int("ETL_MAX_WORKERS")
        if not _parallel_enabled():
            workers = 1
        elif workers is None or workers < 1:
            workers = 4
        return cls(
            tournaments=_int("LIMIT_TOURNAMENTS"),
            matches_per_tournament=_int("LIMIT_MATCHES_PER_TOURNAMENT"),
            results_per_tournament=_int("LIMIT_RESULTS_PER_TOURNAMENT"),
            max_workers=workers,
        )


def _int_or_none(value: Any) -> int | None:
    """Parse int or return None if missing/empty."""
    if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_or_none(value: Any) -> date | None:
    """Parse date string or return None if missing/empty."""
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    try:
        from datetime import datetime

        if isinstance(value, str):
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        pass
    return None


def _normalize_tournament_vis(raw: Dict[str, Any]) -> Dict[str, Any]:
    """VIS: No, Name, CountryCode, CountryName, StartDate, EndDate, Season, Type, City, Gender, Status."""
    return {
        "tournament_id": raw.get("No"),
        "name": raw.get("Name"),
        "season": raw.get("Season"),
        "tier": raw.get("Type"),
        "start_date": _date_or_none(raw.get("StartDate")),
        "end_date": _date_or_none(raw.get("EndDate")),
        "city": raw.get("City") or None,
        "country_code": raw.get("CountryCode"),
        "country_name": raw.get("CountryName") or None,
        "gender": str(raw["Gender"]) if raw.get("Gender") is not None else None,
        "status": str(raw["Status"]) if raw.get("Status") is not None else None,
        "timezone": None,
        "payload": raw,
    }


def _normalize_team_vis(raw: Dict[str, Any]) -> Dict[str, Any]:
    """VIS: No, NoTournament, NoPlayer1, NoPlayer2, CountryCode, Status."""
    return {
        "team_id": raw.get("No"),
        "tournament_id": raw.get("NoTournament"),
        "player_a_id": raw.get("NoPlayer1"),
        "player_b_id": raw.get("NoPlayer2"),
        "country_code": raw.get("CountryCode"),
        "status": str(raw["Status"]) if raw.get("Status") is not None else None,
        "valid_from": None,
        "valid_to": None,
        "payload": raw,
    }


def _normalize_match_vis(no_tournament: int, raw: Dict[str, Any]) -> Dict[str, Any]:
    """VIS: No, NoTournament, NoRound, NoTeamA, NoTeamB, MatchPointsA/B, DateTimeLocal, ResultType, Status."""
    points_a, points_b = raw.get("MatchPointsA"), raw.get("MatchPointsB")
    team_a, team_b = raw.get("NoTeamA"), raw.get("NoTeamB")
    winner = None
    if (
        points_a is not None
        and points_b is not None
        and team_a is not None
        and team_b is not None
    ):
        try:
            if int(points_a) > int(points_b):
                winner = team_a
            elif int(points_b) > int(points_a):
                winner = team_b
        except (TypeError, ValueError):
            pass
    return {
        "match_id": raw.get("No"),
        "tournament_id": raw.get("NoTournament") or no_tournament,
        "phase": None,
        "round": raw.get("NoRound"),
        "team1_id": team_a,
        "team2_id": team_b,
        "winner_team_id": winner,
        "score_sets": _format_score_vis(points_a, points_b),
        "duration_minutes": None,
        "played_at": raw.get("DateTimeLocal"),
        "result_type": (
            str(raw["ResultType"]) if raw.get("ResultType") is not None else None
        ),
        "status": str(raw["Status"]) if raw.get("Status") is not None else None,
        "payload": raw,
    }


def _format_score_vis(points_a: Any, points_b: Any) -> str | None:
    if points_a is not None and points_b is not None:
        return f"{points_a}-{points_b}"
    return None


def load_tournaments(engine: Engine) -> List[Dict[str, Any]]:
    """Load from VIS GetBeachTournamentList into raw.raw_fivb_tournaments. Returns raw list for downstream use."""
    tournaments_raw = fetch_beach_tournaments(
        filter_expr=f"Season='{TOURNAMENT_SEASON}'"
    )
    if not tournaments_raw:
        raise RuntimeError(f"VIS {VIS_GET_BEACH_TOURNAMENT_LIST} returned no data")
    rows = [_normalize_tournament_vis(t) for t in tournaments_raw]
    if not rows:
        raise RuntimeError(
            f"VIS {VIS_GET_BEACH_TOURNAMENT_LIST} returned no valid tournaments after normalize"
        )
    bulk_upsert(
        engine,
        "raw.raw_fivb_tournaments",
        rows,
        RAW_CONFLICT_COLUMNS["raw.raw_fivb_tournaments"],
    )
    print(f"  Loaded {len(rows)} tournaments -> raw.raw_fivb_tournaments")
    return tournaments_raw


def load_teams(engine: Engine) -> None:
    """Load from VIS GetBeachTeamList (single request, all teams) into raw.raw_fivb_teams.
    Skips teams with null tournament_id (API sometimes omits NoTournament); PK requires (team_id, tournament_id).
    """
    teams_raw = fetch_beach_teams()
    if not teams_raw:
        raise RuntimeError(f"VIS {VIS_GET_BEACH_TEAM_LIST} returned no data")
    rows = [_normalize_team_vis(t) for t in teams_raw]
    # PK (team_id, tournament_id) requires both; skip rows missing tournament_id
    rows = [
        r
        for r in rows
        if r.get("tournament_id") is not None and r.get("team_id") is not None
    ]
    if not rows:
        raise RuntimeError(
            f"VIS {VIS_GET_BEACH_TEAM_LIST} returned no valid teams (all missing tournament_id or team_id)"
        )
    bulk_upsert(
        engine,
        "raw.raw_fivb_teams",
        rows,
        RAW_CONFLICT_COLUMNS["raw.raw_fivb_teams"],
    )
    print(f"  Loaded {len(rows)} teams -> raw.raw_fivb_teams")


def load_matches_for_tournament(
    engine: Engine, no_tournament: int, limit: int | None = None
) -> None:
    """Load from VIS GetBeachMatchList into raw.raw_fivb_matches for one tournament."""
    matches_raw = fetch_beach_matches_for_tournament(no_tournament)
    if limit is not None and limit > 0:
        matches_raw = matches_raw[:limit]
    rows = [_normalize_match_vis(no_tournament, m) for m in matches_raw]
    if not rows and (limit is None or limit > 0):
        raise RuntimeError(
            f"VIS {VIS_GET_BEACH_MATCH_LIST} returned no data for tournament {no_tournament}"
        )
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_matches",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_matches"],
        )


def _normalize_result_vis(no_tournament: int, raw: Dict[str, Any]) -> Dict[str, Any]:
    """VIS GetBeachTournamentRanking: Rank, Position, NoTeam."""
    pos = raw.get("Rank") or raw.get("Position")
    return {
        "tournament_id": no_tournament,
        "team_id": raw.get("NoTeam"),
        "finishing_pos": (
            int(pos) if pos is not None and str(pos).strip() != "" else None
        ),
        "points": None,
        "prize_money": None,
    }


def _normalize_player_vis(raw: Dict[str, Any]) -> Dict[str, Any]:
    """VIS GetPlayer/GetPlayerList: No, FirstName, LastName, Birthdate (or BirthDate), Height, CountryCode, Gender.
    Height is in 0.01mm (e.g. 1930000 = 193 cm)."""
    first = raw.get("FirstName") or ""
    last = raw.get("LastName") or ""
    full = (first + " " + last).strip() or raw.get("FullName")
    birth = raw.get("BirthDate") or raw.get("Birthdate")
    height_raw = raw.get("Height")
    if height_raw is not None:
        try:
            h = int(height_raw)
            height_cm = (
                h // 10000 if h >= 10000 else (h if h < 500 else None)
            )  # 1930000 -> 193; else small int as cm
        except (TypeError, ValueError):
            height_cm = None
    else:
        height_cm = None
    return {
        "player_id": raw.get("No"),
        "first_name": first or None,
        "last_name": last or None,
        "full_name": full or None,
        "gender": str(raw["Gender"]) if raw.get("Gender") is not None else None,
        "birth_date": _date_or_none(birth),
        "height_cm": height_cm,
        "country_code": raw.get("CountryCode"),
        "profile_url": None,
        "payload": raw,
    }


def load_results_for_tournament(
    engine: Engine, no_tournament: int, limit: int | None = None
) -> None:
    """Load from VIS GetBeachTournamentRanking into raw.raw_fivb_results for one tournament (all phases)."""
    for phase in (None, "MainDraw", "Qualification"):
        try:
            ranking_raw = fetch_beach_tournament_ranking(no_tournament, phase=phase)
        except Exception:
            continue
        valid = [
            r
            for r in ranking_raw
            if isinstance(r, dict)
            and "Errors" not in r
            and (r.get("Rank") is not None or r.get("Position") is not None)
        ]
        # When limit is set, take at least 1 so dbt gets some result data
        if limit is not None:
            valid = valid[: max(1, limit)]
        rows = [_normalize_result_vis(no_tournament, r) for r in valid]
        if rows:
            bulk_upsert(
                engine,
                "raw.raw_fivb_results",
                rows,
                RAW_CONFLICT_COLUMNS["raw.raw_fivb_results"],
            )


def load_players(engine: Engine) -> int:
    """Load from VIS GetPlayerList (single request, all players) into raw.raw_fivb_players. Returns number loaded."""
    players_raw = fetch_player_list()
    if not players_raw:
        raise RuntimeError(f"VIS {VIS_GET_PLAYER_LIST} returned no data")
    valid = [
        r
        for r in players_raw
        if isinstance(r, dict) and "Errors" not in r and r.get("No") is not None
    ]
    if not valid:
        raise RuntimeError(
            f"VIS {VIS_GET_PLAYER_LIST} returned no valid players (all errors or missing No)"
        )
    rows = [_normalize_player_vis(r) for r in valid]
    bulk_upsert(
        engine,
        "raw.raw_fivb_players",
        rows,
        RAW_CONFLICT_COLUMNS["raw.raw_fivb_players"],
    )
    return len(rows)


def _load_one_tournament(
    engine: Engine,
    no_int: int,
    limits: IngestionLimits,
) -> Tuple[int, Optional[Exception]]:
    """Load matches and results for one tournament (teams loaded separately). Returns (no_int, error)."""
    try:
        load_matches_for_tournament(engine, no_int, limit=limits.matches_per_tournament)
        load_results_for_tournament(engine, no_int, limit=limits.results_per_tournament)
        return (no_int, None)
    except Exception as e:
        return (no_int, e)


def _progress_iter(iterable: List[Any], desc: str, total: Optional[int] = None):
    """Wrap iterable with tqdm progress bar."""
    return tqdm(iterable, desc=desc, total=total or len(iterable), unit="item")


def _verify_raw_tables_non_empty(engine: Engine) -> None:
    """Raise if core raw tables (tournaments, teams, matches) are empty; warn on empty results or players."""
    import logging

    core = ["raw.raw_fivb_tournaments", "raw.raw_fivb_teams", "raw.raw_fivb_matches"]
    optional = ["raw.raw_fivb_results", "raw.raw_fivb_players"]
    empty_core: List[str] = []
    empty_optional: List[str] = []
    with engine.connect() as conn:
        for table in core:
            (cnt,) = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            if cnt == 0:
                empty_core.append(table)
        for table in optional:
            (cnt,) = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            if cnt == 0:
                empty_optional.append(table)
    if empty_core:
        raise RuntimeError(
            f"Core raw tables are empty (endpoints did not load): {', '.join(empty_core)}"
        )
    if empty_optional:
        logging.warning(
            "Optional raw tables are empty (some endpoints returned no data): %s",
            ", ".join(empty_optional),
        )


def run_full_ingestion(limits: IngestionLimits | None = None) -> None:
    """
    End-to-end ingestion using VIS Volleyball competitions (Beach) endpoints.
    https://www.fivb.org/VisSDK/VisWebService/#Volleyball%20competitions.html
    Ranking from GetBeachTournamentRanking: https://www.fivb.org/VisSDK/VisWebService/#GetBeachTournamentRanking.html

    Order: (1) GetBeachTournamentList, (2) GetBeachTeamList (single request), (3) GetPlayerList (single request),
    (4) per-tournament GetBeachMatchList / GetBeachTournamentRanking in parallel.
    """
    limits = limits or IngestionLimits()
    engine = get_engine()
    log = logging.getLogger(__name__)

    import os

    print("ETL: starting raw ingestion (FIVB VIS Beach)")
    if os.environ.get("TRUNCATE_RAW", "").strip() in ("1", "true", "yes"):
        truncate_raw_tables(engine)
        print("  Truncated raw tables (TRUNCATE_RAW=1)")

    ensure_raw_tables(engine)

    # 1. GetBeachTournamentList -> raw_fivb_tournaments
    print(f"\n1. {VIS_GET_BEACH_TOURNAMENT_LIST}")
    log.info("Loading tournament list (%s)", VIS_GET_BEACH_TOURNAMENT_LIST)
    tournaments = load_tournaments(engine)
    to_process = tournaments
    if limits.tournaments is not None and limits.tournaments > 0:
        tournaments = [t for t in tournaments if t.get("Season") == "2025"]
        to_process = tournaments[: limits.tournaments]

    tournament_ids = []
    for t in to_process:
        no = t.get("No")
        if no is None:
            continue
        try:
            tournament_ids.append(int(no))
        except (TypeError, ValueError):
            continue
    print(f"  Processing {len(tournament_ids)} tournaments for matches/results")

    # 2. GetBeachTeamList (single request, all teams) -> raw_fivb_teams
    print(f"\n2. {VIS_GET_BEACH_TEAM_LIST} (single request)")
    log.info("Loading teams (%s)", VIS_GET_BEACH_TEAM_LIST)
    load_teams(engine)

    # 3. GetPlayerList (single request, all players) -> raw_fivb_players
    print(f"\n3. {VIS_GET_PLAYER_LIST}")
    log.info("Loading players (%s)", VIS_GET_PLAYER_LIST)
    players_loaded = load_players(engine)
    print(f"  Loaded {players_loaded} players -> raw.raw_fivb_players")

    # 4. Per tournament: GetBeachMatchList, GetBeachTournamentRanking (parallel when max_workers > 1)
    failures: List[Tuple[int, Exception]] = []
    workers = max(1, limits.max_workers)
    mode = "parallel" if workers > 1 else "sequential"
    print(f"\n4. Matches + results (per tournament, {mode}, max_workers={workers})")
    log.info(
        "Matches + results: %s (max_workers=%s)",
        mode,
        workers,
    )
    if workers == 1:
        for no_int in _progress_iter(
            tournament_ids, "Tournaments", total=len(tournament_ids)
        ):
            _, err = _load_one_tournament(engine, no_int, limits)
            if err is not None:
                failures.append((no_int, err))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_load_one_tournament, engine, no_int, limits): no_int
                for no_int in tournament_ids
            }
            for fut in _progress_iter(
                as_completed(futures), "Tournaments", total=len(futures)
            ):
                no_int = futures[fut]
                try:
                    _, err = fut.result()
                    if err is not None:
                        failures.append((no_int, err))
                except Exception as e:
                    failures.append((no_int, e))

    if failures:
        for no_int, err in failures:
            log.error("Tournament %s failed: %s", no_int, err)
        print(
            f"  WARNING: {len(failures)} of {len(tournament_ids)} tournaments had failures"
        )
        log.warning(
            "%s of %s tournaments had failures", len(failures), len(tournament_ids)
        )
    else:
        print(f"  Completed matches + results for {len(tournament_ids)} tournaments")

    _verify_raw_tables_non_empty(engine)
    print("\nETL: raw ingestion complete")


if __name__ == "__main__":
    run_full_ingestion(limits=IngestionLimits.from_env())
