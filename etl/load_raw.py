"""
Raw ingestion from the FIVB VIS Web Service (Beach) using all documented endpoints and fields.

Uses etl.vis_client (DEFAULT_FIELDS) so we request every available field; full payload
is stored in each raw table's payload jsonb column.

Endpoints and raw tables:
  1. GetEventList                    -> raw_fivb_events
  2. GetBeachTournamentList           -> raw_fivb_tournaments
  3. GetBeachTeamList                 -> raw_fivb_teams
  4. GetPlayerList                    -> raw_fivb_players
  5. Per tournament: GetBeachMatchList (bulk), GetBeachTournamentRanking -> raw_fivb_matches, raw_fivb_results
  6. Per tournament: GetBeachRoundList -> raw_fivb_rounds
  7. GetBeachRoundRanking (pool round standings) disabled: tournament rankings award points; pool data derivable from matches.
  8. GetBeachWorldTourRanking (M/W)    -> raw_fivb_team_rankings
  9. GetBeachOlympicSelectionRanking (M/W) -> raw_fivb_team_rankings
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial
from typing import Any, Dict, List, Optional, Tuple


def _format_elapsed(seconds: float) -> str:
    """Format seconds as human-readable (e.g. 12.3s, 1m 23s)."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = seconds - 60 * m
    return f"{m}m {s:.1f}s" if s >= 0.05 else f"{m}m"

from sqlalchemy import text
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .db import (
    bulk_upsert,
    ensure_raw_tables,
    ensure_raw_tournament_empty_check_table,
    get_engine,
    RAW_CONFLICT_COLUMNS,
    truncate_raw_tables,
)
from .vis_client import (
    TOURNAMENT_SEASON,
    fetch_beach_matches_all,
    fetch_beach_matches_for_tournament,
    fetch_beach_olympic_selection_ranking,
    fetch_beach_round_list,
    fetch_beach_round_ranking,
    fetch_beach_teams,
    fetch_beach_tournament_ranking,
    fetch_beach_tournaments,
    fetch_beach_world_tour_ranking,
    fetch_event_list,
    fetch_player_list,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionLimits:
    """Optional caps for per-tournament retrieval and parallelism."""

    tournaments: int | None = None
    matches_per_tournament: int | None = None
    results_per_tournament: int | None = None
    max_workers: int = 8
    # Two-bucket skip: recent (start_date >= today - cutoff_days) skip if ingested within recent_hours; older skip if within older_days. 0 = disabled.
    recent_cutoff_days: float = 90.0
    recent_window_hours: float = 24.0
    older_window_days: float = 30.0

    @classmethod
    def from_env(cls) -> "IngestionLimits":
        import os

        def _int(key: str) -> int | None:
            v = os.environ.get(key)
            return int(v) if v not in (None, "") else None

        def _float(key: str, default: float) -> float:
            v = os.environ.get(key)
            if v is None or v == "":
                return default
            try:
                return float(v)
            except ValueError:
                return default

        def _parallel() -> bool:
            v = os.environ.get("ETL_PARALLEL", "").strip().lower()
            if v in ("0", "false", "no", "off"):
                return False
            return True

        workers = _int("ETL_MAX_WORKERS")
        if not _parallel():
            workers = 1
        elif workers is None or workers < 1:
            workers = 8
        return cls(
            tournaments=_int("LIMIT_TOURNAMENTS"),
            matches_per_tournament=_int("LIMIT_MATCHES_PER_TOURNAMENT"),
            results_per_tournament=_int("LIMIT_RESULTS_PER_TOURNAMENT"),
            max_workers=workers,
            recent_cutoff_days=_float("ETL_RECENT_CUTOFF_DAYS", 90.0),
            recent_window_hours=_float("ETL_RECENT_WINDOW_HOURS", 24.0),
            older_window_days=_float("ETL_OLDER_WINDOW_DAYS", 30.0),
        )


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any):
    """Return float for numeric PrizeMoney/points; None for empty or non-numeric."""
    if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_or_none(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    try:
        if isinstance(value, str):
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        pass
    return None


def _bool_vis(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in ("1", "true", "yes", "on")


def _tournament_year(raw: Dict[str, Any]) -> int | None:
    """Infer tournament year from Season or StartDate/EndDate for filtering."""
    season = raw.get("Season")
    if season is not None:
        try:
            y = int(season)
            if 1900 <= y <= 2100:
                return y
        except (TypeError, ValueError):
            pass
    start = _date_or_none(raw.get("StartDate"))
    if start is not None:
        return start.year
    end = _date_or_none(raw.get("EndDate"))
    if end is not None:
        return end.year
    return None


# ---- Events ----
def _normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_id": _int_or_none(raw.get("No")),
        "code": raw.get("Code") or None,
        "name": raw.get("Name") or None,
        "start_date": _date_or_none(raw.get("StartDate")),
        "end_date": _date_or_none(raw.get("EndDate")),
        "type": raw.get("Type") or None,
        "no_parent_event": _int_or_none(raw.get("NoParentEvent")),
        "country_code": raw.get("CountryCode") or None,
        "has_beach_tournament": _bool_vis(raw.get("HasBeachTournament")),
        "has_men_tournament": _bool_vis(raw.get("HasMenTournament")),
        "has_women_tournament": _bool_vis(raw.get("HasWomenTournament")),
        "is_vis_managed": _bool_vis(raw.get("IsVisManaged")),
        "payload": raw,
    }


# ---- Tournaments ----
# GetBeachTournamentList does not return City; Name often is the venue/city (e.g. "Acapulco", "Berlin")
def _normalize_tournament(raw: Dict[str, Any]) -> Dict[str, Any]:
    name = raw.get("Name") or None
    city = raw.get("City") or None
    # When API omits City, use Name as city when it looks like a single place name (one word, no "Tournament"/"Cup"/"Open" etc.)
    if not city and name and isinstance(name, str):
        name_clean = name.strip()
        if name_clean and " " not in name_clean and name_clean.lower() not in (
            "open", "cup", "championship", "masters", "satellite", "challenger", "continental", "olympic", "world",
            "finals", "grand", "slam", "pro", "beach", "ecva", "cazova", "afecavol", "central",
        ):
            city = name_clean
    # VIS BeachTournament has no top-level EndDate; API returns EndDateMainDraw / EndDateQualification
    end_date = _date_or_none(raw.get("EndDate")) or _date_or_none(raw.get("EndDateMainDraw")) or _date_or_none(raw.get("EndDateQualification"))
    return {
        "tournament_id": _int_or_none(raw.get("No")),
        "name": name,
        "season": raw.get("Season") or None,
        "tier": raw.get("Type") or None,
        "start_date": _date_or_none(raw.get("StartDate")),
        "end_date": end_date,
        "city": city,
        "country_code": raw.get("CountryCode") or None,
        "country_name": raw.get("CountryName") or None,
        "gender": str(raw["Gender"]) if raw.get("Gender") is not None else None,
        "status": str(raw["Status"]) if raw.get("Status") is not None else None,
        "timezone": raw.get("Timezone") or None,
        "payload": raw,
    }


# ---- Teams ----
def _normalize_team(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_id": _int_or_none(raw.get("No")),
        "tournament_id": _int_or_none(raw.get("NoTournament")),
        "player_a_id": _int_or_none(raw.get("NoPlayer1")),
        "player_b_id": _int_or_none(raw.get("NoPlayer2")),
        "country_code": raw.get("CountryCode") or None,
        "status": str(raw["Status"]) if raw.get("Status") is not None else None,
        "valid_from": _date_or_none(raw.get("ValidFrom")),
        "valid_to": _date_or_none(raw.get("ValidTo")),
        "payload": raw,
    }


# ---- Matches (full fields: LocalDate, LocalTime, set points, durations, player refs, etc.) ----
def _normalize_match(no_tournament: int, raw: Dict[str, Any]) -> Dict[str, Any]:
    points_a, points_b = raw.get("MatchPointsA"), raw.get("MatchPointsB")
    team_a, team_b = raw.get("NoTeamA"), raw.get("NoTeamB")
    winner = None
    if points_a is not None and points_b is not None and team_a is not None and team_b is not None:
        try:
            if int(points_a) > int(points_b):
                winner = team_a
            elif int(points_b) > int(points_a):
                winner = team_b
        except (TypeError, ValueError):
            pass
    score_sets = f"{points_a}-{points_b}" if points_a is not None and points_b is not None else None
    played_at = raw.get("BeginDateTimeUtc") or raw.get("DateTimeLocal")
    # DurationSet1/2/3 are in seconds; sum and convert to minutes
    dur_sec = 0
    for key in ("DurationSet1", "DurationSet2", "DurationSet3"):
        v = raw.get(key)
        if v is not None and str(v).strip() != "":
            try:
                dur_sec += int(float(v))
            except (TypeError, ValueError):
                pass
    duration_minutes = dur_sec // 60 if dur_sec else None
    return {
        "match_id": _int_or_none(raw.get("No")),
        "tournament_id": _int_or_none(raw.get("NoTournament")) or no_tournament,
        "phase": raw.get("Phase") or None,
        "round": raw.get("NoRound") or raw.get("RoundCode"),
        "team1_id": _int_or_none(team_a),
        "team2_id": _int_or_none(team_b),
        "winner_team_id": _int_or_none(winner),
        "score_sets": score_sets,
        "duration_minutes": duration_minutes,
        "played_at": played_at,
        "result_type": str(raw["ResultType"]) if raw.get("ResultType") is not None else None,
        "status": str(raw["Status"]) if raw.get("Status") is not None else None,
        "payload": raw,
    }


# ---- Tournament results (finishing positions) ----
# GetBeachTournamentRanking returns EarnedPointsTeam and EarningsTotalTeam (not Points/PrizeMoney)
def _normalize_result(no_tournament: int, raw: Dict[str, Any]) -> Dict[str, Any]:
    pos = raw.get("Rank") or raw.get("Position")
    points = raw.get("EarnedPointsTeam") or raw.get("Points")
    prize_money = raw.get("EarningsTotalTeam") or raw.get("PrizeMoney")
    return {
        "tournament_id": no_tournament,
        "team_id": _int_or_none(raw.get("NoTeam")),
        "finishing_pos": int(pos) if pos is not None and str(pos).strip() != "" else None,
        "points": _int_or_none(points),
        "prize_money": _decimal_or_none(prize_money),
        "payload": raw,
    }


# ---- Players ----
def _normalize_player(raw: Dict[str, Any]) -> Dict[str, Any]:
    first = raw.get("FirstName") or ""
    last = raw.get("LastName") or ""
    full = (first + " " + last).strip() or raw.get("FullName")
    birth = raw.get("BirthDate") or raw.get("Birthdate")
    height_raw = raw.get("Height")
    if height_raw is not None:
        try:
            h = int(height_raw)
            height_cm = h // 10000 if h >= 10000 else (h if h < 500 else None)
        except (TypeError, ValueError):
            height_cm = None
    else:
        height_cm = None
    # GetPlayerList returns FederationCode (player's country/federation); CountryCode is not returned by the list endpoint.
    federation_code = raw.get("FederationCode")
    country_code = (str(federation_code).strip() or None) if federation_code else None
    return {
        "player_id": _int_or_none(raw.get("No")),
        "first_name": first or None,
        "last_name": last or None,
        "full_name": full or None,
        "gender": str(raw["Gender"]) if raw.get("Gender") is not None else None,
        "birth_date": _date_or_none(birth),
        "height_cm": height_cm,
        "country_code": country_code,
        "profile_url": None,
        "payload": raw,
    }


# ---- Rounds ----
def _normalize_round(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "round_id": _int_or_none(raw.get("No")),
        "tournament_id": _int_or_none(raw.get("NoTournament")),
        "code": raw.get("Code") or None,
        "name": raw.get("Name") or None,
        "bracket": raw.get("Bracket") or None,
        "phase": raw.get("Phase") or None,
        "start_date": _date_or_none(raw.get("StartDate")),
        "end_date": _date_or_none(raw.get("EndDate")),
        "rank_method": raw.get("RankMethod") or None,
        "payload": raw,
    }


# ---- Round rankings (pool standings) ----
def _normalize_round_ranking(no_round: int, raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "round_id": no_round,
        "position": _int_or_none(raw.get("Position")),
        "rank": _int_or_none(raw.get("Rank")),
        "team_federation_code": raw.get("TeamFederationCode") or None,
        "team_name": raw.get("TeamName") or None,
        "match_points": _int_or_none(raw.get("MatchPoints")),
        "matches_won": _int_or_none(raw.get("MatchesWon")),
        "matches_lost": _int_or_none(raw.get("MatchesLost")),
        "payload": raw,
    }


# ---- Team rankings (World Tour, Olympic) ----
def _normalize_team_ranking(
    ranking_type: str, snapshot_date: date, gender: str, raw: Dict[str, Any]
) -> Dict[str, Any]:
    # World Tour: EarnedPointsTeam; Olympic: Points
    earned = raw.get("EarnedPointsTeam") or raw.get("Points")
    return {
        "ranking_type": ranking_type,
        "snapshot_date": snapshot_date,
        "gender": gender,
        "position": _int_or_none(raw.get("Position")),
        "no_player1": _int_or_none(raw.get("NoPlayer1")),
        "no_player2": _int_or_none(raw.get("NoPlayer2")),
        "team_name": raw.get("TeamName") or None,
        "earned_points": _int_or_none(earned),
        "payload": raw,
    }


# ---- Loaders ----
def load_events(engine: Engine) -> int:
    """GetEventList -> raw_fivb_events (uses DEFAULT_FIELDS)."""
    with tqdm(total=1, desc="GetEventList", unit="call") as pbar:
        data = fetch_event_list(
            has_beach_tournament=True,
            no_parent_event=0,
            start_date="2024-01-01",
            end_date="2026-12-31",
        )
        pbar.update(1)
    rows = [_normalize_event(r) for r in data if isinstance(r, dict) and r.get("No") is not None]
    rows = [r for r in rows if r.get("event_id") is not None]
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_events",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_events"],
        )
    print(f"  Loaded {len(rows)} events -> raw.raw_fivb_events")
    return len(rows)


def load_tournaments(engine: Engine) -> List[Dict[str, Any]]:
    """GetBeachTournamentList -> raw_fivb_tournaments. Returns raw list for downstream."""
    with tqdm(total=1, desc="GetBeachTournamentList", unit="call") as pbar:
        data = fetch_beach_tournaments(filter_expr=f"Season='{TOURNAMENT_SEASON}'")
        pbar.update(1)
    if not data:
        raise RuntimeError("GetBeachTournamentList returned no data")
    rows = [_normalize_tournament(t) for t in data]
    rows = [r for r in rows if r.get("tournament_id") is not None]
    if not rows:
        raise RuntimeError("No valid tournaments after normalize")
    bulk_upsert(
        engine,
        "raw.raw_fivb_tournaments",
        rows,
        RAW_CONFLICT_COLUMNS["raw.raw_fivb_tournaments"],
    )
    print(f"  Loaded {len(rows)} tournaments -> raw.raw_fivb_tournaments")
    return data


def load_teams(engine: Engine) -> int:
    """GetBeachTeamList (all teams) -> raw_fivb_teams."""
    with tqdm(total=1, desc="GetBeachTeamList", unit="call") as pbar:
        data = fetch_beach_teams()
        pbar.update(1)
    if not data:
        raise RuntimeError("GetBeachTeamList returned no data")
    rows = [_normalize_team(t) for t in data]
    rows = [r for r in rows if r.get("team_id") is not None and r.get("tournament_id") is not None]
    if not rows:
        raise RuntimeError("No valid teams (missing team_id or tournament_id)")
    bulk_upsert(
        engine,
        "raw.raw_fivb_teams",
        rows,
        RAW_CONFLICT_COLUMNS["raw.raw_fivb_teams"],
    )
    print(f"  Loaded {len(rows)} teams -> raw.raw_fivb_teams")
    return len(rows)


def load_players(engine: Engine) -> int:
    """GetPlayerList -> raw_fivb_players."""
    with tqdm(total=1, desc="GetPlayerList", unit="call") as pbar:
        data = fetch_player_list()
        pbar.update(1)
    if not data:
        raise RuntimeError("GetPlayerList returned no data")
    valid = [r for r in data if isinstance(r, dict) and "Errors" not in r and r.get("No") is not None]
    rows = [_normalize_player(r) for r in valid]
    if not rows:
        raise RuntimeError("No valid players after normalize")
    bulk_upsert(
        engine,
        "raw.raw_fivb_players",
        rows,
        RAW_CONFLICT_COLUMNS["raw.raw_fivb_players"],
    )
    print(f"  Loaded {len(rows)} players -> raw.raw_fivb_players")
    return len(rows)


def load_matches_for_tournament(
    engine: Engine, no_tournament: int, limit: int | None = None
) -> None:
    """GetBeachMatchList for one tournament -> raw_fivb_matches."""
    data = fetch_beach_matches_for_tournament(no_tournament)
    if limit is not None and limit > 0:
        data = data[:limit]
    rows = [_normalize_match(no_tournament, m) for m in data]
    rows = [r for r in rows if r.get("match_id") is not None]
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_matches",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_matches"],
        )


def load_all_matches_bulk(engine: Engine) -> int:
    """Get all beach matches in one API call (GetBeachMatchList, no filter) -> raw_fivb_matches.
    Much faster than per-tournament match fetches. Returns count loaded."""
    with tqdm(total=1, desc="GetBeachMatchList (all)", unit="call") as pbar:
        data = fetch_beach_matches_all()
        pbar.update(1)
    if not data:
        return 0
    rows = []
    for m in data:
        no_tournament = _int_or_none(m.get("NoTournament"))
        if no_tournament is None:
            continue
        row = _normalize_match(no_tournament, m)
        if row.get("match_id") is not None:
            rows.append(row)
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_matches",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_matches"],
        )
    print(f"  Loaded {len(rows)} matches -> raw.raw_fivb_matches (1 bulk call)")
    return len(rows)


def _record_results_empty(engine: Engine, tournament_id: int) -> None:
    """Record that we checked this tournament for results and got none (so we can skip it for a while)."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.raw_fivb_tournament_empty_check (tournament_id, results_empty_at)
                VALUES (:tid, now())
                ON CONFLICT (tournament_id) DO UPDATE SET results_empty_at = now()
            """),
            {"tid": tournament_id},
        )


def _record_rounds_empty(engine: Engine, tournament_id: int) -> None:
    """Record that we checked this tournament for rounds and got none."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.raw_fivb_tournament_empty_check (tournament_id, rounds_empty_at)
                VALUES (:tid, now())
                ON CONFLICT (tournament_id) DO UPDATE SET rounds_empty_at = now()
            """),
            {"tid": tournament_id},
        )


def _clear_results_empty(engine: Engine, tournament_id: int) -> None:
    """Clear the 'results empty' sentinel after we wrote results."""
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE raw.raw_fivb_tournament_empty_check SET results_empty_at = NULL WHERE tournament_id = :tid"),
            {"tid": tournament_id},
        )


def _clear_rounds_empty(engine: Engine, tournament_id: int) -> None:
    """Clear the 'rounds empty' sentinel after we wrote rounds."""
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE raw.raw_fivb_tournament_empty_check SET rounds_empty_at = NULL WHERE tournament_id = :tid"),
            {"tid": tournament_id},
        )


def _fetch_and_upsert_results_phase(
    engine: Engine,
    no_tournament: int,
    phase: Optional[str],
    limit: int | None,
) -> bool:
    """Single phase of GetBeachTournamentRanking: fetch + normalize + upsert. Returns True if we wrote any rows."""
    try:
        data = fetch_beach_tournament_ranking(no_tournament, phase=phase)
    except Exception:
        return False
    valid = [
        r for r in data
        if isinstance(r, dict) and "Errors" not in r and (r.get("Rank") is not None or r.get("Position") is not None)
    ]
    if limit is not None:
        valid = valid[: max(1, limit)]
    rows = [_normalize_result(no_tournament, r) for r in valid]
    rows = [r for r in rows if r.get("team_id") is not None]
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_results",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_results"],
        )
        return True
    return False


def load_results_for_tournament(
    engine: Engine, no_tournament: int, limit: int | None = None
) -> None:
    """GetBeachTournamentRanking (Qualification + MainDraw) -> raw_fivb_results."""
    any_wrote = False
    for phase in (None, "MainDraw", "Qualification"):
        any_wrote |= _fetch_and_upsert_results_phase(engine, no_tournament, phase, limit)
    if any_wrote:
        _clear_results_empty(engine, no_tournament)
    else:
        _record_results_empty(engine, no_tournament)


def load_rounds_for_tournament(engine: Engine, no_tournament: int) -> List[Dict[str, Any]]:
    """GetBeachRoundList -> raw_fivb_rounds. Returns rounds for optional round-ranking load."""
    data = fetch_beach_round_list(no_tournament)
    rows = [_normalize_round(r) for r in data if isinstance(r, dict) and r.get("No") is not None]
    rows = [r for r in rows if r.get("round_id") is not None]
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_rounds",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_rounds"],
        )
        _clear_rounds_empty(engine, no_tournament)
    else:
        _record_rounds_empty(engine, no_tournament)
    return data


def load_round_ranking_for_round(engine: Engine, no_round: int) -> None:
    """GetBeachRoundRanking for one round (pool rounds only)."""
    try:
        data = fetch_beach_round_ranking(no_round)
    except Exception:
        return
    rows = [
        _normalize_round_ranking(no_round, r)
        for r in data
        if isinstance(r, dict) and r.get("Position") is not None
    ]
    rows = [r for r in rows if r.get("position") is not None]
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_round_rankings",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_round_rankings"],
        )


def _load_one_team_ranking(
    engine: Engine,
    snapshot_date: date,
    ranking_type: str,
    gender: str,
    fetcher: Any,
) -> None:
    """Fetch one ranking (e.g. beach_world_tour/M) and upsert into raw_fivb_team_rankings."""
    try:
        data = fetcher(gender=gender)
    except Exception as e:
        logger.warning("%s %s failed: %s", ranking_type, gender, e)
        return
    rows = [
        _normalize_team_ranking(ranking_type, snapshot_date, gender, r)
        for r in data
        if isinstance(r, dict) and r.get("Position") is not None
    ]
    rows = [r for r in rows if r.get("position") is not None]
    if rows:
        bulk_upsert(
            engine,
            "raw.raw_fivb_team_rankings",
            rows,
            RAW_CONFLICT_COLUMNS["raw.raw_fivb_team_rankings"],
        )


def load_team_rankings(
    engine: Engine, snapshot_date: date, parallel: bool = True
) -> None:
    """GetBeachWorldTourRanking and GetBeachOlympicSelectionRanking (M/W) -> raw_fivb_team_rankings."""
    tasks = []
    for gender in ("M", "W"):
        for ranking_type, fetcher in [
            ("beach_world_tour", fetch_beach_world_tour_ranking),
            ("beach_olympic", fetch_beach_olympic_selection_ranking),
        ]:
            tasks.append((ranking_type, gender, fetcher))
    if parallel and len(tasks) > 1:
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            list(
                executor.map(
                    lambda t: _load_one_team_ranking(engine, snapshot_date, t[0], t[1], t[2]),
                    tasks,
                )
            )
    else:
        for ranking_type, gender, fetcher in tqdm(
            tasks, desc="Rankings (World Tour + Olympic M/W)", unit="fetch"
        ):
            _load_one_team_ranking(engine, snapshot_date, ranking_type, gender, fetcher)
    print("  Loaded World Tour + Olympic rankings (M/W) -> raw.raw_fivb_team_rankings")


def _load_one_tournament(
    engine: Engine,
    no_int: int,
    limits: IngestionLimits,
) -> Tuple[int, Optional[Exception], Dict[str, float]]:
    """Load results and rounds for one tournament (matches loaded in bulk in step 5a).
    Runs the 4 API calls (3 ranking phases + rounds) in parallel within this task.
    Returns (tournament_id, error_or_none, timings_sec) with keys: results, rounds."""
    timings: Dict[str, float] = {"results": 0.0, "rounds": 0.0}
    try:
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    _fetch_and_upsert_results_phase,
                    engine,
                    no_int,
                    phase,
                    limits.results_per_tournament,
                )
                for phase in (None, "MainDraw", "Qualification")
            ]
            futures.append(executor.submit(load_rounds_for_tournament, engine, no_int))
            for fut in futures:
                fut.result()
        elapsed = time.perf_counter() - t0
        timings["results"] = elapsed * 0.5  # approximate split for reporting
        timings["rounds"] = elapsed * 0.5
        return (no_int, None, timings)
    except Exception as e:
        return (no_int, e, timings)


def _tournament_ids_to_skip(engine: Engine, limits: IngestionLimits) -> set[int]:
    """Tournament IDs to skip: (1) recent/older already ingested within window; (2) recently confirmed empty (no results and no rounds)."""
    skip: set[int] = set()
    with engine.connect() as conn:
        if limits.recent_window_hours > 0 or limits.older_window_days > 0:
            rows = conn.execute(
                text("""
                    WITH last_ingested AS (
                        SELECT tournament_id, max(ingested_at) AS last_ingested FROM (
                            SELECT tournament_id, ingested_at FROM raw.raw_fivb_results
                            UNION ALL
                            SELECT tournament_id, ingested_at FROM raw.raw_fivb_rounds
                        ) x GROUP BY tournament_id
                    )
                    SELECT i.tournament_id
                    FROM last_ingested i
                    JOIN raw.raw_fivb_tournaments t ON t.tournament_id = i.tournament_id
                    WHERE
                      (t.start_date >= current_date - :cutoff_days * interval '1 day'
                       AND i.last_ingested >= now() - :recent_hours * interval '1 hour')
                      OR
                      (t.start_date IS NULL OR t.start_date < current_date - :cutoff_days * interval '1 day')
                      AND i.last_ingested >= now() - :older_days * interval '1 day'
                """),
                {
                    "cutoff_days": limits.recent_cutoff_days,
                    "recent_hours": limits.recent_window_hours,
                    "older_days": limits.older_window_days,
                },
            ).fetchall()
            skip = {r[0] for r in rows}
        # Also skip tournaments we recently confirmed have no results and no rounds (VIS returns empty)
        if limits.older_window_days > 0:
            empty_rows = conn.execute(
                text("""
                    SELECT tournament_id
                    FROM raw.raw_fivb_tournament_empty_check
                    WHERE results_empty_at IS NOT NULL
                      AND rounds_empty_at IS NOT NULL
                      AND results_empty_at >= now() - :older_days * interval '1 day'
                      AND rounds_empty_at >= now() - :older_days * interval '1 day'
                """),
                {"older_days": limits.older_window_days},
            ).fetchall()
            skip |= {r[0] for r in empty_rows}
    return skip


def _verify_core_tables(engine: Engine) -> None:
    core = ["raw.raw_fivb_tournaments", "raw.raw_fivb_teams", "raw.raw_fivb_matches"]
    with engine.connect() as conn:
        for table in core:
            (cnt,) = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            if cnt == 0:
                raise RuntimeError(f"Core raw table empty: {table}")


def run_full_ingestion(limits: IngestionLimits | None = None) -> None:
    """
    Full ingestion from all VIS beach endpoints using DEFAULT_FIELDS.
    Order: events, tournaments, teams, players, then per-tournament (matches, results, rounds, round rankings),
    then World Tour and Olympic team rankings.
    """
    import os

    limits = limits or IngestionLimits.from_env()
    engine = get_engine()

    print("ETL: raw ingestion (FIVB VIS Beach – all endpoints, full fields)")
    if os.environ.get("TRUNCATE_RAW", "").strip().lower() in ("1", "true", "yes"):
        truncate_raw_tables(engine)
        print("  Truncated raw tables (TRUNCATE_RAW=1)")

    ensure_raw_tables(engine)
    ensure_raw_tournament_empty_check_table(engine)

    timings: List[Tuple[str, float]] = []

    # 1–4. Events, Tournaments, Teams, Players (parallel – independent API calls)
    print("\n1–4. GetEventList + GetBeachTournamentList + GetBeachTeamList + GetPlayerList (parallel)")
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        fut_events = executor.submit(load_events, engine)
        fut_tournaments = executor.submit(load_tournaments, engine)
        fut_teams = executor.submit(load_teams, engine)
        fut_players = executor.submit(load_players, engine)
        fut_events.result()
        tournaments = fut_tournaments.result()
        fut_teams.result()
        fut_players.result()
    elapsed = time.perf_counter() - t0
    timings.append(("Events + Tournaments + Teams + Players (parallel)", elapsed))
    print(f"  → {_format_elapsed(elapsed)}")
    to_process = tournaments
    if limits.tournaments is not None and limits.tournaments > 0:
        to_process = tournaments[: limits.tournaments]
    # Only expand (results/rounds) for tournaments from 2015 onwards
    MIN_EXPAND_YEAR = 2015
    tournament_ids = []
    today = date.today()
    for t in to_process:
        no = _int_or_none(t.get("No"))
        if no is None:
            continue
        year = _tournament_year(t)
        if year is not None and year < MIN_EXPAND_YEAR:
            continue
        # Skip future tournaments: no results exist before start
        start = _date_or_none(t.get("StartDate"))
        if start is not None and start > today:
            continue
        tournament_ids.append(no)
    # Skip by two-bucket rule: recent (end_date >= today - 90d) if ingested in 24h; older if in 30d
    skip_ids = _tournament_ids_to_skip(engine, limits)
    if skip_ids:
        tournament_ids = [tid for tid in tournament_ids if tid not in skip_ids]
        print(f"  Skipping {len(skip_ids)} (recent: 24h, older: 30d); will load {len(tournament_ids)}")
    else:
        print(f"  Will process {len(tournament_ids)} tournaments for matches/results/rounds (from {MIN_EXPAND_YEAR} onwards)")

    # 5–6. One queue: bulk matches + per-tournament (results/rounds) + 4 rankings — all tasks share a worker pool
    ranking_tasks: List[Tuple[str, str, Any]] = []
    for gender in ("M", "W"):
        for ranking_type, fetcher in [
            ("beach_world_tour", fetch_beach_world_tour_ranking),
            ("beach_olympic", fetch_beach_olympic_selection_ranking),
        ]:
            ranking_tasks.append((ranking_type, gender, fetcher))

    workers = max(1, limits.max_workers)
    num_tasks = 1 + len(tournament_ids) + len(ranking_tasks)
    print(f"\n5–6. Matches + per-tournament + rankings (queue, {num_tasks} tasks, workers={workers})")
    t0 = time.perf_counter()
    failures: List[Tuple[int, Exception]] = []
    step_totals: Dict[str, float] = {"results": 0.0, "rounds": 0.0}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        futures.append(executor.submit(load_all_matches_bulk, engine))
        for rt, g, fet in ranking_tasks:
            futures.append(
                executor.submit(
                    partial(_load_one_team_ranking, engine, date.today(), rt, g, fet)
                )
            )
        for no_int in tournament_ids:
            futures.append(executor.submit(_load_one_tournament, engine, no_int, limits))
        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Matches + tournaments + rankings",
            unit="task",
        ):
            try:
                result = fut.result()
                if isinstance(result, tuple) and len(result) == 3:
                    _no_int, err, step_sec = result
                    if err is not None:
                        failures.append((_no_int, err))
                    else:
                        for k in step_totals:
                            step_totals[k] += step_sec.get(k, 0.0)
            except Exception as e:
                logger.exception("Task failed: %s", e)
    elapsed = time.perf_counter() - t0
    timings.append(("Matches + per-tournament + rankings (queue)", elapsed))
    print(f"  → {_format_elapsed(elapsed)}")
    n_ok = len(tournament_ids) - len(failures)
    if n_ok > 0:
        print(
            f"  Breakdown (sum across {n_ok} tournaments): GetBeachTournamentRanking {_format_elapsed(step_totals['results'])}, GetBeachRoundList {_format_elapsed(step_totals['rounds'])}"
        )
    if failures:
        for no_int, err in failures:
            logger.error("Tournament %s failed: %s", no_int, err)
        print(f"  WARNING: {len(failures)} of {len(tournament_ids)} tournaments had failures")
    else:
        print(f"  Completed {len(tournament_ids)} tournaments + bulk matches + 4 rankings")

    _verify_core_tables(engine)

    # Summary
    total = sum(t for _, t in timings)
    print("\n--- Timings ---")
    for name, t in timings:
        pct = (t / total * 100) if total > 0 else 0
        print(f"  {name}: {_format_elapsed(t)} ({pct:.0f}%)")
    print(f"  Total: {_format_elapsed(total)}")
    print("\nETL: raw ingestion complete")


if __name__ == "__main__":
    run_full_ingestion(limits=IngestionLimits.from_env())
