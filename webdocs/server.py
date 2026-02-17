"""
FIVB VIS API – Swagger webdocs and JSON proxy for test requests.

Serves Swagger UI at /docs and proxies requests to the VIS Web Service so you can
explore endpoints from the browser (avoids CORS). Run from project root:

  python3 -m webdocs.server
  # or: python3 -m uvicorn webdocs.server:app --reload --port 8000

Then open http://127.0.0.1:8000/docs
"""

from __future__ import annotations

import sys
from pathlib import Path

# Run from project root so etl is importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from etl.vis_client import (
    TOURNAMENT_SEASON,
    fetch_beach_matches_for_tournament,
    fetch_beach_olympic_selection_ranking,
    fetch_beach_round,
    fetch_beach_round_list,
    fetch_beach_round_ranking,
    fetch_beach_team,
    fetch_beach_teams,
    fetch_beach_teams_for_tournament,
    fetch_beach_tournament,
    fetch_beach_tournament_ranking,
    fetch_beach_tournaments,
    fetch_beach_world_tour_ranking,
    fetch_event,
    fetch_event_list,
    fetch_player,
    fetch_player_list,
    vis_request,
)

# ---------------------------------------------------------------------------
# Request body models (optional params for Try it out)
# ---------------------------------------------------------------------------


class GetBeachTournamentListRequest(BaseModel):
    """List beach tournaments. Filter by season (e.g. '2025 2026')."""

    Filter: Optional[str] = Field(
        default=f"Season='{TOURNAMENT_SEASON}'",
        description="VIS filter expression, e.g. Season='2025 2026'",
    )
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachTournamentRequest(BaseModel):
    """Single beach tournament by number."""

    No: int = Field(..., description="Tournament No (e.g. 502)", example=502)


class GetBeachTeamListRequest(BaseModel):
    """List beach teams; optionally filter by tournament."""

    NoTournament: Optional[int] = Field(502, description="Filter by tournament No")
    Filter: Optional[str] = Field(
        None,
        description="Override: full VIS filter e.g. NoTournament='502'",
    )
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachMatchListRequest(BaseModel):
    """List beach matches for a tournament."""

    NoTournament: int = Field(..., description="Tournament No", example=502)
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachMatchRequest(BaseModel):
    """Single beach match by number."""

    No: int = Field(..., description="Match No", example=106968)


class GetBeachTournamentRankingRequest(BaseModel):
    """Tournament finishing positions (Phase: Qualification or MainDraw)."""

    No: int = Field(..., description="Tournament No", example=502)
    Phase: Optional[str] = Field("MainDraw", description="Qualification | MainDraw")
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachRoundListRequest(BaseModel):
    """List rounds for a tournament."""

    NoTournament: int = Field(..., description="Tournament No", example=502)
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetPlayerListRequest(BaseModel):
    """List players. Omit Filter for full list."""

    Filter: Optional[str] = Field(None, description="Optional VIS filter")
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetPlayerRequest(BaseModel):
    """Single player by number."""

    No: int = Field(..., description="Player No", example=1)


class GetBeachWorldTourRankingRequest(BaseModel):
    """Beach World Tour ranking."""

    Gender: str = Field("W", description="M or W")
    Number: Optional[int] = Field(10, description="Top N entries")
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachOlympicSelectionRankingRequest(BaseModel):
    """Beach Olympic selection ranking."""

    Gender: str = Field("W", description="M or W")
    GamesYear: Optional[int] = Field(None, description="Optional year filter")
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachRoundRequest(BaseModel):
    """Single beach round by number."""

    No: int = Field(..., description="Round No", example=335652)


class GetBeachRoundRankingRequest(BaseModel):
    """Pool/round standings (only for rounds with ranking, e.g. pools)."""

    No: int = Field(..., description="Round No (use a pool round)", example=335652)
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetBeachTeamRequest(BaseModel):
    """Single beach team by number."""

    No: int = Field(..., description="Team No", example=375442)


class GetEventListRequest(BaseModel):
    """List events (e.g. World Tour). Filter by beach, parent, dates."""

    HasBeachTournament: bool = True
    NoParentEvent: int = Field(0, description="0 = top-level events")
    StartDate: Optional[str] = Field("2024-01-01", description="YYYY-MM-DD")
    EndDate: Optional[str] = Field("2026-12-31", description="YYYY-MM-DD")
    limit: Optional[int] = Field(10, description="Max records to return (default 10; 0 = no limit)")


class GetEventRequest(BaseModel):
    """Single event by number."""

    No: int = Field(..., description="Event No", example=1)


# ---------------------------------------------------------------------------
# FastAPI app and routes
# ---------------------------------------------------------------------------

app = FastAPI(
    title="FIVB VIS Beach API",
    description="JSON proxy for the [FIVB VIS Web Service](https://www.fivb.org/VisSDK/VisWebService/) "
    "beach volleyball endpoints. Use **Try it out** to send test requests; the server forwards them to the VIS API.",
    version="1.0.0",
)


# Default max records to return for list endpoints (keeps Swagger UI responsive)
DEFAULT_LIST_LIMIT = 10


def _list_response(data: List[Any], limit: Optional[int] = None) -> dict:
    total = len(data)
    out: dict = {"count": total, "data": data}
    if limit is not None and limit > 0 and total > limit:
        n_first = limit // 2
        n_last = limit - n_first
        data = list(data[:n_first]) + list(data[-n_last:])
        out["data"] = data
        out["_truncated"] = "first_and_last"
        out["_first_count"] = n_first
        out["_last_count"] = n_last
    return out


@app.get("/", include_in_schema=False)
def root():
    """Redirect to Swagger UI."""
    return RedirectResponse(url="/docs", status_code=302)


@app.post("/api/GetBeachTournamentList", response_model=dict)
def api_get_beach_tournament_list(body: GetBeachTournamentListRequest):
    """List beach tournaments (optionally filtered by season)."""
    kwargs = {} if body.Filter is None else {"filter_expr": body.Filter}
    data = fetch_beach_tournaments(**kwargs)
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachTournament", response_model=dict)
def api_get_beach_tournament(body: GetBeachTournamentRequest):
    """Single beach tournament by No."""
    data = fetch_beach_tournament(no=body.No)
    return _list_response(data)


@app.post("/api/GetBeachTeamList", response_model=dict)
def api_get_beach_team_list(body: GetBeachTeamListRequest):
    """List beach teams (all or filtered by NoTournament)."""
    if body.Filter:
        data = fetch_beach_teams(filter_expr=body.Filter)
    else:
        data = fetch_beach_teams_for_tournament(no_tournament=body.NoTournament or 502)
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachMatchList", response_model=dict)
def api_get_beach_match_list(body: GetBeachMatchListRequest):
    """List beach matches for a tournament."""
    data = fetch_beach_matches_for_tournament(no_tournament=body.NoTournament)
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachMatch", response_model=dict)
def api_get_beach_match(body: GetBeachMatchRequest):
    """Single beach match by No."""
    data = vis_request(
        "GetBeachMatch",
        "//BeachMatch",
        No=body.No,
        accept_json=True,
    )
    return _list_response(data)


@app.post("/api/GetBeachTournamentRanking", response_model=dict)
def api_get_beach_tournament_ranking(body: GetBeachTournamentRankingRequest):
    """Tournament finishing positions (Phase: Qualification or MainDraw)."""
    data = fetch_beach_tournament_ranking(
        no_tournament=body.No,
        phase=body.Phase,
    )
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachRoundList", response_model=dict)
def api_get_beach_round_list(body: GetBeachRoundListRequest):
    """List rounds for a tournament."""
    data = fetch_beach_round_list(no_tournament=body.NoTournament)
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetPlayerList", response_model=dict)
def api_get_player_list(body: GetPlayerListRequest):
    """List players. Omit Filter for full list."""
    kwargs = {} if body.Filter is None else {"filter_expr": body.Filter}
    data = fetch_player_list(**kwargs)
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetPlayer", response_model=dict)
def api_get_player(body: GetPlayerRequest):
    """Single player by No."""
    data = fetch_player(no=body.No)
    return _list_response(data)


@app.post("/api/GetBeachWorldTourRanking", response_model=dict)
def api_get_beach_world_tour_ranking(body: GetBeachWorldTourRankingRequest):
    """Beach World Tour ranking."""
    data = fetch_beach_world_tour_ranking(
        gender=body.Gender,
        number=body.Number,
    )
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachOlympicSelectionRanking", response_model=dict)
def api_get_beach_olympic_selection_ranking(body: GetBeachOlympicSelectionRankingRequest):
    """Beach Olympic selection ranking."""
    data = fetch_beach_olympic_selection_ranking(
        gender=body.Gender,
        games_year=body.GamesYear,
    )
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachRound", response_model=dict)
def api_get_beach_round(body: GetBeachRoundRequest):
    """Single beach round by No."""
    data = fetch_beach_round(no=body.No)
    return _list_response(data)


@app.post("/api/GetBeachRoundRanking", response_model=dict)
def api_get_beach_round_ranking(body: GetBeachRoundRankingRequest):
    """Pool/round standings (only for rounds with ranking, e.g. pools)."""
    try:
        data = fetch_beach_round_ranking(no_round=body.No)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"VIS error (e.g. NotARankingRound): {e}")
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetBeachTeam", response_model=dict)
def api_get_beach_team(body: GetBeachTeamRequest):
    """Single beach team by No."""
    data = fetch_beach_team(no=body.No)
    return _list_response(data)


@app.post("/api/GetEventList", response_model=dict)
def api_get_event_list(body: GetEventListRequest):
    """List events (e.g. World Tour). Filter by beach, parent, dates."""
    data = fetch_event_list(
        has_beach_tournament=body.HasBeachTournament,
        no_parent_event=body.NoParentEvent,
        start_date=body.StartDate,
        end_date=body.EndDate,
    )
    cap = None if body.limit == 0 else (body.limit or DEFAULT_LIST_LIMIT)
    return _list_response(data, limit=cap)


@app.post("/api/GetEvent", response_model=dict)
def api_get_event(body: GetEventRequest):
    """Single event by No."""
    data = fetch_event(no=body.No)
    return _list_response(data)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
