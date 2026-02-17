#!/usr/bin/env python3
"""
Explore FIVB VIS Web Service endpoints and save request/response samples for data contract documentation.

Runs test requests for beach volleyball (and related) endpoints, then writes each result to
api_examples/ as JSON so we can understand request shape and response schema.

Usage (from project root, with venv activated):
  python scripts/explore_vis_api.py

Output: api_examples/*.json (one per endpoint) and api_examples/manifest.json.
Docs: https://www.fivb.org/VisSDK/VisWebService/
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

# Run from project root so etl is importable
if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from etl.vis_client import (
    TOURNAMENT_SEASON,
    fetch_beach_round_list,
    vis_request_raw,
)

# Max length of response_text to store in JSON (rest truncated for readability)
MAX_RESPONSE_TEXT_IN_JSON = 50_000

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "api_examples"


def _truncate(s: str, max_len: int = MAX_RESPONSE_TEXT_IN_JSON) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"\n... [truncated, total {len(s)} chars]"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Get one tournament and one match ID from list endpoints for single-entity and filter tests
    print("Fetching tournament list for IDs...")
    list_result = vis_request_raw(
        "GetBeachTournamentList",
        "//BeachTournament",
        Filter=f"Season='{TOURNAMENT_SEASON}'",
    )
    sample_tournaments = list_result.get("parsed_sample") or []
    no_tournament = int(sample_tournaments[0]["No"]) if sample_tournaments else 502
    if sample_tournaments:
        list_result["response_text"] = _truncate(list_result.get("response_text", ""))

    # Endpoint definitions: (request_type, node_path, description, kwargs, accept_json, old_style)
    endpoints: list[dict[str, Any]] = [
        # --- Beach tournaments ---
        {
            "id": "GetBeachTournamentList",
            "request_type": "GetBeachTournamentList",
            "node_path": "//BeachTournament",
            "description": "List beach tournaments (optionally filtered by season)",
            "kwargs": {"Filter": f"Season='{TOURNAMENT_SEASON}'"},
            "accept_json": True,
            "old_style": False,
        },
        {
            "id": "GetBeachTournament",
            "request_type": "GetBeachTournament",
            "node_path": "//BeachTournament",
            "description": "Single beach tournament by No",
            "kwargs": {"No": no_tournament},
            "accept_json": True,
            "old_style": False,
        },
        # --- Beach teams ---
        {
            "id": "GetBeachTeamList",
            "request_type": "GetBeachTeamList",
            "node_path": "//BeachTeam",
            "description": "List beach teams (all or filtered by NoTournament)",
            "kwargs": {"Filter": f"NoTournament='{no_tournament}'"},
            "accept_json": True,
            "old_style": False,
        },
        # --- Beach matches ---
        {
            "id": "GetBeachMatchList",
            "request_type": "GetBeachMatchList",
            "node_path": "//BeachMatch",
            "description": "List beach matches for a tournament",
            "kwargs": {"Filter": f"NoTournament='{no_tournament}'"},
            "accept_json": True,
            "old_style": False,
        },
        # --- Beach match (single) - need a match No from list ---
        {
            "id": "GetBeachMatch",
            "request_type": "GetBeachMatch",
            "node_path": "//BeachMatch",
            "description": "Single beach match by No",
            "kwargs": {},  # No filled after we get match list
            "accept_json": True,
            "old_style": False,
        },
        # --- Beach tournament ranking (results) ---
        {
            "id": "GetBeachTournamentRanking",
            "request_type": "GetBeachTournamentRanking",
            "node_path": "//BeachTournamentRankingEntry",
            "description": "Tournament finishing positions (Phase: Qualification | MainDraw)",
            "kwargs": {"No": no_tournament, "Phase": "MainDraw"},
            "accept_json": False,
            "old_style": True,
        },
        # --- Beach rounds (Filter as child element + old-style wrapper per VIS docs) ---
        {
            "id": "GetBeachRoundList",
            "request_type": "GetBeachRoundList",
            "node_path": "//BeachRound",
            "description": "List rounds for a tournament",
            "kwargs": {},  # children set below from no_tournament
            "accept_json": False,
            "old_style": False,
        },
        # --- Players ---
        {
            "id": "GetPlayerList",
            "request_type": "GetPlayerList",
            "node_path": "//Player",
            "description": "List players (optionally filtered)",
            "kwargs": {},
            "accept_json": True,
            "old_style": False,
        },
        {
            "id": "GetPlayer",
            "request_type": "GetPlayer",
            "node_path": "//Player",
            "description": "Single player by No (old-style wrapper required)",
            "kwargs": {},  # No filled from GetPlayerList sample
            "accept_json": False,
            "old_style": True,
        },
        # --- Rankings (old-style wrapper required per VIS docs) ---
        {
            "id": "GetBeachWorldTourRanking",
            "request_type": "GetBeachWorldTourRanking",
            "node_path": "//BeachWorldTourRankingEntry",
            "description": "Beach World Tour ranking",
            "kwargs": {"Gender": "W", "Number": 10},
            "accept_json": False,
            "old_style": True,
        },
        {
            "id": "GetBeachOlympicSelectionRanking",
            "request_type": "GetBeachOlympicSelectionRanking",
            "node_path": "//BeachOlympicSelectionRankingEntry",
            "description": "Beach Olympic selection ranking",
            "kwargs": {"Gender": "W"},
            "accept_json": False,
            "old_style": True,
        },
        # --- Additional beach-relevant (single-entity + events) ---
        {
            "id": "GetBeachRound",
            "request_type": "GetBeachRound",
            "node_path": "//BeachRound",
            "description": "Single beach round by No",
            "kwargs": {},  # No from GetBeachRoundList
            "accept_json": False,
            "old_style": True,
        },
        {
            "id": "GetBeachRoundRanking",
            "request_type": "GetBeachRoundRanking",
            "node_path": "//BeachRoundRankingEntry",
            "description": "Pool/round standings (only for rounds with ranking)",
            "kwargs": {},  # No from GetBeachRoundList (use pool round if available)
            "accept_json": False,
            "old_style": True,
        },
        {
            "id": "GetBeachTeam",
            "request_type": "GetBeachTeam",
            "node_path": "//BeachTeam",
            "description": "Single beach team by No",
            "kwargs": {},  # No from GetBeachTeamList
            "accept_json": False,
            "old_style": True,
        },
        {
            "id": "GetEventList",
            "request_type": "GetEventList",
            "node_path": "//Event",
            "description": "List events (e.g. World Tour) with beach filter",
            "kwargs": {},
            "accept_json": False,
            "old_style": False,
        },
        {
            "id": "GetEvent",
            "request_type": "GetEvent",
            "node_path": "//Event",
            "description": "Single event by No",
            "kwargs": {},  # No from GetEventList
            "accept_json": False,
            "old_style": True,
        },
    ]

    match_list_result = vis_request_raw(
        "GetBeachMatchList",
        "//BeachMatch",
        Filter=f"NoTournament='{no_tournament}'",
    )
    match_sample = (match_list_result.get("parsed_sample") or [])
    no_match = int(match_sample[0]["No"]) if match_sample else 106968
    player_list_result = vis_request_raw("GetPlayerList", "//Player")
    player_sample = player_list_result.get("parsed_sample") or []
    no_player = int(player_sample[0]["No"]) if player_sample else 1
    # Rounds and team for single-entity requests
    round_list_result = vis_request_raw(
        "GetBeachRoundList",
        "//BeachRound",
        children=[("Filter", {"NoTournament": str(no_tournament)})],
    )
    def _get_no(rec: dict) -> int:
        return int(rec.get("No") or rec.get("no") or 0)

    round_sample = round_list_result.get("parsed_sample") or []
    no_round = (_get_no(round_sample[0]) or 335652) if round_sample else 335652
    # Prefer a pool round (Code PA, PB, etc.) for GetBeachRoundRanking so we get standings, not NotARankingRound
    no_round_with_ranking = no_round
    all_rounds = fetch_beach_round_list(no_tournament)  # full list so we don't miss pool rounds in first 5
    for r in (all_rounds or []):
        if not isinstance(r, dict):
            continue
        code = (r.get("Code") or r.get("code") or "").upper()
        if code in ("PA", "PB", "PC", "PD", "A", "B", "C", "D"):
            no_round_with_ranking = _get_no(r) or no_round
            break
    team_list_result = vis_request_raw(
        "GetBeachTeamList",
        "//BeachTeam",
        Filter=f"NoTournament='{no_tournament}'",
    )
    team_sample = team_list_result.get("parsed_sample") or []
    no_team = _get_no(team_sample[0]) if team_sample else 375442
    # Events (top-level beach events)
    event_list_result = vis_request_raw(
        "GetEventList",
        "//Event",
        children=[
            (
                "Filter",
                {
                    "HasBeachTournament": "True",
                    "NoParentEvent": "0",
                    "StartDate": "2024-01-01",
                    "EndDate": "2026-12-31",
                },
            )
        ],
    )
    event_sample = event_list_result.get("parsed_sample") or []
    no_event = (_get_no(event_sample[0]) or 1) if event_sample else 1

    for ep in endpoints:
        if ep["id"] == "GetBeachMatch":
            ep["kwargs"]["No"] = no_match
        elif ep["id"] == "GetPlayer":
            ep["kwargs"]["No"] = no_player
        elif ep["id"] == "GetBeachRoundList":
            ep["children"] = [("Filter", {"NoTournament": str(no_tournament)})]
        elif ep["id"] == "GetBeachRound":
            ep["kwargs"]["No"] = no_round
        elif ep["id"] == "GetBeachRoundRanking":
            ep["kwargs"]["No"] = no_round_with_ranking  # use pool round when available
        elif ep["id"] == "GetBeachTeam":
            ep["kwargs"]["No"] = no_team
        elif ep["id"] == "GetEventList":
            ep["children"] = [
                (
                    "Filter",
                    {
                        "HasBeachTournament": "True",
                        "NoParentEvent": "0",
                        "StartDate": "2024-01-01",
                        "EndDate": "2026-12-31",
                    },
                )
            ]
        elif ep["id"] == "GetEvent":
            ep["kwargs"]["No"] = no_event

    manifest = {"base_url": "https://www.fivb.org/Vis2009/XmlRequest.asmx", "endpoints": []}

    for ep in endpoints:
        eid = ep["id"]
        print(f"  {eid} ...")
        try:
            request_kwargs = dict(ep["kwargs"])
            if ep.get("children") is not None:
                request_kwargs["children"] = ep["children"]
            result = vis_request_raw(
                ep["request_type"],
                ep["node_path"],
                accept_json=ep["accept_json"],
                old_style=ep.get("old_style", False),
                **request_kwargs,
            )
            result["response_text"] = _truncate(result.get("response_text", ""))
            result["_description"] = ep["description"]
            out_path = OUTPUT_DIR / f"{eid}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            manifest["endpoints"].append({
                "id": eid,
                "request_type": ep["request_type"],
                "description": ep["description"],
                "status_code": result.get("response_status_code"),
                "parsed_record_count": result.get("parsed_record_count"),
                "file": f"{eid}.json",
            })
        except Exception as err:
            print(f"  {eid} FAILED: {err}")
            manifest["endpoints"].append({
                "id": eid,
                "request_type": ep["request_type"],
                "description": ep["description"],
                "error": str(err),
                "file": None,
            })

    manifest_path = OUTPUT_DIR / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nWrote {len(endpoints)} endpoint samples to {OUTPUT_DIR}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
