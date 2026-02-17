"""
Integration tests for the FIVB VIS Web Service client.

These tests perform real HTTP requests to https://www.fivb.org/Vis2009/XmlRequest.asmx.
Run from project root: pytest tests/test_vis_client.py -v
"""

from __future__ import annotations

from etl.vis_client import (
    fetch_beach_matches_for_tournament,
    fetch_beach_teams_for_tournament,
    fetch_beach_tournament,
    fetch_beach_tournaments,
    vis_request,
)


def _is_list_of_dicts(data) -> bool:
    return isinstance(data, list) and (not data or isinstance(data[0], dict))


def test_vis_request_get_beach_tournament_list_returns_list():
    """GetBeachTournamentList returns a non-empty list of dicts with expected keys."""
    result = vis_request(
        "GetBeachTournamentList",
        "//BeachTournament",
        accept_json=True,
    )
    assert isinstance(result, list), f"Expected list, got {type(result)}"
    assert len(result) > 0, "Expected at least one tournament"
    first = result[0]
    assert isinstance(first, dict), f"Expected dict items, got {type(first)}"
    # VIS XML uses PascalCase; JSON might use same or different
    has_no = "No" in first or "no" in first
    has_name = "Name" in first or "name" in first
    assert has_no or has_name, f"Expected No/Name in keys: {list(first.keys())}"


def test_fetch_beach_tournaments():
    """fetch_beach_tournaments() returns list of tournament records."""
    tournaments = fetch_beach_tournaments()
    assert _is_list_of_dicts(
        tournaments
    ), f"Expected list of dicts: {type(tournaments)} {tournaments[:1] if tournaments else []}"
    if tournaments:
        keys = list(tournaments[0].keys())
        assert any(k in keys for k in ("No", "no", "Name", "name")), keys


def test_fetch_beach_tournament_single():
    """fetch_beach_tournament(no) returns one tournament (list with one element)."""
    result = fetch_beach_tournament(502)
    assert isinstance(result, list)
    assert len(result) == 1, f"Expected one tournament, got {len(result)}"
    t = result[0]
    assert isinstance(t, dict)
    assert t.get("No") == 502 or t.get("no") == 502, list(t.keys())


def test_fetch_beach_teams_for_tournament():
    """fetch_beach_teams_for_tournament(no) returns list of teams."""
    teams = fetch_beach_teams_for_tournament(502)
    assert isinstance(teams, list)
    if teams:
        assert isinstance(teams[0], dict), type(teams[0])


def test_fetch_beach_matches_for_tournament():
    """fetch_beach_matches_for_tournament(no) returns list of matches."""
    matches = fetch_beach_matches_for_tournament(502)
    assert isinstance(matches, list)
    if matches:
        assert isinstance(matches[0], dict), type(matches[0])


