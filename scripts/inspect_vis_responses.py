"""
Run each VIS request used by load_raw and print full response as JSON.
Writes full output to scripts/vis_responses/*.json and prints truncated view to stdout.
Run from project root: python scripts/inspect_vis_responses.py
Requires: pip install -e . (DATABASE_URL not required).
"""

from __future__ import annotations

import json
import os
import sys

# Ensure project root on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from etl.vis_client import (
    TOURNAMENT_SEASON,
    fetch_beach_tournaments,
    fetch_beach_teams_for_tournament,
    fetch_beach_matches_for_tournament,
    fetch_beach_tournament_ranking,
    fetch_player,
)

# Max items to show in stdout (full payloads still written to files)
PRINT_HEAD = 5
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "vis_responses")


def _truncate(obj, head: int):
    """If obj is a list, return first `head` items plus _truncated hint."""
    if isinstance(obj, list) and len(obj) > head:
        return obj[:head] + [{"_truncated": f"... {len(obj) - head} more (total {len(obj)})"}]
    return obj


def _dump(obj, label: str, filename: str) -> None:
    """Write full JSON to file; print truncated JSON to stdout."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
    print(f"\n{'='*60}")
    print(label)
    print("=" * 60)
    print(f"Full output written to: {path}")
    print("Preview (first %s items):" % PRINT_HEAD)
    print(json.dumps(_truncate(obj, PRINT_HEAD), indent=2, default=str))


def main():
    # 1. GetBeachTournamentList (same as load_raw: filter includes seasons)
    tournaments = fetch_beach_tournaments(
        filter_expr=f"Season='{TOURNAMENT_SEASON}'"
    )
    _dump(
        tournaments,
        "1. GetBeachTournamentList (filter_expr=%r)" % f"Season='{TOURNAMENT_SEASON}'",
        "1_get_beach_tournament_list.json",
    )

    # Pick first tournament for per-tournament requests
    no_t = None
    for t in (tournaments or [])[:50]:
        no = t.get("No")
        if no is not None:
            try:
                no_t = int(no)
                break
            except (TypeError, ValueError):
                continue
    if no_t is None:
        print("\n(No tournament No found; skipping per-tournament requests.)")
        no_t = 0  # placeholder so we still try rankings/player with a dummy

    # 2. GetBeachTeamList
    teams = fetch_beach_teams_for_tournament(no_t) if no_t else []
    _dump(
        teams,
        "2. GetBeachTeamList (NoTournament=%s)" % no_t,
        "2_get_beach_team_list.json",
    )

    # 3. GetBeachMatchList
    matches = fetch_beach_matches_for_tournament(no_t) if no_t else []
    _dump(
        matches,
        "3. GetBeachMatchList (NoTournament=%s)" % no_t,
        "3_get_beach_match_list.json",
    )

    # 4. GetBeachTournamentRanking (None, MainDraw, Qualification - same as load_raw)
    for i, phase in enumerate((None, "MainDraw", "Qualification")):
        ranking = fetch_beach_tournament_ranking(no_t, phase=phase) if no_t else []
        _dump(
            ranking,
            "4. GetBeachTournamentRanking (NoTournament=%s, Phase=%r)" % (no_t, phase),
            "4_get_beach_tournament_ranking_phase_%s.json" % (i if phase is None else phase),
        )

    # 5. GetPlayer (only if we have a player id from teams)
    pid = None
    if teams:
        pid = teams[0].get("NoPlayer1") or teams[0].get("NoPlayer2")
    if pid is not None:
        try:
            players = fetch_player(int(pid))
            _dump(players, "5. GetPlayer (No=%s)" % pid, "5_get_player.json")
        except Exception as e:
            _dump(
                {"error": str(e), "player_no": pid},
                "5. GetPlayer (No=%s)" % pid,
                "5_get_player.json",
            )
    else:
        print("\n(No team/player id available; skipping GetPlayer.)")


if __name__ == "__main__":
    main()
