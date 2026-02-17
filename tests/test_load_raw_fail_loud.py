"""
Tests that the ETL fails loudly when VIS endpoints return no data.

Uses mocks so no real API or DB is required.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from etl.load_raw import (
    load_tournaments,
    load_players,
    _verify_raw_tables_non_empty,
)


def _mock_engine_with_counts(counts: dict[str, int]):
    """Build a mock engine that returns given COUNT(*) per table."""

    def execute(sql):
        s = sql if isinstance(sql, str) else str(sql)
        for table, cnt in counts.items():
            if table in s:
                return MagicMock(fetchone=lambda c=cnt: (c,))
        return MagicMock(fetchone=lambda: (0,))

    conn = MagicMock()
    conn.execute = execute
    conn.__enter__ = lambda self: self
    conn.__exit__ = lambda *a: None
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def test_load_tournaments_raises_when_api_returns_empty():
    with patch("etl.load_raw.fetch_beach_tournaments", return_value=[]):
        engine = MagicMock()
        with pytest.raises(
            RuntimeError, match="GetBeachTournamentList returned no data"
        ):
            load_tournaments(engine)


def test_verify_raw_tables_non_empty_raises_when_any_empty():
    engine = _mock_engine_with_counts(
        {
            "raw_fivb_tournaments": 1,
            "raw_fivb_teams": 1,
            "raw_fivb_matches": 1,
            "raw_fivb_results": 0,  # empty
            "raw_fivb_players": 1,
        }
    )
    with pytest.raises(RuntimeError, match="Raw tables are empty.*raw_fivb_results"):
        _verify_raw_tables_non_empty(engine)


def test_load_players_raises_when_api_returns_empty():
    with patch("etl.load_raw.fetch_player_list", return_value=[]):
        engine = MagicMock()
        with pytest.raises(RuntimeError, match="GetPlayerList returned no data"):
            load_players(engine)


def test_load_players_raises_when_no_valid_players():
    with patch("etl.load_raw.fetch_player_list", return_value=[{"Errors": "x"}]):
        engine = MagicMock()
        with pytest.raises(RuntimeError, match="no valid players"):
            load_players(engine)
