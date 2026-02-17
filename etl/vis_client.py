"""
Client for the FIVB VIS Web Service (data API).

Uses the official API documented at:
  https://www.fivb.org/VisSDK/VisWebService/

Single endpoint: POST XML request to XmlRequest.asmx. We request JSON (Accept: application/json)
and normalize camelCase keys to PascalCase; XML is used only when the server returns XML.
No authentication required for public data.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

VIS_BASE_URL = "https://www.fivb.org/Vis2009/XmlRequest.asmx"


def _escape_attr(v: Any) -> str:
    return (
        str(v)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _build_request_xml(
    request_type: str,
    old_style: bool = False,
    children: Optional[List[Tuple[str, Dict[str, Any]]]] = None,
    **kwargs: Any,
) -> str:
    """Build VIS request XML. Attribute names are PascalCase (Type, Fields, No, Filter, etc.).
    If children is provided (e.g. [('Filter', {'NoTournament': 502})]), the Request has child
    elements and is wrapped in <Requests> (required by the API for this format)."""

    def to_pascal(s: str) -> str:
        return s[:1].upper() + s[1:] if s else s

    attrs = {"Type": request_type}
    for k, v in kwargs.items():
        if k in ("old_style", "children") or v is None or v == "":
            continue
        key = to_pascal(k)
        if key == "Fields" and isinstance(v, (list, tuple)):
            v = " ".join(str(x) for x in v)
        attrs[key] = str(v)

    parts = ["<Request"]
    for k, v in attrs.items():
        parts.append(f' {k}="{_escape_attr(v)}"')
    if children:
        parts.append(">")
        for tag, child_attrs in children:
            segs = [f"<{tag}"]
            for ck, cv in child_attrs.items():
                segs.append(f' {ck}="{_escape_attr(cv)}"')
            segs.append(" />")
            parts.append("".join(segs))
        parts.append("</Request>")
        inner = "".join(parts)
        return f"<Requests>{inner}</Requests>"
    parts.append(" />")
    inner = "".join(parts)
    if old_style:
        return f"<Requests>{inner}</Requests>"
    return inner


def _local_tag(elem: ET.Element) -> str:
    """Return tag name without namespace."""
    tag = elem.tag
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _xml_to_records(root: ET.Element, node_path: str) -> List[Dict[str, Any]]:
    """Extract repeated nodes as list of dicts (attributes + direct child element text)."""
    # ElementTree findall uses simple paths; normalize to .//Tag
    path = node_path
    if path.startswith("//"):
        path = "." + path
    if not path.startswith("."):
        path = f".//{path}"
    nodes = root.findall(path)
    records = []
    for node in nodes:
        if node is None:
            continue
        rec = {}
        if node.attrib:
            rec.update(node.attrib)
        # Direct child elements with text (e.g. <Rank>1</Rank><NoTeam>x</NoTeam>)
        for child in node:
            if len(child) == 0 and child.text is not None:
                text = child.text.strip() if child.text else ""
                rec[_local_tag(child)] = text
            elif child.attrib:
                rec[_local_tag(child)] = child.attrib
        # Flatten single child text when no other content
        if len(node) == 0 and node.text and node.text.strip():
            rec["_text"] = node.text.strip()
        records.append(rec)
    return records


def _camel_to_pascal(s: str) -> str:
    """Turn camelCase into PascalCase (e.g. countryCode -> CountryCode, no -> No)."""
    if not s:
        return s
    return s[0].upper() + s[1:]


def _normalize_json_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize JSON record keys to PascalCase to match XML attribute names (No, Name, CountryCode)."""
    if not isinstance(rec, dict):
        return rec
    return {_camel_to_pascal(k): v for k, v in rec.items()}


def _parse_vis_response(text: str, content_type: str, node_path: str):
    """Parse VIS response (JSON preferred, else XML) into a list of record dicts."""
    if "json" in (content_type or ""):
        try:
            import json

            data = json.loads(text)
            # VIS returns {"data": [ {...}, ... ]} with camelCase keys
            if isinstance(data, list):
                return [_normalize_json_record(r) for r in data]
            if isinstance(data, dict):
                payload = data.get("data")
                if isinstance(payload, list):
                    return [_normalize_json_record(r) for r in payload]
                if isinstance(payload, dict):
                    return [_normalize_json_record(payload)]
                for v in data.values():
                    if isinstance(v, list):
                        return [_normalize_json_record(r) for r in v]
                    if isinstance(v, dict):
                        return [_normalize_json_record(v)]
                return []
            return []
        except Exception as e:
            logger.warning("VIS JSON parse failed: %s", e)
            return []

    # XML fallback
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        logger.warning("VIS XML parse failed: %s", e)
        return []

    # Remove namespace if present
    if root.tag.startswith("{"):
        for elem in root.iter():
            if "}" in elem.tag:
                elem.tag = elem.tag.split("}", 1)[1]

    return _xml_to_records(root, node_path)


# Default field sets (space-separated). Per VIS docs, request all available fields;
# the API returns only those the client has access to. Sources:
# https://www.fivb.org/VisSDK/VisWebService/ (BeachTournament, BeachMatch, BeachTeam, BeachRound, Player, Event, etc.)
DEFAULT_FIELDS = {
    # BeachTournament: doc includes Code, NoEvent, StartDateMainDraw, EndDateMainDraw, StartDateQualification, EndDateQualification, NbTeamsMainDraw, NbTeamsQualification, Title, Version, ...
    "GetBeachTournamentList": (
        "No Name Code CountryCode CountryName City StartDate EndDate Season Gender Type Status "
        "NoEvent Title Version StartDateMainDraw EndDateMainDraw StartDateQualification EndDateQualification "
        "NbTeamsMainDraw NbTeamsQualification NbTeamsFromQualification"
    ),
    "GetBeachTournament": (
        "No Name Code CountryCode City StartDate EndDate Season Gender Type Status "
        "NoEvent Title Version StartDateMainDraw EndDateMainDraw StartDateQualification EndDateQualification "
        "NbTeamsMainDraw NbTeamsQualification"
    ),
    # BeachMatch: doc includes LocalDate, LocalTime, NoPlayerA1, NoPlayerA2, NoPlayerB1, NoPlayerB2, TeamAName, TeamBName, set points, durations, ...
    "GetBeachMatchList": (
        "No NoTournament NoRound NoTeamA NoTeamB NoInTournament MatchPointsA MatchPointsB "
        "DateTimeLocal LocalDate LocalTime ResultType Status "
        "NoPlayerA1 NoPlayerA2 NoPlayerB1 NoPlayerB2 TeamAName TeamBName "
        "PointsTeamASet1 PointsTeamASet2 PointsTeamASet3 PointsTeamBSet1 PointsTeamBSet2 PointsTeamBSet3 "
        "DurationSet1 DurationSet2 DurationSet3 Court RoundCode RoundName TournamentCode TournamentName WinnerRank LoserRank BeginDateTimeUtc"
    ),
    "GetBeachMatch": (
        "No NoTournament NoRound NoTeamA NoTeamB NoInTournament MatchPointsA MatchPointsB "
        "DateTimeLocal LocalDate LocalTime ResultType Status "
        "NoPlayerA1 NoPlayerA2 NoPlayerB1 NoPlayerB2 TeamAName TeamBName "
        "PointsTeamASet1 PointsTeamASet2 PointsTeamASet3 PointsTeamBSet1 PointsTeamBSet2 PointsTeamBSet3 "
        "DurationSet1 DurationSet2 DurationSet3 Court RoundCode RoundName TournamentCode TournamentName WinnerRank LoserRank BeginDateTimeUtc"
    ),
    # BeachTeam: doc includes Name, Rank, ConfederationCode, EarnedPointsTeam, PositionInMainDraw, IsInMainDraw, ...
    "GetBeachTeamList": (
        "No NoTournament NoPlayer1 NoPlayer2 Name CountryCode ConfederationCode Status Rank "
        "EarnedPointsTeam EarnedPointsPlayer PositionInMainDraw PositionInQualification "
        "IsInMainDraw IsInQualification TournamentName TournamentTitle TournamentType"
    ),
    "GetBeachTeam": (
        "No NoTournament NoPlayer1 NoPlayer2 Name CountryCode ConfederationCode Status Rank "
        "EarnedPointsTeam EarnedPointsPlayer PositionInMainDraw PositionInQualification "
        "IsInMainDraw IsInQualification TournamentName TournamentTitle"
    ),
    "GetBeachTournamentRanking": "Rank Position NoTeam",
    # BeachRound: doc includes No, NoTournament, NoInTournament, Version, RankMethod
    "GetBeachRoundList": "No NoTournament NoInTournament Code Name Bracket Phase StartDate EndDate Version RankMethod",
    "GetBeachRound": "No NoTournament NoInTournament Code Name Bracket Phase StartDate EndDate Version RankMethod",
    "GetBeachRoundRanking": "Position Rank TeamFederationCode TeamName MatchPoints MatchesWon MatchesLost",
    # BeachWorldTourRankingEntry: doc includes NoPlayer1, NoPlayer2, EarnedPointsPlayer, EarningsTeam, EarningsPlayer, ...
    "GetBeachWorldTourRanking": (
        "Position Rank TeamName TeamFederationCode NbParticipations "
        "EarnedPointsTeam EarnedPointsPlayer EarningsTotalTeam EarningsTeam EarningsPlayer "
        "EarningsBonusTeam EarningsBonusPlayer EarningsTotalPlayer NoPlayer1 NoPlayer2 HasTournamentsRemoved"
    ),
    # BeachOlympicSelectionRankingEntry: doc includes NoPlayer1, NoPlayer2, GamesYear
    "GetBeachOlympicSelectionRanking": (
        "Position TeamName TeamCountryCode NbParticipations SelectionRank Points Status "
        "NoPlayer1 NoPlayer2 GamesYear"
    ),
    # Player: doc has many fields; request core + beach-relevant public
    "GetPlayer": (
        "No FederationCode FirstName LastName Gender Nationality NationalityCode BirthDate BirthPlace Height Weight "
        "PlaysBeach PlaysVolley TeamName CountryCode ConfederationCode ActiveBeach ActiveVolley"
    ),
    "GetPlayerList": (
        "No FirstName LastName BirthDate BirthPlace Height Weight CountryCode Gender "
        "FederationCode NationalityCode PlaysBeach PlaysVolley TeamName ConfederationCode"
    ),
    # Event: doc includes Type, Version, NoParentEvent, CountryCode, HasBeachTournament, IsVisManaged
    "GetEventList": "No Code Name StartDate EndDate Type Version NoParentEvent CountryCode HasBeachTournament HasMenTournament HasWomenTournament IsVisManaged",
    "GetEvent": "No Code Name StartDate EndDate Type Version NoParentEvent CountryCode HasBeachTournament IsVisManaged",
}


def vis_request(
    request_type: str,
    node_path: str,
    fields: Optional[str] = None,
    accept_json: bool = True,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Send one VIS request and return a list of record dicts.

    Prefers JSON (accept_json=True); response keys are normalized to PascalCase.
    request_type: e.g. GetBeachTournamentList, GetBeachMatchList, GetBeachTeamList.
    node_path: used when parsing XML fallback; ignored for JSON.
    fields: optional Fields value; if omitted uses DEFAULT_FIELDS for that request_type.
    **kwargs: other request attributes (No, Filter, NoTournament, Phase, Gender, old_style, etc.).
    """
    if fields is None:
        fields = DEFAULT_FIELDS.get(request_type, "")
    old_style = kwargs.pop("old_style", False)
    children = kwargs.pop("children", None)
    xml_body = _build_request_xml(
        request_type,
        old_style=old_style,
        children=children,
        Fields=fields or None,
        **kwargs,
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FIVB-Leaderboard-ETL/1.0)",
        "Content-Type": "application/xml; charset=utf-8",
        "Accept": "application/json" if accept_json else "application/xml",
    }
    resp = requests.post(
        VIS_BASE_URL, data=xml_body.encode("utf-8"), headers=headers, timeout=60
    )
    resp.raise_for_status()
    ct = resp.headers.get("Content-Type", "")
    text = resp.text or ""

    if not text.strip():
        logger.warning("VIS empty response for %s", request_type)
        return []

    out = _parse_vis_response(text, ct, node_path)
    if not isinstance(out, list):
        return [out] if isinstance(out, dict) else []
    return out


def vis_request_raw(
    request_type: str,
    node_path: str,
    fields: Optional[str] = None,
    accept_json: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Send one VIS request and return request XML, raw response text, content-type, and parsed records.
    Used for documenting API contracts (save request + response samples).
    """
    if fields is None:
        fields = DEFAULT_FIELDS.get(request_type, "")
    old_style = kwargs.pop("old_style", False)
    children = kwargs.pop("children", None)
    xml_body = _build_request_xml(
        request_type,
        old_style=old_style,
        children=children,
        Fields=fields or None,
        **kwargs,
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FIVB-Leaderboard-ETL/1.0)",
        "Content-Type": "application/xml; charset=utf-8",
        "Accept": "application/json" if accept_json else "application/xml",
    }
    resp = requests.post(
        VIS_BASE_URL, data=xml_body.encode("utf-8"), headers=headers, timeout=60
    )
    text = resp.text or ""
    ct = resp.headers.get("Content-Type", "")
    parsed = []
    if text.strip():
        parsed = _parse_vis_response(text, ct, node_path)
        if not isinstance(parsed, list):
            parsed = [parsed] if isinstance(parsed, dict) else []
    return {
        "request_type": request_type,
        "request_xml": xml_body,
        "request_url": VIS_BASE_URL,
        "response_status_code": resp.status_code,
        "response_content_type": ct,
        "response_text": text,
        "parsed_record_count": len(parsed),
        "parsed_sample": parsed[:5] if parsed else [],
    }


# ---- Volleyball competitions (Beach) ----
# Endpoints per https://www.fivb.org/VisSDK/VisWebService/#Volleyball%20competitions.html
# GetBeachTournamentList, GetBeachTournament, GetBeachTeamList, GetBeachMatchList,
# GetBeachTournamentRanking (https://www.fivb.org/VisSDK/VisWebService/#GetBeachTournamentRanking.html); GetPlayer.

# Default season for GetBeachTournamentList filter (e.g. space-separated years). Use in filter_expr, e.g. f"Season='{TOURNAMENT_SEASON}'".
# https://www.fivb.org/VisSDK/VisWebService/#VolleyTournamentFilter.html
TOURNAMENT_SEASON = "2025 2026"


def fetch_beach_tournaments(
    fields: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get list of beach tournaments (VIS GetBeachTournamentList).
    filter_expr: optional Filter (VolleyTournamentFilter); include season here if needed (e.g. \"Season='2025 2026'\")."""
    kwargs: Dict[str, Any] = {}
    if filter_expr:
        kwargs["Filter"] = filter_expr
    return vis_request(
        "GetBeachTournamentList", "//BeachTournament", fields=fields, **kwargs
    )


def fetch_beach_tournament(
    no: int, fields: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get one beach tournament by number (VIS GetBeachTournament)."""
    return vis_request("GetBeachTournament", "//BeachTournament", No=no, fields=fields)


def fetch_beach_matches_for_tournament(
    no_tournament: int,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get beach matches for a tournament (VIS GetBeachMatchList with Filter)."""
    return vis_request(
        "GetBeachMatchList",
        "//BeachMatch",
        fields=fields,
        Filter=f"NoTournament='{no_tournament}'",
    )


def fetch_beach_matches_all(
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get all beach matches in one request (VIS GetBeachMatchList with no filter).
    Per VIS docs: 'If it is not specified, the response will contain all the beach volleyball matches.'
    Use this for bulk ingestion instead of one call per tournament."""
    return vis_request(
        "GetBeachMatchList",
        "//BeachMatch",
        fields=fields,
    )


def fetch_beach_matches_date_range(
    first_date: str,
    last_date: str,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get beach matches in a date range (VIS GetBeachMatchList with Filter FirstDate/LastDate).
    Use when fetch_beach_matches_all() is too large or times out."""
    return vis_request(
        "GetBeachMatchList",
        "//BeachMatch",
        fields=fields,
        children=[("Filter", {"FirstDate": first_date, "LastDate": last_date})],
    )


def fetch_beach_teams(
    fields: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get beach teams (VIS GetBeachTeamList). filter_expr optional (e.g. NoTournament='123'). Omit for all teams."""
    kwargs: Dict[str, Any] = {}
    if filter_expr:
        kwargs["Filter"] = filter_expr
    return vis_request(
        "GetBeachTeamList",
        "//BeachTeam",
        fields=fields,
        **kwargs,
    )


def fetch_beach_teams_for_tournament(
    no_tournament: int,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get beach teams for a tournament (VIS GetBeachTeamList with Filter)."""
    return fetch_beach_teams(
        fields=fields,
        filter_expr=f"NoTournament='{no_tournament}'",
    )


def fetch_beach_tournament_ranking(
    no_tournament: int,
    phase: Optional[str] = None,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get ranking for one beach tournament (VIS GetBeachTournamentRanking).
    Returns list with Rank, Position, NoTeam per entry (phase: Qualification | MainDraw or None).
    Uses XML (accept_json=False) because this endpoint returns NotInJson when JSON is requested."""
    return vis_request(
        "GetBeachTournamentRanking",
        "//BeachTournamentRankingEntry",
        No=no_tournament,
        Phase=phase,
        fields=fields,
        old_style=True,
        accept_json=False,
    )


def fetch_beach_round_list(
    no_tournament: int,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get list of beach rounds for a tournament (VIS GetBeachRoundList).
    Uses Filter as a child element and old-style wrapper per API docs."""
    return vis_request(
        "GetBeachRoundList",
        "//BeachRound",
        fields=fields,
        children=[("Filter", {"NoTournament": str(no_tournament)})],
        accept_json=False,
    )


def fetch_beach_round(
    no: int, fields: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get one beach round by number (VIS GetBeachRound). Uses old-style wrapper."""
    return vis_request(
        "GetBeachRound",
        "//BeachRound",
        No=no,
        fields=fields,
        old_style=True,
        accept_json=False,
    )


def fetch_beach_round_ranking(
    no_round: int, fields: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get pool/round standings for a beach round (VIS GetBeachRoundRanking).
    Only for rounds with ranking (e.g. pools); direct-elimination rounds return error. Uses old-style."""
    return vis_request(
        "GetBeachRoundRanking",
        "//BeachRoundRankingEntry",
        No=no_round,
        fields=fields,
        old_style=True,
        accept_json=False,
    )


def fetch_beach_team(
    no: int, fields: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get one beach team by number (VIS GetBeachTeam). Uses old-style wrapper."""
    return vis_request(
        "GetBeachTeam",
        "//BeachTeam",
        No=no,
        fields=fields,
        old_style=True,
        accept_json=False,
    )


def fetch_event_list(
    fields: Optional[str] = None,
    has_beach_tournament: bool = True,
    no_parent_event: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get list of events (VIS GetEventList). Events group tournaments (e.g. World Tour).
    Filter as child; use has_beach_tournament=True for beach. NoParentEvent=0 for top-level."""
    attrs: Dict[str, str] = {
        "HasBeachTournament": "true" if has_beach_tournament else "false",
        "NoParentEvent": str(no_parent_event),
    }
    if start_date:
        attrs["StartDate"] = start_date
    if end_date:
        attrs["EndDate"] = end_date
    return vis_request(
        "GetEventList",
        "//Event",
        fields=fields,
        children=[("Filter", attrs)],
        accept_json=False,
    )


def fetch_event(no: int, fields: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get one event by number (VIS GetEvent). Uses old-style wrapper."""
    return vis_request(
        "GetEvent",
        "//Event",
        No=no,
        fields=fields,
        old_style=True,
        accept_json=False,
    )


def fetch_player_list(
    fields: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get all players (VIS GetPlayerList). filter_expr optional. Omit for full list."""
    kwargs: Dict[str, Any] = {}
    if filter_expr:
        kwargs["Filter"] = filter_expr
    return vis_request(
        "GetPlayerList",
        "//Player",
        fields=fields,
        **kwargs,
    )


def fetch_player(no: int, fields: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get one player by number (VIS GetPlayer). Returns list of one record or empty.
    Uses old-style wrapper (required by API)."""
    return vis_request(
        "GetPlayer",
        "//Player",
        No=no,
        fields=fields,
        old_style=True,
        accept_json=False,
    )


def fetch_beach_world_tour_ranking(
    gender: str = "W",
    number: Optional[int] = None,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get beach World Tour ranking (VIS GetBeachWorldTourRanking). Gender M or W. Uses old-style wrapper."""
    kwargs: Dict[str, Any] = {"Gender": gender, "old_style": True}
    if number is not None:
        kwargs["Number"] = number
    return vis_request(
        "GetBeachWorldTourRanking",
        "//BeachWorldTourRankingEntry",
        fields=fields,
        accept_json=False,
        **kwargs,
    )


def fetch_beach_olympic_selection_ranking(
    gender: str = "W",
    games_year: Optional[int] = None,
    fields: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get beach Olympic selection ranking (VIS GetBeachOlympicSelectionRanking). Gender M or W. Uses old-style wrapper."""
    kwargs: Dict[str, Any] = {"Gender": gender, "old_style": True}
    if games_year is not None:
        kwargs["GamesYear"] = games_year
    return vis_request(
        "GetBeachOlympicSelectionRanking",
        "//BeachOlympicSelectionRankingEntry",
        fields=fields,
        accept_json=False,
        **kwargs,
    )
