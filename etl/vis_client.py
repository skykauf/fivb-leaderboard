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
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

VIS_BASE_URL = "https://www.fivb.org/Vis2009/XmlRequest.asmx"


def _build_request_xml(
    request_type: str, old_style: bool = False, **kwargs: Any
) -> str:
    """Build VIS request XML. Attribute names are PascalCase (Type, Fields, No, Filter, etc.)."""

    def to_pascal(s: str) -> str:
        return s[:1].upper() + s[1:] if s else s

    attrs = {"Type": request_type}
    for k, v in kwargs.items():
        if k == "old_style" or v is None or v == "":
            continue
        key = to_pascal(k)
        if key == "Fields" and isinstance(v, (list, tuple)):
            v = " ".join(str(x) for x in v)
        attrs[key] = str(v)

    parts = ["<Request"]
    for k, v in attrs.items():
        escaped = (
            str(v)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )
        parts.append(f' {k}="{escaped}"')
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


# Default field sets (space-separated per VIS docs). Can be overridden.
DEFAULT_FIELDS = {
    "GetBeachTournamentList": "No Name CountryCode CountryName City StartDate EndDate Season Gender Type Status",
    "GetBeachTournament": "No Name CountryCode City StartDate EndDate Season Gender Type Status",
    "GetBeachMatchList": "No NoTournament NoRound NoTeamA NoTeamB MatchPointsA MatchPointsB DateTimeLocal ResultType Status",
    "GetBeachMatch": "No NoTournament NoRound NoTeamA NoTeamB MatchPointsA MatchPointsB DateTimeLocal ResultType Status",
    "GetBeachTeamList": "No NoTournament NoPlayer1 NoPlayer2 CountryCode Status",
    "GetBeachTeam": "No NoTournament NoPlayer1 NoPlayer2 CountryCode Status",
    "GetBeachTournamentRanking": "Rank Position NoTeam",
    "GetPlayer": "No FirstName LastName BirthDate Height CountryCode Gender",
    "GetPlayerList": "No FirstName LastName BirthDate Height CountryCode Gender",
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
    xml_body = _build_request_xml(
        request_type, old_style=old_style, Fields=fields or None, **kwargs
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
    """Get one player by number (VIS GetPlayer). Returns list of one record or empty."""
    return vis_request(
        "GetPlayer",
        "//Player",
        No=no,
        fields=fields,
    )
