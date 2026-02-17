# VIS Web Service – Beach Volleyball–Relevant Endpoints

Reference for FIVB VIS endpoints that are relevant to beach volleyball.  
Docs: <https://www.fivb.org/VisSDK/VisWebService/>

## Implemented and tested (16 total)

| Request | Description | Notes |
|--------|-------------|--------|
| **GetBeachTournamentList** | List tournaments | Filter e.g. `Season='2025 2026'` |
| **GetBeachTournament** | Single tournament by No | |
| **GetBeachTeamList** | List teams | Filter e.g. `NoTournament='502'` |
| **GetBeachMatchList** | List matches | Filter optional: `NoTournament` for one tournament; **omit filter for all matches** (used for bulk ETL). |
| **GetBeachMatch** | Single match by No | |
| **GetBeachTournamentRanking** | Tournament finishing positions | Phase: Qualification \| MainDraw; old-style, XML |
| **GetBeachRoundList** | List rounds for a tournament | **Filter as child** `<Filter NoTournament="502"/>`; old-style |
| **GetPlayerList** | List all players | |
| **GetPlayer** | Single player by No | old-style, XML |
| **GetBeachWorldTourRanking** | World Tour ranking | Gender M/W; old-style, XML |
| **GetBeachOlympicSelectionRanking** | Olympic selection ranking | Gender M/W; old-style, XML |
| **GetBeachRound** | Single round by No | [GetBeachRound](https://www.fivb.org/VisSDK/VisWebService/GetBeachRound.html); old-style |
| **GetBeachRoundRanking** | Pool/round standings (teams in a round) | [GetBeachRoundRanking](https://www.fivb.org/VisSDK/VisWebService/GetBeachRoundRanking.html); only for rounds with ranking; old-style |
| **GetBeachTeam** | Single team by No | [GetBeachTeam](https://www.fivb.org/VisSDK/VisWebService/GetBeachTeam.html); old-style |
| **GetEventList** | List events (e.g. “World Tour 2025”) | [GetEventList](https://www.fivb.org/VisSDK/VisWebService/GetEventList.html); **Filter as child** HasBeachTournament, NoParentEvent, StartDate/EndDate |
| **GetEvent** | Single event by No | [GetEvent](https://www.fivb.org/VisSDK/VisWebService/GetEvent.html); old-style |

Events are parent containers (e.g. “Swatch World Tour 2011”, “World Championships 2011”) that group beach tournaments; useful for hierarchy and season structure.

## Indoor-only (not used for beach warehouse)

- GetVolleyTournamentList, GetVolleyTournament, GetVolleyTeamList, GetVolleyMatchList, GetVolleyMatch, GetVolleyTournamentRanking, GetVolleyPlayerList, GetVolleyPlayer, GetVolleyPoolList, GetVolleyPool, GetVolleyPoolRanking, GetVolleyLive

## Other VIS endpoints (optional)

- **GetRefereeList** – referees (filter by tournament etc.)
- **GetPressReleaseList** / **GetPressReleaseText** – media
