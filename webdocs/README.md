# FIVB VIS API – Swagger webdocs

Interactive API docs for the **FIVB VIS Web Service** beach endpoints. The server proxies requests to the VIS API so you can use **Try it out** from the browser without CORS issues.

## Quick start

From the **project root**:

```bash
# Install dependencies (once)
pip3 install -r webdocs/requirements.txt

# Start the server
python3 -m webdocs.server
```

Then open **http://127.0.0.1:8000/docs** for Swagger UI.

Alternative (with reload):

```bash
python3 -m uvicorn webdocs.server:app --reload --port 8000
```

## What you get

- **Swagger UI** at `/docs` – list of all 16 beach endpoints with request body schemas and **Try it out**.
- **ReDoc** at `/redoc` – alternate docs view.
- **OpenAPI JSON** at `/openapi.json` – machine-readable spec.

Each operation is a `POST /api/{RequestType}` (e.g. `POST /api/GetBeachTournamentList`) with a JSON body. The server forwards the request to the FIVB VIS API and returns `{ "count": N, "data": [ ... ] }`.

## Endpoints (16)

| Operation | Description |
|-----------|-------------|
| GetBeachTournamentList | List tournaments (filter by season) |
| GetBeachTournament | Single tournament by No |
| GetBeachTeamList | List teams (filter by NoTournament) |
| GetBeachMatchList | List matches (one tournament via filter, or all matches with no filter) |
| GetBeachMatch | Single match by No |
| GetBeachTournamentRanking | Tournament finishing positions |
| GetBeachRoundList | List rounds for a tournament |
| GetBeachRound | Single round by No |
| GetBeachRoundRanking | Pool/round standings |
| GetBeachTeam | Single team by No |
| GetPlayerList | List players |
| GetPlayer | Single player by No |
| GetBeachWorldTourRanking | World Tour ranking |
| GetBeachOlympicSelectionRanking | Olympic selection ranking |
| GetEventList | List events (World Tour, etc.) |
| GetEvent | Single event by No |

VIS reference: https://www.fivb.org/VisSDK/VisWebService/
