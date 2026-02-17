# FIVB VIS Web Service – Request/Response Samples

This folder holds **request and response samples** for the [FIVB VIS Web Service](https://www.fivb.org/VisSDK/VisWebService/) so we can understand the data contracts for the data warehouse.

## How it was generated

From the project root (with venv activated):

```bash
python3 scripts/explore_vis_api.py
```

The script sends test requests to `POST https://www.fivb.org/Vis2009/XmlRequest.asmx` for each endpoint and writes one JSON file per endpoint plus `manifest.json`.

## Contents

| File | Description |
|------|-------------|
| `manifest.json` | Index of all endpoints: id, request_type, description, status_code, parsed_record_count, file. |
| `GetBeachTournamentList.json` | List of beach tournaments (optional Filter e.g. by Season). |
| `GetBeachTournament.json` | Single tournament by `No`. |
| `GetBeachTeamList.json` | List of beach teams (optional Filter e.g. by NoTournament). |
| `GetBeachMatchList.json` | List of matches (for a tournament with filter, or all matches with no filter; bulk ETL uses no filter). |
| `GetBeachMatch.json` | Single match by `No`. |
| `GetBeachTournamentRanking.json` | Tournament finishing positions (Phase: Qualification \| MainDraw). **Returns XML** (old_style). |
| `GetBeachRoundList.json` | List of rounds for a tournament (may require different Filter/params). |
| `GetPlayerList.json` | List of players. |
| `GetPlayer.json` | Single player by `No`. |
| `GetBeachWorldTourRanking.json` | World Tour ranking (may require Season/Gender params per docs). |
| `GetBeachOlympicSelectionRanking.json` | Olympic selection ranking (may require params per docs). |

## Per-file structure

Each `*.json` file (except `manifest.json`) has the same shape:

- **request_type** – VIS request type (e.g. `GetBeachTournamentList`).
- **request_xml** – Exact XML body sent in the POST.
- **request_url** – `https://www.fivb.org/Vis2009/XmlRequest.asmx`.
- **response_status_code** – HTTP status (200, 400, 404, etc.).
- **response_content_type** – e.g. `application/json` or `application/xml`.
- **response_text** – Raw response body (truncated at 50k chars in the file).
- **parsed_record_count** – Number of records parsed from the response.
- **parsed_sample** – First up to 5 records (PascalCase keys) for schema inspection.
- **_description** – Short description of the endpoint.

## Response data quality

See **[RESPONSE_DATA_QUALITY.md](RESPONSE_DATA_QUALITY.md)** for a per-endpoint summary of whether responses contain useful, parsable data, and any caveats (e.g. GetBeachRoundRanking only returns data for pool rounds; GetEvent needs a valid event No).

## Notes

- **Old-style wrapper**: Several endpoints return `NotInNewFormat` (400) unless the request is wrapped in `<Requests><Request ... /></Requests>`. These use `old_style=True` in the client: **GetBeachTournamentRanking**, **GetPlayer**, **GetBeachWorldTourRanking**, **GetBeachOlympicSelectionRanking**.
- **GetBeachMatchList**: The filter is optional. With no filter, the API returns **all beach matches** (one request); the ETL uses this for bulk loading. With `Filter NoTournament='502'` you get matches for a single tournament.
- **GetBeachRoundList** requires the filter as a **child element**, not an attribute: `<Request Type="GetBeachRoundList" Fields="..."><Filter NoTournament="502"/></Request>`, and must be wrapped in `<Requests>`.
- **GetBeachWorldTourRanking** and **GetBeachOlympicSelectionRanking** require the **Gender** parameter (e.g. `M` or `W` per [PersonGender](https://www.fivb.org/VisSDK/VisWebService/PersonGender.html)).
- **GetBeachTournamentRanking** is requested with `accept_json=False` because the service returns XML for this call.
- Response bodies are truncated in the JSON for readability; full payloads can be captured by modifying `MAX_RESPONSE_TEXT_IN_JSON` in `scripts/explore_vis_api.py` or by saving `response_text` to a separate file.
