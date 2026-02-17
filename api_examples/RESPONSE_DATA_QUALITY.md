# VIS endpoint response data quality

Summary of whether each endpoint returns **useful, parsable data** in the saved samples. Re-run `python3 scripts/explore_vis_api.py` to refresh samples.

## Fully useful and parsable (14)

| Endpoint | Notes |
|----------|--------|
| **GetBeachTournamentList** | List of tournaments; JSON/XML with No, Name, CountryCode, StartDate, Season, etc. |
| **GetBeachTournament** | Single tournament; same fields. |
| **GetBeachTeamList** | Teams with No, NoTournament, NoPlayer1, NoPlayer2, CountryCode. |
| **GetBeachMatchList** | Matches with No, NoTournament, NoRound, NoTeamA/B, MatchPoints, etc. |
| **GetBeachMatch** | Single match; full match details. |
| **GetBeachRoundList** | Rounds with Code, Name, Bracket, Phase, StartDate, EndDate, No. |
| **GetBeachRound** | Single round; same fields. |
| **GetPlayerList** | Players; No, FirstName, LastName, BirthDate, Height, Gender. |
| **GetPlayer** | Single player; FederationCode, FirstName, LastName, etc. |
| **GetBeachWorldTourRanking** | Ranking entries; Position, Rank, TeamName, TeamFederationCode, EarnedPointsTeam, etc. |
| **GetBeachOlympicSelectionRanking** | Position, TeamName, TeamCountryCode, Points, SelectionRank, etc. |
| **GetBeachTeam** | Single team; No, NoTournament, NoPlayer1, NoPlayer2, CountryCode. |
| **GetEventList** | Events (Code, Name, StartDate, EndDate, No); Filter by HasBeachTournament, dates. |
| **GetEvent** | Single event; same fields. *Requires valid event No (e.g. from GetEventList); No=0 returns NoData.* |

## Conditional or partial data (2)

| Endpoint | Issue | Recommendation |
|----------|--------|-----------------|
| **GetBeachTournamentRanking** | Some tournaments return **NoTeam=0** for all entries (e.g. test events). Position and Rank are still valid. | Use for finishing positions; join to teams by other means when NoTeam is 0, or use tournaments known to have NoTeam populated. |
| **GetBeachRoundRanking** | Returns **InternalServerError NotARankingRound** for elimination rounds (no pool standings). Only rounds that have a ranking (e.g. pool rounds Code PA, PB, PC, PD) return data. | Use round No from a pool round (from GetBeachRoundList). The exploration script now prefers a pool round when available. |

## Parsing notes

- **JSON responses**: Keys are camelCase; the client normalizes to PascalCase in `parsed_sample`.
- **XML responses**: Attributes and child elements become record keys; single-element responses (GetPlayer, GetBeachRound, GetEvent, etc.) produce one record.
- **Error responses**: When the API returns `<NoData>`, `<InternalServerError>`, or similar, `parsed_record_count` is 0 and `parsed_sample` is empty; `response_text` still contains the raw body for debugging.
