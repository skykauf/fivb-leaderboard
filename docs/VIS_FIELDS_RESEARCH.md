# VIS Web Service: Additional Fields Research

Research on VIS entity fields we do **not** currently ingest (or only keep in `payload`), based on the official docs. No code changes—reference only.

**Sources:** [BeachTournament](https://www.fivb.org/VisSDK/VisWebService/BeachTournament.html), [BeachMatch](https://www.fivb.org/VisSDK/VisWebService/BeachMatch.html), [BeachTeam](https://www.fivb.org/VisSDK/VisWebService/BeachTeam.html), [Player](https://www.fivb.org/VisSDK/VisWebService/Player.html), [BeachRound](https://www.fivb.org/VisSDK/VisWebService/BeachRound.html), [Event](https://www.fivb.org/VisSDK/VisWebService/Event.html).

---

## 1. BeachTournament

**Currently requested:** No, Name, Code, CountryCode, CountryName, City, StartDate, EndDate, Season, Gender, Type, Status, Timezone, NoEvent, Title, Version, StartDateMainDraw, EndDateMainDraw, StartDateQualification, EndDateQualification, NbTeamsMainDraw, NbTeamsQualification, NbTeamsFromQualification.

**Currently normalized:** tournament_id, name, season, tier, start_date, end_date (incl. from EndDateMainDraw), city (incl. inferred from Name), country_code, country_name, gender, status, timezone.

| Field | VIS description | Use case |
|-------|-----------------|----------|
| **Deadline** | Deadline date for team registration | Display / eligibility logic |
| **PrizeMoney** | Prize money structure for the tournament (complex type) | Prize analytics; may need separate parsing |
| **Earnings** | Total earnings for the tournament | Tournament-level financial summary |
| **EarningsBonus** | Total bonus earnings | Same |
| **DefaultLocalTimeOffset** | Local time offset in minutes; doc says *"currently not used"* | Skip unless VIS starts populating |
| **OrganizerCode** / **OrganizerType** | Who organizes (confederation/federation code) | Filtering / grouping by organizer |

**Recommendation:** Add **Deadline** if you care about registration cutoffs. **PrizeMoney** / **Earnings** only if you need tournament-level prize analytics (and accept complex/optional response). **OrganizerCode** is low-effort and useful for “who runs this event.”

---

## 2. BeachMatch

**Currently requested:** No, NoTournament, NoRound, NoTeamA/B, NoInTournament, MatchPointsA/B, DateTimeLocal, LocalDate, LocalTime, ResultType, Status, Phase, NoPlayerA1/A2, NoPlayerB1/B2, TeamAName, TeamBName, PointsTeamA/B Set1–3, DurationSet1–3, Court, RoundCode, RoundName, TournamentCode/Name, WinnerRank, LoserRank, BeginDateTimeUtc.

**Currently normalized:** match_id, tournament_id, phase, round (NoRound or RoundCode), team1/2_id, winner_team_id, score_sets, duration_minutes, played_at, result_type, status. Court and set-level details are in `payload` only.

| Field | VIS description | Use case |
|-------|-----------------|----------|
| **RoundPhase** | Phase of the round (e.g. pool vs elimination) | Replaces match.phase (often null); better “pool vs bracket” flag |
| **Court** | Name of the court | Already requested; consider promoting to a column for “court utilization” or schedule views |
| **TeamAPositionInMainDraw** / **TeamBPositionInMainDraw** | Main-draw seed/position | Seeding and “upset” analysis |
| **TeamAPositionInQualification** / **TeamBPositionInQualification** | Qualifier position | Same for qualifier draws |
| **Referee1Name**, **Referee2Name**, **Referee1FederationCode**, **Referee2FederationCode** | Referee identity | Referee stats / assignment analysis |
| **Format** (BeachMatchFormat) | Match format | Filter or label (e.g. best-of-3) |
| **NbSpectators** | Spectator count | Engagement / venue analytics |
| **FastestServeTeamAPlayer1/2**, **FastestServeTeamBPlayer1/2** (Speed) | Serve speed | Performance stats (if populated) |
| **PointsTeamA/B Set4, Set5**, **DurationSet4, DurationSet5** | Extra sets | Long matches; we only use sets 1–3 today |
| **TeamAType** / **TeamBType** | e.g. bye, unknown | Interpret NoTeamA/B = 0 / -1 |
| **EndDateTimeUtc** | Match end time | Doc says *"currently not used"* — skip unless VIS uses it |
| **LocalTimeOffset** | Match local offset | Doc says *"currently not used"* — skip |

**Recommendation:** **RoundPhase** is high value (reliable phase when match.phase is null). **Court** is easy to add as a column. **TeamA/B PositionInMainDraw/Qualification** and **Referee*** are good for analytics if you need them. **Format**, **NbSpectators**, and **FastestServe*** are optional and depend on how often the API fills them.

---

## 3. BeachTeam

**Currently requested:** No, NoTournament, NoPlayer1/2, Name, CountryCode, ConfederationCode, Status, Rank, EarnedPointsTeam, EarnedPointsPlayer, PositionInMainDraw, PositionInQualification, IsInMainDraw, IsInQualification, TournamentName/Title/Type, ValidFrom, ValidTo.

**Currently normalized:** team_id, tournament_id, player_a_id, player_b_id, country_code, status, valid_from, valid_to. Name and rank live in `payload` only.

| Field | VIS description | Use case |
|-------|-----------------|----------|
| **Name** | Team name (e.g. “Smith / Jones”) | Already in payload; consider a column for display/joining when NoTeam is 0 in rankings |
| **Rank** | Tournament rank | Already requested; column useful for “final standing” without joining results |
| **EarningsTeam**, **EarningsPlayer**, **EarningsBonusTeam**, **EarningsTotalTeam** | Prize money at team/player level | Finances per team per tournament |
| **EntryPoints1/2**, **QualificationPoints1/2**, **TechnicalPoints1/2** | Entry/qualification/technical points | Seeding and draw logic |
| **MainDrawSeed1/2** | Main-draw seed index | Seeding analysis |
| **PositionInEntry**, **PositionInDispatch** | Entry list order / dispatch order | Draw and registration analytics |
| **IsInConfederationQuota**, **IsInFederationQuota**, **IsInReserve** | Quota / reserve flags | Filtering and fairness analysis |
| **Player1/2 FirstName, LastName, Birthdate, Height, Weight, BeachPosition** (etc.) | Denormalized player info on team | Avoid extra Player lookups for team cards; heavy payload |
| **NoShirt1/2** | Jersey numbers | Display only |
| **StatusDate**, **StatusText** | When/why status changed | Audit / debugging |
| **TournamentEndDateMainDraw** | Tournament main-draw end date | Redundant if we have it on tournament |
| **Type** (BeachTeamType) | Team type | Interpret special teams |

**Recommendation:** **Name** and **Rank** as columns help when joining or displaying teams. **Earnings*** and **EntryPoints*** / **MainDrawSeed*** are useful for prize and seeding analytics. Denormalized player fields (Player1FirstName, etc.) are optional and increase payload size.

---

## 4. Player

**Currently requested (GetPlayerList):** No, FirstName, LastName, BirthDate, BirthPlace, Height, Weight, CountryCode, Gender, FederationCode, NationalityCode, PlaysBeach, PlaysVolley, TeamName, ConfederationCode.

**Currently normalized:** player_id, first_name, last_name, full_name, gender, birth_date, height_cm (raw), height_inches (staging, from height_cm), country_code (from FederationCode), profile_url (always null).

| Field | VIS description | Use case |
|-------|-----------------|----------|
| **WebSite**, **FacebookUrl**, **InstagramUri**, **TwitterUri** | Social / profile URLs | Profile links; **profile_url** could map to WebSite or first available |
| **Profile** | Text profile | Bio / display |
| **PopularName** | Nickname / display name | Display |
| **ActiveBeach**, **ActiveVolley**, **ActiveSnow** | Activity flags | Filter active players |
| **BeachPosition** (PlayerBeachPosition) | Court position (e.g. blocker, defender) | Role in models or filters |
| **Handedness** | Left/right | Analytics or display |
| **BirthCountryCode**, **BirthCountryName** | Birth country | Demographics; different from federation |
| **Nationality** (vs NationalityCode) | Human-readable nationality | Display |
| **BeachHighBlock**, **BeachHighJump**, **BeachHighSpike**, **BeachHighStand** | Physical stats (beach) | Performance / scouting |
| **BeachNbSelOG**, **BeachNbSelWC**, **BeachNbSelOther** | Olympic / World Champs / other selections | Experience and “pedigree” |
| **BeachCurrentTeam**, **BeachFirstEvent**, **BeachYearBegin** | Career context | Narrative / display |
| **NoPhoto** | Official portrait number | Doc says *"currently not used"* — skip for now |

**Recommendation:** **WebSite** (or one social URL) is the best candidate to back a real **profile_url**. **ActiveBeach** and **BeachPosition** are small and useful for filters. **PopularName**, **BirthCountryCode**, **Handedness** are nice-to-have. Physical and selection stats are optional and may be sparsely populated.

---

## 5. BeachRound

**Currently requested:** No, NoTournament, NoInTournament, Code, Name, Bracket, Phase, StartDate, EndDate, Version, RankMethod.

**Currently normalized:** round_id, tournament_id, code, name, bracket, phase, start_date, end_date, rank_method.

| Field | VIS description | Use case |
|-------|-----------------|----------|
| **RankingInformation** | Text for tie-breaks / special cases | Display next to pool standings |
| **Version** | Doc says *"currently not used"* | Skip |
| **DeletedDT**, **LastChangeDT** | Audit | Rarely needed |

**Recommendation:** **RankingInformation** is useful if you show pool rankings and want to explain ties; otherwise low priority.

---

## 6. Event

**Currently requested:** No, Code, Name, StartDate, EndDate, Type, Version, NoParentEvent, CountryCode, HasBeachTournament, HasMenTournament, HasWomenTournament, IsVisManaged.

**Currently normalized:** event_id, code, name, start_date, end_date, type, no_parent_event, country_code, has_beach_tournament, has_men_tournament, has_women_tournament, is_vis_managed.

| Field | VIS description | Use case |
|-------|-----------------|----------|
| **AccreditationStartDate** / **AccreditationEndDate** | Accreditation window | Operational |
| **OrganizerCode** / **OrganizerType** | Organizer (confederation/federation) | Grouping; align with BeachTournament if added there |
| **Content** | Nested events/tournaments (XML) | Drill-down; complex to parse |
| **Venues** | List of venues (XML) | Venue-level analytics |
| **InfoLocation**, **InfoSchedule**, **InfoFormat** (etc.) | HTML fragments | Rich event pages; heavy |

**Recommendation:** **OrganizerCode** / **OrganizerType** are small and consistent with tournament-level organizer. **Venues** only if you need venue dimension. Info* fields are for presentation, not analytics.

---

## 7. Tournament ranking (GetBeachTournamentRanking)

We already request **Rank, Position, NoTeam, TeamName, TeamFederationCode, EarnedPointsTeam, EarningsTotalTeam**. The BeachTournamentRankingEntry type in the docs aligns with this; no critical missing fields identified.

---

## 8. World Tour / Olympic ranking (GetBeachWorldTourRanking, GetBeachOlympicSelectionRanking)

We already request the main ranking fields plus **NoPlayer1/2**, **NbParticipations**, **Earnings***, **HasTournamentsRemoved**, **SelectionRank**, **GamesYear** where applicable. No high-value missing fields noted.

---

## Summary: high-value candidates

| Priority | Entity | Field(s) | Reason |
|----------|--------|----------|--------|
| High | BeachMatch | **RoundPhase** | Reliable phase (match.phase often null) |
| High | Player | **WebSite** (or social URL) | Populate **profile_url** |
| Medium | BeachMatch | **Court** | Already in request; add as column for schedule/venue use |
| Medium | BeachTeam | **Name**, **Rank** | Display and joins when NoTeam=0 in rankings |
| Medium | BeachTournament | **Deadline**, **OrganizerCode** | Registration and organizer grouping |
| Medium | Player | **ActiveBeach**, **BeachPosition** | Filtering and role |
| Lower | BeachMatch | **TeamA/B PositionInMainDraw/Qualification**, **Referee*** | Seeding and referee analytics |
| Lower | BeachTeam | **Earnings***, **EntryPoints***, **MainDrawSeed*** | Prize and seeding |
| Lower | Player | **PopularName**, **BirthCountryCode**, **Handedness** | Display and demographics |
| Lower | BeachRound | **RankingInformation** | Tie-break text for pool standings |

All of these are already requestable via the Fields parameter; the main work is adding them to **DEFAULT_FIELDS**, then (optionally) normalizing into columns and documenting in staging schema and **RESPONSE_DATA_QUALITY.md**. Before adding, confirm with a sample response that the API actually returns the field for your filters (e.g. season, tournament type).
