{{
  config(
    materialized='view'
  )
}}

{#
  Rankings are built from GetBeachTournamentRanking (raw_fivb_results), not from
  the deprecated GetBeachWorldTourRanking. Each tournament result (tournament_id, team_id,
  finishing_pos, points) is joined to teams to get both players; each player gets a row
  with ranking_type='tournament', snapshot_date=tournament end_date, rank=finishing_pos.
  One row per (ranking_type, snapshot_date, fivb_player_id) — best rank kept when
  a player has multiple results on the same date.
#}

with results as (
  select
    cast(r.tournament_id as bigint)   as tournament_id,
    cast(r.team_id as bigint)        as team_id,
    r.finishing_pos,
    r.points,
    r.ingested_at
  from {{ source('raw_fivb', 'results') }} r
),

tournaments as (
  select
    cast(tournament_id as bigint)    as tournament_id,
    coalesce(end_date, start_date)    as snapshot_date
  from {{ source('raw_fivb', 'tournaments') }}
),

teams as (
  select
    cast(team_id as bigint)          as team_id,
    cast(tournament_id as bigint)    as tournament_id,
    cast(player_a_id as bigint)      as player_a_id,
    cast(player_b_id as bigint)      as player_b_id
  from {{ source('raw_fivb', 'teams') }}
),

-- One row per player per result (each team result becomes two rows)
player_results as (
  select
    'tournament'                     as ranking_type,
    t.snapshot_date,
    cast(tm.player_a_id as bigint)   as fivb_player_id,
    r.finishing_pos                  as rank,
    r.points,
    r.ingested_at
  from results r
  join tournaments t on r.tournament_id = t.tournament_id
  join teams tm on r.team_id = tm.team_id and r.tournament_id = tm.tournament_id
  where tm.player_a_id is not null

  union all

  select
    'tournament',
    t.snapshot_date,
    cast(tm.player_b_id as bigint),
    r.finishing_pos,
    r.points,
    r.ingested_at
  from results r
  join tournaments t on r.tournament_id = t.tournament_id
  join teams tm on r.team_id = tm.team_id and r.tournament_id = tm.tournament_id
  where tm.player_b_id is not null
),

deduped as (
  select
    ranking_type,
    snapshot_date,
    fivb_player_id,
    rank,
    points,
    ingested_at,
    row_number() over (
      partition by ranking_type, snapshot_date, fivb_player_id
      order by rank asc nulls last, ingested_at desc
    ) as _rn
  from player_results
)

select
  ranking_type,
  snapshot_date,
  fivb_player_id,
  rank,
  points,
  ingested_at
from deduped
where _rn = 1
