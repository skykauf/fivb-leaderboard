{{
  config(
    materialized='table',
    tags=['mart']
  )
}}

with matches as (
  select
    m.fivb_match_id,
    m.fivb_tournament_id,
    m.phase,
    m.round,
    m.fivb_team1_id,
    m.fivb_team2_id,
    m.fivb_winner_team_id,
    m.score_sets,
    m.duration_min,
    m.played_at,
    m.result_type,
    m.status as match_status
  from {{ ref('stg_fivb_matches') }} m
),

tournaments as (
  select
    fivb_tournament_id,
    name            as tournament_name,
    season          as tournament_season,
    tier            as tournament_tier,
    start_date      as tournament_start_date,
    end_date        as tournament_end_date,
    city            as tournament_city,
    country         as tournament_country,
    country_name    as tournament_country_name,
    gender          as tournament_gender,
    status          as tournament_status,
    timezone        as tournament_timezone
  from {{ ref('stg_fivb_tournaments') }}
),

teams_deduped as (
  select
    fivb_team_id,
    fivb_tournament_id,
    fivb_player_a_id,
    fivb_player_b_id,
    country         as team_country,
    status          as team_status,
    row_number() over (
      partition by fivb_team_id, fivb_tournament_id
      order by ingested_at desc
    ) as _rn
  from {{ ref('stg_fivb_teams') }}
),

team1 as (
  select
    fivb_team_id,
    fivb_tournament_id,
    fivb_player_a_id,
    fivb_player_b_id,
    team_country
  from teams_deduped
  where _rn = 1
),

team2 as (
  select
    fivb_team_id,
    fivb_tournament_id,
    fivb_player_a_id,
    fivb_player_b_id,
    team_country
  from teams_deduped
  where _rn = 1
),

players as (
  select
    fivb_player_id,
    first_name,
    last_name,
    full_name,
    gender         as player_gender,
    birth_date,
    height_cm,
    country        as player_country,
    profile_url
  from {{ ref('stg_fivb_players') }}
),

base as (
  select
    m.fivb_match_id,
    m.fivb_tournament_id,
    m.phase,
    m.round,
    m.fivb_team1_id,
    m.fivb_team2_id,
    m.fivb_winner_team_id,
    m.score_sets,
    m.duration_min,
    m.played_at,
    m.result_type,
    m.match_status,
    t.tournament_name,
    t.tournament_season,
    t.tournament_tier,
    t.tournament_start_date,
    t.tournament_end_date,
    t.tournament_city,
    t.tournament_country,
    t.tournament_country_name,
    t.tournament_gender,
    t.tournament_status,
    t.tournament_timezone,
    t1.fivb_player_a_id as team1_player_a_id,
    t1.fivb_player_b_id as team1_player_b_id,
    t1.team_country     as team1_country,
    t2.fivb_player_a_id as team2_player_a_id,
    t2.fivb_player_b_id as team2_player_b_id,
    t2.team_country     as team2_country
  from matches m
  left join tournaments t
    on t.fivb_tournament_id = m.fivb_tournament_id
  left join team1 t1
    on t1.fivb_team_id = m.fivb_team1_id
   and t1.fivb_tournament_id = m.fivb_tournament_id
  left join team2 t2
    on t2.fivb_team_id = m.fivb_team2_id
   and t2.fivb_tournament_id = m.fivb_tournament_id
)

select
  b.fivb_match_id,
  b.fivb_tournament_id,
  b.phase,
  b.round,
  b.played_at,
  b.score_sets,
  b.duration_min,
  b.result_type,
  b.match_status,
  b.fivb_winner_team_id,
  -- Tournament metadata
  b.tournament_name,
  b.tournament_season,
  b.tournament_tier,
  b.tournament_start_date,
  b.tournament_end_date,
  b.tournament_city,
  b.tournament_country,
  b.tournament_country_name,
  b.tournament_gender,
  b.tournament_status,
  b.tournament_timezone,
  -- Team 1
  b.fivb_team1_id,
  b.team1_country,
  b.team1_player_a_id,
  b.team1_player_b_id,
  p1a.full_name       as team1_player_a_name,
  p1a.player_country  as team1_player_a_country,
  p1b.full_name       as team1_player_b_name,
  p1b.player_country  as team1_player_b_country,
  -- Team 2
  b.fivb_team2_id,
  b.team2_country,
  b.team2_player_a_id,
  b.team2_player_b_id,
  p2a.full_name       as team2_player_a_name,
  p2a.player_country  as team2_player_a_country,
  p2b.full_name       as team2_player_b_name,
  p2b.player_country  as team2_player_b_country
from base b
left join players p1a on p1a.fivb_player_id = b.team1_player_a_id
left join players p1b on p1b.fivb_player_id = b.team1_player_b_id
left join players p2a on p2a.fivb_player_id = b.team2_player_a_id
left join players p2b on p2b.fivb_player_id = b.team2_player_b_id
