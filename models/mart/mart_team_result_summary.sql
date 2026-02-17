{{
  config(
    materialized='table',
    tags=['mart']
  )
}}

with results as (
  select
    tournament_id,
    team_id,
    finishing_pos,
    points,
    prize_money
  from {{ ref('fact_result') }}
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
    country as team_country,
    status  as team_status,
    row_number() over (
      partition by fivb_team_id, fivb_tournament_id
      order by ingested_at desc
    ) as _rn
  from {{ ref('stg_fivb_teams') }}
),

teams as (
  select
    fivb_team_id,
    fivb_tournament_id,
    fivb_player_a_id,
    fivb_player_b_id,
    team_country,
    team_status
  from teams_deduped
  where _rn = 1
),

players as (
  select
    fivb_player_id,
    first_name,
    last_name,
    full_name,
    gender   as player_gender,
    birth_date,
    height_cm,
    country  as player_country,
    profile_url
  from {{ ref('stg_fivb_players') }}
)

select
  r.tournament_id,
  r.team_id,
  r.finishing_pos,
  r.points       as result_points,
  r.prize_money,
  -- Tournament metadata
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
  -- Team metadata
  tm.team_country,
  tm.team_status,
  tm.fivb_player_a_id as player_a_id,
  tm.fivb_player_b_id as player_b_id,
  pa.full_name        as player_a_name,
  pa.player_country   as player_a_country,
  pa.player_gender    as player_a_gender,
  pa.birth_date       as player_a_birth_date,
  pa.height_cm        as player_a_height_cm,
  pa.profile_url      as player_a_profile_url,
  pb.full_name        as player_b_name,
  pb.player_country   as player_b_country,
  pb.player_gender    as player_b_gender,
  pb.birth_date       as player_b_birth_date,
  pb.height_cm        as player_b_height_cm,
  pb.profile_url      as player_b_profile_url
from results r
left join tournaments t
  on t.fivb_tournament_id = r.tournament_id
left join teams tm
  on tm.fivb_team_id = r.team_id
 and tm.fivb_tournament_id = r.tournament_id
left join players pa on pa.fivb_player_id = tm.fivb_player_a_id
left join players pb on pb.fivb_player_id = tm.fivb_player_b_id
