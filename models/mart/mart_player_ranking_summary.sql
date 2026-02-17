{{
  config(
    materialized='table',
    tags=['mart']
  )
}}

with rankings as (
  select
    ranking_type,
    snapshot_date,
    fivb_player_id,
    rank,
    points,
    ingested_at
  from {{ ref('stg_fivb_rankings') }}
),

rankings_deduped as (
  select
    ranking_type,
    snapshot_date,
    fivb_player_id,
    rank,
    points,
    row_number() over (
      partition by ranking_type, snapshot_date, fivb_player_id
      order by ingested_at desc
    ) as _rn
  from rankings
),

players as (
  select
    fivb_player_id,
    first_name,
    last_name,
    full_name,
    gender,
    birth_date,
    height_cm,
    country  as player_country,
    profile_url
  from {{ ref('stg_fivb_players') }}
)

select
  r.ranking_type,
  r.snapshot_date,
  r.fivb_player_id,
  r.rank,
  r.points,
  -- Player metadata
  p.first_name,
  p.last_name,
  p.full_name,
  p.gender         as player_gender,
  p.birth_date     as player_birth_date,
  p.height_cm      as player_height_cm,
  p.player_country,
  p.profile_url    as player_profile_url
from rankings_deduped r
left join players p on p.fivb_player_id = r.fivb_player_id
where r._rn = 1
