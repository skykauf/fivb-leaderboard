{{ config(
    materialized='table'
) }}

with teams as (

    select
        fivb_team_id,
        fivb_tournament_id,
        fivb_player_a_id,
        fivb_player_b_id,
        country,
        status,
        valid_from,
        valid_to,
        ingested_at
    from {{ ref('stg_fivb_teams') }}

),
deduped as (

    select
        fivb_team_id,
        fivb_tournament_id,
        fivb_player_a_id,
        fivb_player_b_id,
        country,
        status,
        valid_from,
        valid_to,
        row_number() over (partition by fivb_team_id order by ingested_at desc) as _rn
    from teams

)

select
    fivb_team_id        as team_id,
    fivb_team_id        as fivb_team_id,
    fivb_tournament_id  as tournament_id,
    fivb_player_a_id    as player_a_id,
    fivb_player_b_id    as player_b_id,
    country,
    status,
    valid_from,
    valid_to
from deduped
where _rn = 1

