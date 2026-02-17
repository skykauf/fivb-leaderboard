{{ config(
    materialized='table'
) }}

with players as (

    select
        fivb_player_id,
        first_name,
        last_name,
        full_name,
        gender,
        birth_date,
        height_cm,
        country,
        profile_url,
        ingested_at
    from {{ ref('stg_fivb_players') }}

),
deduped as (

    select
        fivb_player_id,
        first_name,
        last_name,
        full_name,
        gender,
        birth_date,
        height_cm,
        country,
        profile_url,
        row_number() over (partition by fivb_player_id order by ingested_at desc) as _rn
    from players

)

select
    fivb_player_id   as player_id,
    fivb_player_id   as fivb_player_id,
    first_name,
    last_name,
    full_name,
    gender,
    birth_date,
    height_cm,
    country          as primary_country,
    profile_url
from deduped
where _rn = 1

