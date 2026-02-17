{{ config(
    materialized='view'
) }}

with source as (

    select
        *
    from {{ source('raw_fivb', 'teams') }}

),
renamed as (

    select
        cast(team_id as bigint)          as fivb_team_id,
        cast(tournament_id as bigint)    as fivb_tournament_id,
        cast(player_a_id as bigint)      as fivb_player_a_id,
        cast(player_b_id as bigint)      as fivb_player_b_id,
        country_code                     as country,
        status,
        valid_from,
        valid_to,
        ingested_at
    from source

),
deduped as (

    select
        *,
        row_number() over (
            partition by fivb_team_id
            order by ingested_at desc
        ) as _rn
    from renamed

)

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
from deduped
where _rn = 1

