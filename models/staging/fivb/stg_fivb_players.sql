{{ config(
    materialized='view'
) }}

with source as (

    select
        *
    from {{ source('raw_fivb', 'players') }}

),
renamed as (

    select
        cast(player_id as bigint)        as fivb_player_id,
        first_name,
        last_name,
        full_name,
        gender,
        birth_date,
        height_cm,
        country_code           as country,
        profile_url,
        ingested_at
    from source

),
deduped as (

    select
        *,
        row_number() over (
            partition by fivb_player_id
            order by ingested_at desc
        ) as _rn
    from renamed

)

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
from deduped
where _rn = 1

