{{ config(
    materialized='view'
) }}

with source as (

    select
        *
    from {{ source('raw_fivb', 'tournaments') }}

),
renamed as (

    select
        cast(tournament_id as bigint)    as fivb_tournament_id,
        name,
        season,
        tier,
        start_date,
        end_date,
        city,
        country_code                     as country,
        country_name,
        gender,
        status,
        timezone,
        ingested_at
    from source

),
deduped as (

    select
        *,
        row_number() over (
            partition by fivb_tournament_id
            order by ingested_at desc
        ) as _rn
    from renamed

)

select
    fivb_tournament_id,
    name,
    season,
    tier,
    start_date,
    end_date,
    city,
    country,
    country_name,
    gender,
    status,
    timezone,
    ingested_at
from deduped
where _rn = 1

