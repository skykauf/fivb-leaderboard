{{ config(
    materialized='table'
) }}

with tournaments as (

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
    from {{ ref('stg_fivb_tournaments') }}

),
deduped as (

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
        row_number() over (partition by fivb_tournament_id order by ingested_at desc) as _rn
    from tournaments

)

select
    fivb_tournament_id  as tournament_id,
    fivb_tournament_id  as fivb_tournament_id,
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
    timezone
from deduped
where _rn = 1

