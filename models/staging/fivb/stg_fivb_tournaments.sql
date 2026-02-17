{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    tournament_id,
    name,
    season,
    tier,
    start_date,
    end_date,
    city,
    country_code,
    country_name,
    gender,
    status,
    timezone,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_tournaments') }}
