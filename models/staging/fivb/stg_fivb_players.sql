{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    player_id,
    first_name,
    last_name,
    full_name,
    gender,
    birth_date,
    height_cm,
    country_code,
    profile_url,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_players') }}
