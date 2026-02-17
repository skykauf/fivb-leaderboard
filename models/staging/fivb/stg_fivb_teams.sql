{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    team_id,
    tournament_id,
    player_a_id,
    player_b_id,
    country_code,
    status,
    valid_from,
    valid_to,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_teams') }}
