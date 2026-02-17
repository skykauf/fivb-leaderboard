{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    ranking_type,
    snapshot_date,
    gender,
    position,
    no_player1,
    no_player2,
    team_name,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_team_rankings') }}
