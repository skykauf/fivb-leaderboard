{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    tournament_id,
    team_id,
    finishing_pos,
    points,
    prize_money,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_results') }}
