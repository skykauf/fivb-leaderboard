{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    match_id,
    tournament_id,
    phase,
    round as round_code,
    team1_id,
    team2_id,
    winner_team_id,
    score_sets,
    duration_minutes,
    played_at,
    result_type,
    status,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_matches') }}
