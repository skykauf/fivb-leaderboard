{{ config(
    materialized='table'
) }}

with matches as (

    select
        fivb_match_id,
        fivb_tournament_id,
        phase,
        round,
        fivb_team1_id,
        fivb_team2_id,
        fivb_winner_team_id,
        score_sets,
        duration_min,
        played_at,
        result_type,
        status,
        ingested_at
    from {{ ref('stg_fivb_matches') }}

),
deduped as (

    select
        fivb_match_id,
        fivb_tournament_id,
        phase,
        round,
        fivb_team1_id,
        fivb_team2_id,
        fivb_winner_team_id,
        score_sets,
        duration_min,
        played_at,
        result_type,
        status,
        row_number() over (partition by fivb_match_id order by ingested_at desc) as _rn
    from matches

)

select
    fivb_match_id        as match_id,
    fivb_tournament_id   as tournament_id,
    phase,
    round,
    fivb_team1_id        as team1_id,
    fivb_team2_id        as team2_id,
    fivb_winner_team_id  as winner_team_id,
    score_sets,
    duration_min,
    played_at,
    result_type,
    status
from deduped
where _rn = 1

