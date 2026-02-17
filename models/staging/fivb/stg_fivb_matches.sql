{{ config(
    materialized='view'
) }}

with source as (

    select
        *
    from {{ source('raw_fivb', 'matches') }}

),
renamed as (

    select
        cast(match_id as bigint)         as fivb_match_id,
        cast(tournament_id as bigint)    as fivb_tournament_id,
        phase,
        round,
        cast(team1_id as bigint)         as fivb_team1_id,
        cast(team2_id as bigint)         as fivb_team2_id,
        cast(winner_team_id as bigint)   as fivb_winner_team_id,
        score_sets,
        duration_minutes                 as duration_min,
        played_at,
        result_type,
        status,
        ingested_at
    from source

),
deduped as (

    select
        *,
        row_number() over (
            partition by fivb_match_id
            order by ingested_at desc
        ) as _rn
    from renamed

)

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
from deduped
where _rn = 1

