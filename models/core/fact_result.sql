{{ config(
    materialized='table'
) }}

with results as (

    select
        cast(tournament_id as bigint)    as fivb_tournament_id,
        cast(team_id as bigint)          as fivb_team_id,
        finishing_pos,
        points,
        prize_money,
        ingested_at
    from {{ source('raw_fivb', 'results') }}

),
deduped as (

    select
        fivb_tournament_id,
        fivb_team_id,
        finishing_pos,
        points,
        prize_money,
        row_number() over (
            partition by fivb_tournament_id, fivb_team_id
            order by ingested_at desc
        ) as _rn
    from results

)

select
    fivb_tournament_id   as tournament_id,
    fivb_team_id         as team_id,
    finishing_pos,
    points,
    prize_money
from deduped
where _rn = 1

