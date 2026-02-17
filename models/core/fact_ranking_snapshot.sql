{{ config(
    materialized='table'
) }}

with rankings as (

    select
        ranking_type,
        snapshot_date,
        fivb_player_id,
        rank,
        points,
        ingested_at
    from {{ ref('stg_fivb_rankings') }}

),
deduped as (

    select
        ranking_type,
        snapshot_date,
        fivb_player_id,
        rank,
        points,
        row_number() over (
            partition by ranking_type, snapshot_date, fivb_player_id
            order by ingested_at desc
        ) as _rn
    from rankings

)

select
    ranking_type,
    snapshot_date,
    fivb_player_id   as player_id,
    fivb_player_id   as fivb_player_id,
    rank,
    points
from deduped
where _rn = 1

