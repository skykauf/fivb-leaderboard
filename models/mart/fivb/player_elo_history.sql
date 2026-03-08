{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'elo'],
    )
}}
-- Player Elo history with names; built on top of core.player_elo_history (from Python Elo script).
select
    e.player_id,
    p.full_name as player_name,
    e.gender,
    e.as_of_date,
    e.match_id,
    e.elo_rating
from {{ source('elo', 'player_elo_history') }} as e
left join {{ ref('stg_fivb_players') }} as p on p.player_id = e.player_id
