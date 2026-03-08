{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'elo'],
    )
}}
-- Latest Elo rating per player per gender (current rating for joins / leaderboards).
with ranked as (
    select
        e.player_id,
        p.full_name as player_name,
        e.gender,
        e.elo_rating,
        e.as_of_date as elo_as_of_date,
        e.match_id as last_match_id,
        row_number() over (partition by e.player_id, e.gender order by e.as_of_date desc, e.match_id desc) as rn
    from {{ source('elo', 'player_elo_history') }} as e
    left join {{ ref('stg_fivb_players') }} as p on p.player_id = e.player_id
)
select player_id, player_name, gender, elo_rating, elo_as_of_date, last_match_id
from ranked
where rn = 1
