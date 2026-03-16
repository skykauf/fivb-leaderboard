{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'elo'],
    )
}}
-- Flat Elo history: one row per player per match with player and tournament context.
select
    e.player_id,
    p.full_name as player_name,
    p.country_code as player_country_code,
    p.height_inches as player_height_inches,
    e.gender,
    e.as_of_date,
    e.match_id,
    m.tournament_id as tournament_id,
    dt.name as tournament_name,
    dt.season as tournament_season,
    e.elo_rating
from {{ source('elo', 'player_elo_history') }} as e
left join {{ ref('stg_fivb_players') }} as p on p.player_id = e.player_id
left join {{ ref('stg_fivb_matches') }} as m on m.match_id = e.match_id
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
