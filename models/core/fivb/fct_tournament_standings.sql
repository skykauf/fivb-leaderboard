{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'fact'],
    )
}}
select
    r.tournament_id,
    r.team_id,
    r.finishing_pos,
    r.points,
    r.prize_money,
    dt.name as tournament_name,
    dt.season,
    dt.gender as tournament_gender,
    dt.tier as tournament_tier,
    dt.is_major,
    dtt.team_display_name,
    dtt.player_a_name,
    dtt.player_b_name,
    dtt.country_code as team_country_code,
    -- derived
    (r.finishing_pos <= 3) as is_podium,
    (r.finishing_pos = 1) as is_champion
from {{ ref('stg_fivb_results') }} as r
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = r.tournament_id
left join {{ ref('dim_team_tournaments') }} as dtt
    on dtt.team_id = r.team_id and dtt.tournament_id = r.tournament_id
