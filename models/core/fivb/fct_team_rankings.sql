{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'fact'],
    )
}}
select
    tr.ranking_type,
    tr.snapshot_date,
    tr.gender,
    tr.position,
    tr.no_player1,
    tr.no_player2,
    tr.team_name,
    p1.full_name as player1_name,
    p2.full_name as player2_name,
    coalesce(p1.country_code, p2.country_code) as country_code,
    -- derived
    (tr.position <= 10) as is_top_10,
    (tr.position <= 16) as is_top_16
from {{ ref('stg_fivb_team_rankings') }} as tr
left join {{ ref('stg_fivb_players') }} as p1 on p1.player_id = tr.no_player1
left join {{ ref('stg_fivb_players') }} as p2 on p2.player_id = tr.no_player2
