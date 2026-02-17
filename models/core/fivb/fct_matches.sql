{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'fact'],
    )
}}
select
    m.match_id,
    m.tournament_id,
    m.played_at,
    m.phase as match_phase,
    m.round_code,
    m.team1_id,
    m.team2_id,
    m.winner_team_id,
    m.score_sets,
    m.duration_minutes,
    m.result_type,
    m.status as match_status,
    dt.name as tournament_name,
    dt.season,
    dt.gender as tournament_gender,
    dt.tier as tournament_tier,
    r.name as round_name,
    r.bracket as round_bracket,
    r.phase as round_phase,
    t1.team_display_name as team1_display_name,
    t2.team_display_name as team2_display_name,
    t1.country_code as team1_country_code,
    t2.country_code as team2_country_code,
    -- derived
    (m.winner_team_id = m.team1_id) as is_winner_team1,
    lower(coalesce(r.phase, m.phase, '')) in ('final', 'finals', 'gold medal match') as is_final,
    lower(coalesce(r.phase, m.phase, '')) in ('pool', 'pools', 'pool play') or r.bracket = 'Pool' as is_pool_phase
from {{ ref('stg_fivb_matches') }} as m
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
left join {{ ref('stg_fivb_rounds') }} as r
    on r.tournament_id = m.tournament_id and r.code = m.round_code
left join {{ ref('dim_team_tournaments') }} as t1
    on t1.team_id = m.team1_id and t1.tournament_id = m.tournament_id
left join {{ ref('dim_team_tournaments') }} as t2
    on t2.team_id = m.team2_id and t2.tournament_id = m.tournament_id
