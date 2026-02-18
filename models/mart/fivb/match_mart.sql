{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'match'],
    )
}}
-- One row per match with full tournament, round, team, player, ranking, and standings metadata.
select
    m.match_id,
    m.tournament_id,
    m.played_at,
    m.match_phase,
    m.round_code,
    m.team1_id,
    m.team2_id,
    m.winner_team_id,
    m.score_sets,
    m.duration_minutes,
    m.result_type,
    m.match_status,
    -- tournament (from fct_matches + dim for full context)
    m.tournament_name,
    m.season,
    m.tournament_gender,
    m.tournament_tier,
    dt.start_date as tournament_start_date,
    dt.end_date as tournament_end_date,
    dt.duration_days as tournament_duration_days,
    dt.city as tournament_city,
    dt.country_code as tournament_country_code,
    dt.country_name as tournament_country_name,
    dt.status as tournament_status,
    dt.timezone as tournament_timezone,
    dt.season_year as tournament_season_year,
    dt.is_major as tournament_is_major,
    -- round
    m.round_name,
    m.round_bracket,
    m.round_phase,
    r.start_date as round_start_date,
    r.end_date as round_end_date,
    r.rank_method as round_rank_method,
    -- team1
    m.team1_display_name,
    m.team1_country_code,
    t1.player_a_id as team1_player_a_id,
    t1.player_b_id as team1_player_b_id,
    t1.player_a_name as team1_player_a_name,
    t1.player_b_name as team1_player_b_name,
    m.team1_ranking_position,
    st1.finishing_pos as team1_finishing_pos,
    st1.points as team1_tournament_points,
    st1.prize_money as team1_prize_money,
    st1.entry_ranking_position as team1_entry_ranking_position,
    st1.entry_ranking_points as team1_entry_ranking_points,
    -- team2
    m.team2_display_name,
    m.team2_country_code,
    t2.player_a_id as team2_player_a_id,
    t2.player_b_id as team2_player_b_id,
    t2.player_a_name as team2_player_a_name,
    t2.player_b_name as team2_player_b_name,
    m.team2_ranking_position,
    st2.finishing_pos as team2_finishing_pos,
    st2.points as team2_tournament_points,
    st2.prize_money as team2_prize_money,
    st2.entry_ranking_position as team2_entry_ranking_position,
    st2.entry_ranking_points as team2_entry_ranking_points,
    -- derived
    m.is_winner_team1,
    m.is_final,
    m.is_pool_phase,
    m.higher_seed_won
from {{ ref('fct_matches') }} as m
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
left join {{ ref('stg_fivb_rounds') }} as r
    on r.tournament_id = m.tournament_id and r.code = m.round_code
left join {{ ref('dim_team_tournaments') }} as t1
    on t1.team_id = m.team1_id and t1.tournament_id = m.tournament_id
left join {{ ref('dim_team_tournaments') }} as t2
    on t2.team_id = m.team2_id and t2.tournament_id = m.tournament_id
left join {{ ref('fct_tournament_standings') }} as st1
    on st1.tournament_id = m.tournament_id and st1.team_id = m.team1_id
left join {{ ref('fct_tournament_standings') }} as st2
    on st2.tournament_id = m.tournament_id and st2.team_id = m.team2_id
