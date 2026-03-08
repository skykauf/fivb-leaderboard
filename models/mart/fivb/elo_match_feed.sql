{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'elo'],
    )
}}
-- One row per completed match: H2H-only input for the Elo calculator (no rankings/standings).
-- Consumed by the Python Elo script, which writes core.player_elo_history.
select
    m.match_id,
    m.played_at,
    m.tournament_gender,
    t1.player_a_id as team1_player_a_id,
    t1.player_b_id as team1_player_b_id,
    t2.player_a_id as team2_player_a_id,
    t2.player_b_id as team2_player_b_id,
    m.is_winner_team1
from {{ ref('fct_matches') }} as m
join {{ ref('dim_team_tournaments') }} as t1
    on t1.team_id = m.team1_id and t1.tournament_id = m.tournament_id
join {{ ref('dim_team_tournaments') }} as t2
    on t2.team_id = m.team2_id and t2.tournament_id = m.tournament_id
where m.winner_team_id is not null
  and t1.player_a_id is not null and t1.player_b_id is not null
  and t2.player_a_id is not null and t2.player_b_id is not null
