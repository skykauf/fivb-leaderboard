{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'elo'],
    )
}}
-- One row per completed match: H2H-only input for the Elo calculator (no rankings/standings).
-- Consumed by the Python Elo script, which writes core.player_elo_history.
-- Time ordering: played_at is rarely populated; use match_date (COALESCE(played_at, round_start_date, tournament_start_date)) for ordering and as_of_date.
--
-- Dedup by match_id: the same match can appear multiple times if dim_team_tournaments
-- has multiple rows per (team_id, tournament_id) (e.g. same team in different phases
-- in the same tournament). One row per match is required for Elo; we pick arbitrary
-- team rows when duplicated.
with feed as (
    select
        m.match_id,
        m.match_date,
        m.played_at,
        dt.start_date as tournament_start_date,
        m.tournament_gender,
        t1.player_a_id as team1_player_a_id,
        t1.player_b_id as team1_player_b_id,
        t2.player_a_id as team2_player_a_id,
        t2.player_b_id as team2_player_b_id,
        m.is_winner_team1,
        m.round_phase,
        m.round_name,
        m.is_final,
        m.is_pool_phase,
        row_number() over (partition by m.match_id order by m.match_date asc nulls last, t1.team_id, t2.team_id) as rn
    from {{ ref('fct_matches') }} as m
    join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
    join {{ ref('dim_team_tournaments') }} as t1
        on t1.team_id = m.team1_id and t1.tournament_id = m.tournament_id
    join {{ ref('dim_team_tournaments') }} as t2
        on t2.team_id = m.team2_id and t2.tournament_id = m.tournament_id
    where m.winner_team_id is not null
      and t1.player_a_id is not null and t1.player_b_id is not null
      and t2.player_a_id is not null and t2.player_b_id is not null
)
select
    match_id,
    match_date,
    played_at,
    tournament_start_date,
    tournament_gender,
    team1_player_a_id,
    team1_player_b_id,
    team2_player_a_id,
    team2_player_b_id,
    is_winner_team1,
    round_phase,
    round_name,
    is_final,
    is_pool_phase
from feed
where rn = 1
