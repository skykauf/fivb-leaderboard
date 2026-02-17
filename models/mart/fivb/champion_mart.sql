{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'champion'],
    )
}}
-- One row per tournament champion with tournament, team, final match, and ranking-at-event data.
with standings as (
    select
        tournament_id,
        team_id,
        finishing_pos,
        points,
        prize_money,
        tournament_name,
        season,
        tournament_gender,
        tournament_tier,
        is_major,
        team_display_name,
        player_a_name,
        player_b_name,
        team_country_code,
        is_podium,
        is_champion
    from {{ ref('fct_tournament_standings') }}
    where is_champion
),
-- Full tournament attributes (dates, location, etc.)
tournaments as (
    select
        tournament_id,
        name,
        season,
        tier,
        start_date,
        end_date,
        city,
        country_code as host_country_code,
        country_name as host_country_name,
        gender,
        status as tournament_status,
        timezone,
        duration_days,
        season_year,
        is_major
    from {{ ref('dim_tournaments') }}
),
-- Final match for each champion (opponent, score, duration)
final_matches as (
    select
        m.tournament_id,
        m.winner_team_id as team_id,
        m.played_at as final_played_at,
        m.score_sets as final_score_sets,
        m.duration_minutes as final_duration_minutes,
        m.round_name as final_round_name,
        case when m.team1_id = m.winner_team_id then m.team2_display_name else m.team1_display_name end as final_opponent_display_name,
        case when m.team1_id = m.winner_team_id then m.team2_country_code else m.team1_country_code end as final_opponent_country_code
    from {{ ref('fct_matches') }} as m
    where m.is_final and m.winner_team_id is not null
),
-- Latest world ranking for the champion team at or before tournament end (for context)
champion_teams_with_players as (
    select
        s.tournament_id,
        s.team_id,
        tt.player_a_id,
        tt.player_b_id,
        t.end_date,
        t.gender
    from standings s
    join {{ ref('dim_team_tournaments') }} tt on tt.team_id = s.team_id and tt.tournament_id = s.tournament_id
    join tournaments t on t.tournament_id = s.tournament_id
),
rankings_at_event as (
    select
        ctwp.tournament_id,
        ctwp.team_id,
        tr.position as ranking_position_at_tournament,
        tr.ranking_type as ranking_type_at_tournament,
        tr.snapshot_date as ranking_snapshot_date,
        tr.is_top_10 as is_top_10_at_tournament,
        tr.is_top_16 as is_top_16_at_tournament,
        row_number() over (
            partition by ctwp.tournament_id, ctwp.team_id
            order by tr.snapshot_date desc
        ) as rn
    from champion_teams_with_players ctwp
    join {{ ref('fct_team_rankings') }} tr
        on tr.gender = ctwp.gender
        and tr.snapshot_date <= ctwp.end_date
        and (
            (tr.no_player1 = ctwp.player_a_id and tr.no_player2 = ctwp.player_b_id)
            or (tr.no_player1 = ctwp.player_b_id and tr.no_player2 = ctwp.player_a_id)
        )
),
latest_ranking as (
    select
        tournament_id,
        team_id,
        ranking_position_at_tournament,
        ranking_type_at_tournament,
        ranking_snapshot_date,
        is_top_10_at_tournament,
        is_top_16_at_tournament
    from rankings_at_event
    where rn = 1
)
select
    -- Identity
    s.tournament_id,
    s.team_id,
    -- Tournament (from standings + full dim)
    s.tournament_name,
    s.season,
    t.season_year,
    s.tournament_gender,
    s.tournament_tier,
    s.is_major,
    t.start_date as tournament_start_date,
    t.end_date as tournament_end_date,
    t.duration_days as tournament_duration_days,
    t.city as tournament_city,
    t.host_country_code as tournament_host_country_code,
    t.host_country_name as tournament_host_country_name,
    t.tournament_status,
    t.timezone as tournament_timezone,
    -- Champion team
    s.team_display_name as champion_team_display_name,
    s.player_a_name as champion_player_a_name,
    s.player_b_name as champion_player_b_name,
    s.team_country_code as champion_country_code,
    -- Standing
    s.finishing_pos,
    s.points,
    s.prize_money,
    -- Final match
    fm.final_played_at,
    fm.final_opponent_display_name,
    fm.final_opponent_country_code,
    fm.final_score_sets,
    fm.final_duration_minutes,
    fm.final_round_name,
    -- Ranking at time of tournament
    lr.ranking_position_at_tournament,
    lr.ranking_type_at_tournament,
    lr.ranking_snapshot_date,
    lr.is_top_10_at_tournament,
    lr.is_top_16_at_tournament
from standings s
join tournaments t on t.tournament_id = s.tournament_id
left join final_matches fm on fm.tournament_id = s.tournament_id and fm.team_id = s.team_id
left join latest_ranking lr on lr.tournament_id = s.tournament_id and lr.team_id = s.team_id
