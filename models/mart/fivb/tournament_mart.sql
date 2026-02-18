{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'tournament'],
    )
}}
-- One row per tournament + team with final ranking and derived performance metrics.
-- sum_opponent_points_beaten: sum of standings.points for opponents beaten (null when Points missing from GetBeachTournamentRanking).
with wins_with_opponent_points as (
    select
        m.tournament_id,
        m.winner_team_id as team_id,
        st_loser.points as opponent_points
    from {{ ref('fct_matches') }} as m
    left join {{ ref('fct_tournament_standings') }} as st_loser
        on st_loser.tournament_id = m.tournament_id
        and st_loser.team_id = (case when m.winner_team_id = m.team1_id then m.team2_id else m.team1_id end)
    where m.winner_team_id is not null
),
sum_opponent_points_beaten_by_team as (
    select
        tournament_id,
        team_id,
        case when count(opponent_points) = count(*) then sum(opponent_points)::int else null end as sum_opponent_points_beaten
    from wins_with_opponent_points
    group by tournament_id, team_id
),
-- One row per (tournament, team, match): did this team play, win, opponent, pool/elimination, upset/bad loss
team_match_outcomes as (
    select
        m.tournament_id,
        m.team1_id as team_id,
        m.team2_id as opponent_team_id,
        m.match_id,
        (m.winner_team_id = m.team1_id) as won,
        (m.winner_team_id is not null) as has_winner,
        m.is_pool_phase,
        m.higher_seed_won
    from {{ ref('fct_matches') }} as m
    where m.team1_id is not null
    union all
    select
        m.tournament_id,
        m.team2_id as team_id,
        m.team1_id as opponent_team_id,
        m.match_id,
        (m.winner_team_id = m.team2_id) as won,
        (m.winner_team_id is not null) as has_winner,
        m.is_pool_phase,
        m.higher_seed_won
    from {{ ref('fct_matches') }} as m
    where m.team2_id is not null
),
-- Tournament size (team count) for normalizing the quality score
tournament_team_count as (
    select
        tournament_id,
        count(*)::int as n_teams,
        nullif(count(*)::int - 1, 0) as n_minus_1
    from {{ ref('fct_tournament_standings') }}
    group by tournament_id
),
-- Per-match quality contribution: beat good = big plus, beat bad = small plus, lose to good = small minus, lose to bad = big minus
-- Opponent strength from their finishing_pos (1 = champion = strong). Win adds (N-pos)/(N-1), loss subtracts (pos-1)/(N-1).
match_quality_contributions as (
    select
        tmo.tournament_id,
        tmo.team_id,
        tmo.match_id,
        tmo.won,
        st_opp.finishing_pos as opponent_finishing_pos,
        tc.n_teams,
        tc.n_minus_1,
        case
            when not tmo.has_winner or tc.n_minus_1 is null or tc.n_minus_1 = 0 then 0
            when tmo.won then (tc.n_teams - coalesce(st_opp.finishing_pos, tc.n_teams))::numeric / tc.n_minus_1
            else - (coalesce(st_opp.finishing_pos, tc.n_teams) - 1)::numeric / tc.n_minus_1
        end as quality_contribution
    from team_match_outcomes as tmo
    join tournament_team_count as tc on tc.tournament_id = tmo.tournament_id
    left join {{ ref('fct_tournament_standings') }} as st_opp
        on st_opp.tournament_id = tmo.tournament_id and st_opp.team_id = tmo.opponent_team_id
),
quality_win_loss_score_by_team as (
    select
        tournament_id,
        team_id,
        sum(quality_contribution)::numeric as quality_win_loss_score
    from match_quality_contributions
    group by tournament_id, team_id
),
-- Points-based version: opponent strength = entry_ranking_points / max in tournament (0..1). Win +strength, loss -(1-strength).
tournament_max_entry_points as (
    select
        tournament_id,
        nullif(greatest(max(coalesce(entry_ranking_points, 0)), 1), 0) as max_entry_points
    from {{ ref('fct_tournament_standings') }}
    group by tournament_id
),
match_quality_contributions_points as (
    select
        tmo.tournament_id,
        tmo.team_id,
        tmo.match_id,
        tmo.won,
        st_opp.entry_ranking_points as opponent_entry_points,
        tmax.max_entry_points,
        case
            when not tmo.has_winner or tmax.max_entry_points is null then 0
            else
                case
                    when tmo.won then coalesce(st_opp.entry_ranking_points, 0)::numeric / tmax.max_entry_points
                    else (coalesce(st_opp.entry_ranking_points, 0)::numeric / tmax.max_entry_points) - 1
                end
        end as quality_contribution_points
    from team_match_outcomes as tmo
    join tournament_max_entry_points as tmax on tmax.tournament_id = tmo.tournament_id
    left join {{ ref('fct_tournament_standings') }} as st_opp
        on st_opp.tournament_id = tmo.tournament_id and st_opp.team_id = tmo.opponent_team_id
),
quality_win_loss_score_points_by_team as (
    select
        tournament_id,
        team_id,
        sum(quality_contribution_points)::numeric as quality_win_loss_score_points
    from match_quality_contributions_points
    group by tournament_id, team_id
),
match_stats_by_team as (
    select
        tournament_id,
        team_id,
        count(*)::int as matches_played,
        count(*) filter (where won)::int as match_wins,
        count(*) filter (where has_winner and not won)::int as match_losses,
        count(*) filter (where won and higher_seed_won = false)::int as wins_vs_higher_seed,
        count(*) filter (where has_winner and not won and higher_seed_won = false)::int as losses_vs_lower_seed,
        count(*) filter (where won and coalesce(is_pool_phase, false))::int as pool_wins,
        count(*) filter (where won and not coalesce(is_pool_phase, true))::int as elimination_wins
    from team_match_outcomes
    group by tournament_id, team_id
)
select
    s.tournament_id,
    s.team_id,
    -- tournament
    s.tournament_name,
    dt.season,
    dt.season_year,
    dt.tier as tournament_tier,
    dt.gender as tournament_gender,
    dt.start_date as tournament_start_date,
    dt.end_date as tournament_end_date,
    dt.duration_days as tournament_duration_days,
    dt.city as tournament_city,
    dt.country_code as tournament_country_code,
    dt.country_name as tournament_country_name,
    dt.status as tournament_status,
    dt.timezone as tournament_timezone,
    dt.is_major as tournament_is_major,
    -- team
    s.team_display_name,
    s.player_a_name,
    s.player_b_name,
    s.team_country_code,
    -- final ranking / standings
    s.finishing_pos,
    s.points as tournament_points,
    s.prize_money,
    s.entry_ranking_position,
    s.entry_ranking_points,
    s.entry_ranking_snapshot_date,
    s.is_podium,
    s.is_champion,
    -- performance beyond ranking
    opb.sum_opponent_points_beaten,
    -- match counts and quality
    coalesce(ms.matches_played, 0) as matches_played,
    coalesce(ms.match_wins, 0) as match_wins,
    coalesce(ms.match_losses, 0) as match_losses,
    coalesce(ms.wins_vs_higher_seed, 0) as wins_vs_higher_seed,
    coalesce(ms.losses_vs_lower_seed, 0) as losses_vs_lower_seed,
    coalesce(ms.pool_wins, 0) as pool_wins,
    coalesce(ms.elimination_wins, 0) as elimination_wins,
    -- scaled quality of wins/losses: beat good = big +, beat bad = small +, lose to good = small -, lose to bad = big -
    coalesce(qwl.quality_win_loss_score, 0) as quality_win_loss_score,
    -- same idea using entry_ranking_points: opponent strength = points / max in tournament
    coalesce(qwl_p.quality_win_loss_score_points, 0) as quality_win_loss_score_points
from {{ ref('fct_tournament_standings') }} as s
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = s.tournament_id
left join sum_opponent_points_beaten_by_team as opb
    on opb.tournament_id = s.tournament_id and opb.team_id = s.team_id
left join match_stats_by_team as ms
    on ms.tournament_id = s.tournament_id and ms.team_id = s.team_id
left join quality_win_loss_score_by_team as qwl
    on qwl.tournament_id = s.tournament_id and qwl.team_id = s.team_id
left join quality_win_loss_score_points_by_team as qwl_p
    on qwl_p.tournament_id = s.tournament_id and qwl_p.team_id = s.team_id
