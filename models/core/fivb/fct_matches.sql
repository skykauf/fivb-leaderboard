{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'fact'],
    )
}}
-- Latest world ranking position per team as of match date (lower position = higher seed)
with match_team_rankings as (
    select
        m.match_id,
        m.played_at,
        m.team1_id,
        m.team2_id,
        m.winner_team_id,
        dt.gender,
        t1.player_a_id as t1_pa,
        t1.player_b_id as t1_pb,
        t2.player_a_id as t2_pa,
        t2.player_b_id as t2_pb,
        tr.position,
        tr.snapshot_date,
        tr.ranking_type,
        case
            when (tr.no_player1 = t1.player_a_id and tr.no_player2 = t1.player_b_id)
                or (tr.no_player1 = t1.player_b_id and tr.no_player2 = t1.player_a_id)
            then 1
            else 2
        end as which_team
    from {{ ref('stg_fivb_matches') }} as m
    join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
    join {{ ref('dim_team_tournaments') }} as t1 on t1.team_id = m.team1_id and t1.tournament_id = m.tournament_id
    join {{ ref('dim_team_tournaments') }} as t2 on t2.team_id = m.team2_id and t2.tournament_id = m.tournament_id
    join {{ ref('fct_team_rankings') }} as tr
        on tr.gender = dt.gender
        and tr.snapshot_date <= m.played_at
        and (
            ((tr.no_player1 = t1.player_a_id and tr.no_player2 = t1.player_b_id) or (tr.no_player1 = t1.player_b_id and tr.no_player2 = t1.player_a_id))
            or ((tr.no_player1 = t2.player_a_id and tr.no_player2 = t2.player_b_id) or (tr.no_player1 = t2.player_b_id and tr.no_player2 = t2.player_a_id))
        )
),
latest_rank_by_team as (
    select
        match_id,
        which_team,
        position,
        row_number() over (
            partition by match_id, which_team
            order by snapshot_date desc, ranking_type
        ) as rn
    from match_team_rankings
),
team1_rank as (
    select match_id, position as team1_ranking_position
    from latest_rank_by_team
    where which_team = 1 and rn = 1
),
team2_rank as (
    select match_id, position as team2_ranking_position
    from latest_rank_by_team
    where which_team = 2 and rn = 1
)
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
    lower(coalesce(r.phase, m.phase, '')) in ('pool', 'pools', 'pool play') or r.bracket = 'Pool' as is_pool_phase,
    -- true when the team with the better (lower) world ranking position won; null if either team has no ranking or no winner
    case
        when m.winner_team_id is null then null
        when r1.team1_ranking_position is null or r2.team2_ranking_position is null then null
        when m.winner_team_id = m.team1_id then r1.team1_ranking_position < r2.team2_ranking_position
        else r2.team2_ranking_position < r1.team1_ranking_position
    end as higher_seed_won
from {{ ref('stg_fivb_matches') }} as m
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
left join {{ ref('stg_fivb_rounds') }} as r
    on r.tournament_id = m.tournament_id and r.code = m.round_code
left join {{ ref('dim_team_tournaments') }} as t1
    on t1.team_id = m.team1_id and t1.tournament_id = m.tournament_id
left join {{ ref('dim_team_tournaments') }} as t2
    on t2.team_id = m.team2_id and t2.tournament_id = m.tournament_id
left join team1_rank as r1 on r1.match_id = m.match_id
left join team2_rank as r2 on r2.match_id = m.match_id
