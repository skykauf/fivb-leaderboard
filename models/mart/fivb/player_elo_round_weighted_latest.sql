{{
    config(
        materialized='view',
        tags=['marts', 'fivb', 'elo'],
    )
}}
-- Latest round-weighted Elo per player per gender (finals/semis weighted more than pool).
with player_totals as (
    select
        e.player_id,
        e.gender,
        count(*) as matches_played,
        sum(case when tt.team_id = m.winner_team_id then 1 else 0 end) as wins
    from {{ source('elo', 'player_elo_round_weighted_history') }} as e
    join {{ ref('stg_fivb_matches') }} as m on m.match_id = e.match_id
    join {{ ref('dim_team_tournaments') }} as tt
        on tt.tournament_id = m.tournament_id
        and tt.team_id in (m.team1_id, m.team2_id)
        and (tt.player_a_id = e.player_id or tt.player_b_id = e.player_id)
    group by e.player_id, e.gender
),
ranked as (
    select
        e.player_id,
        p.full_name as player_name,
        p.country_code as player_country_code,
        p.height_inches as player_height_inches,
        e.gender,
        e.elo_rating,
        e.as_of_date as last_match_played_at,
        e.match_id as last_match_id,
        m.tournament_id as last_match_tournament_id,
        dt.name as last_match_tournament_name,
        dt.season as last_match_tournament_season,
        row_number() over (partition by e.player_id, e.gender order by e.as_of_date desc, e.match_id desc) as rn
    from {{ source('elo', 'player_elo_round_weighted_history') }} as e
    left join {{ ref('stg_fivb_players') }} as p on p.player_id = e.player_id
    left join {{ ref('stg_fivb_matches') }} as m on m.match_id = e.match_id
    left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = m.tournament_id
)
select
    r.player_id,
    r.player_name,
    r.player_country_code,
    r.player_height_inches,
    r.gender,
    r.elo_rating,
    coalesce(t.matches_played, 0) as matches_played,
    coalesce(t.wins, 0) as wins,
    coalesce(t.matches_played, 0) - coalesce(t.wins, 0) as losses,
    r.last_match_played_at,
    r.last_match_id,
    r.last_match_tournament_id,
    r.last_match_tournament_name,
    r.last_match_tournament_season
from ranked r
left join player_totals t on t.player_id = r.player_id and t.gender = r.gender
where r.rn = 1
