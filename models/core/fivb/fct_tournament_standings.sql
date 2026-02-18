{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'fact'],
    )
}}
-- Team's World Tour ranking as of tournament start (for entry seeding context)
with entry_rankings as (
    select
        r.tournament_id,
        r.team_id,
        tr.position as entry_ranking_position,
        tr.earned_points as entry_ranking_points,
        tr.snapshot_date as entry_ranking_snapshot_date,
        row_number() over (
            partition by r.tournament_id, r.team_id
            order by tr.snapshot_date desc
        ) as rn
    from {{ ref('stg_fivb_results') }} as r
    join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = r.tournament_id
    join {{ ref('dim_team_tournaments') }} as dtt
        on dtt.team_id = r.team_id and dtt.tournament_id = r.tournament_id
    join {{ ref('fct_team_rankings') }} as tr
        on tr.gender = dt.gender
        and tr.ranking_type = 'beach_world_tour'
        and tr.snapshot_date <= dt.start_date
        and (
            (tr.no_player1 = dtt.player_a_id and tr.no_player2 = dtt.player_b_id)
            or (tr.no_player1 = dtt.player_b_id and tr.no_player2 = dtt.player_a_id)
        )
),
latest_entry_ranking as (
    select
        tournament_id,
        team_id,
        entry_ranking_position,
        entry_ranking_points,
        entry_ranking_snapshot_date
    from entry_rankings
    where rn = 1
)
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
    -- entry ranking (World Tour as of tournament start; used for seeding)
    er.entry_ranking_position,
    er.entry_ranking_points,
    er.entry_ranking_snapshot_date,
    -- derived
    (r.finishing_pos <= 3) as is_podium,
    (r.finishing_pos = 1) as is_champion
from {{ ref('stg_fivb_results') }} as r
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = r.tournament_id
left join {{ ref('dim_team_tournaments') }} as dtt
    on dtt.team_id = r.team_id and dtt.tournament_id = r.tournament_id
left join latest_entry_ranking as er
    on er.tournament_id = r.tournament_id and er.team_id = r.team_id
