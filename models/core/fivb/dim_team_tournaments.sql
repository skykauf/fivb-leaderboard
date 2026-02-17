{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'dimension'],
    )
}}
select
    tt.team_id,
    tt.tournament_id,
    tt.player_a_id,
    tt.player_b_id,
    tt.country_code,
    tt.status,
    tt.valid_from,
    tt.valid_to,
    pa.full_name as player_a_name,
    pb.full_name as player_b_name,
    dt.name as tournament_name,
    dt.season,
    dt.gender as tournament_gender,
    -- derived: consistent display name (e.g. "Player A / Player B")
    coalesce(pa.full_name, 'Unknown') || ' / ' || coalesce(pb.full_name, 'Unknown') as team_display_name
from {{ ref('stg_fivb_teams') }} as tt
left join {{ ref('stg_fivb_players') }} as pa on pa.player_id = tt.player_a_id
left join {{ ref('stg_fivb_players') }} as pb on pb.player_id = tt.player_b_id
left join {{ ref('dim_tournaments') }} as dt on dt.tournament_id = tt.tournament_id
