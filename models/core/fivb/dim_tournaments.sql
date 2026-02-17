{{
    config(
        materialized='view',
        tags=['core', 'fivb', 'dimension'],
    )
}}
select
    t.tournament_id,
    t.name,
    t.season,
    t.tier,
    t.start_date,
    t.end_date,
    t.city,
    t.country_code,
    t.country_name,
    t.gender,
    t.status,
    t.timezone,
    -- derived
    (t.end_date - t.start_date + 1)::int as duration_days,
    nullif(regexp_replace(t.season, '\D', '', 'g'), '')::int as season_year,
    case
        when t.tier in ('World Championship', 'Olympic Games') then true
        else false
    end as is_major
from {{ ref('stg_fivb_tournaments') }} as t
