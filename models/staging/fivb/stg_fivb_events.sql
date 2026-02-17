{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    event_id,
    code,
    name,
    start_date,
    end_date,
    type,
    no_parent_event,
    country_code,
    has_beach_tournament,
    has_men_tournament,
    has_women_tournament,
    is_vis_managed,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_events') }}
