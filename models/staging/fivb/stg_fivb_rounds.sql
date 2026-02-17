{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
select
    round_id,
    tournament_id,
    code,
    name,
    bracket,
    phase,
    start_date,
    end_date,
    rank_method,
    payload,
    ingested_at
from {{ source('raw_fivb', 'raw_fivb_rounds') }}
