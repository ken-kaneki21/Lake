{{ config(materialized='incremental', incremental_strategy='merge', unique_key='trip_id') }}
select s.*
from {{ ref('stg_trips') }} s
{% if is_incremental() %}
left join {{ this }} t on s.trip_id = t.trip_id
where t.trip_id is null or s.is_wal > t.is_wal or (
    s.is_wal = t.is_wal and (s.source_lsn > t.source_lsn or
        (s.source_lsn = t.source_lsn and s.transaction_order > t.transaction_order))
)
{% endif %}
