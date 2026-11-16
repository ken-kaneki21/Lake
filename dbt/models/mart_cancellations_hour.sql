{{ config(materialized='table') }}
with cancellations as (
    select distinct cast(get_json_object(b.record_json, '$.trip_id') as bigint) as trip_id,
           date_trunc('hour', timestamp_millis(b.source_ts_ms)) as cancellation_hour,
           b.source_lsn, b.transaction_order
    from lake.bronze.cdc_events b
    left anti join lake.quarantine.cdc_events q
        on b.topic = q.topic and b.partition = q.partition and b.offset = q.offset
    where b.entity = 'trips' and b.op = 'd' and b.error is null
)
select cancellation_hour, count(*) as cancellations
from cancellations group by cancellation_hour
