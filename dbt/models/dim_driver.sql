{{ config(materialized='table') }}
with versions as (
    select cast(get_json_object(b.record_json, '$.driver_id') as bigint) as driver_id,
           cast(get_json_object(b.record_json, '$.rating') as decimal(3,2)) as rating,
           get_json_object(b.record_json, '$.vehicle_type') as vehicle_type,
           get_json_object(b.record_json, '$.city') as city,
           b.op = 'd' as is_deleted,
           timestamp_millis(b.source_ts_ms) as valid_from,
           b.is_wal, b.source_lsn, b.transaction_order, b.offset
    from lake.bronze.cdc_events b
    left anti join lake.quarantine.cdc_events q
        on b.topic = q.topic and b.partition = q.partition and b.offset = q.offset
    where b.entity = 'drivers' and b.error is null
), ranked as (
    select *, row_number() over (
        partition by driver_id, valid_from
        order by is_wal desc, source_lsn desc, transaction_order desc, offset desc
    ) as position
    from versions
), compared as (
    select *, lag(named_struct('rating', rating, 'vehicle_type', vehicle_type,
                               'city', city, 'is_deleted', is_deleted))
                 over (partition by driver_id order by valid_from) as previous
    from ranked where position = 1
), changed as (
    select *
    from compared
    where not (previous <=> named_struct('rating', rating, 'vehicle_type', vehicle_type,
                                         'city', city, 'is_deleted', is_deleted))
), intervals as (
    select driver_id, rating, vehicle_type, city, valid_from, is_deleted,
           lead(valid_from) over (partition by driver_id order by valid_from) as valid_to
    from changed
)
select driver_id, rating, vehicle_type, city, valid_from, valid_to, valid_to is null as is_current
from intervals where not is_deleted
