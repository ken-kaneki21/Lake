{{ config(materialized='view') }}
select trip_id, driver_id, pickup_ts, dropoff_ts, pickup_date, pu_zone_id, do_zone_id,
       trip_distance, fare_amount, tip_amount, total_amount, status
from {{ ref('trip_fact_state') }}
where not is_deleted
