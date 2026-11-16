{{ config(materialized='view') }}
select *, cast(pickup_ts as date) as pickup_date
from lake.silver.trips
