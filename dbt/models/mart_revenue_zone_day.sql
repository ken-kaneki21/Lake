{{ config(materialized='table') }}
select pickup_date, pu_zone_id, count(*) as trips, sum(fare_amount) as fare_amount,
       sum(tip_amount) as tip_amount, sum(total_amount) as revenue
from {{ ref('fct_trips') }}
group by pickup_date, pu_zone_id
