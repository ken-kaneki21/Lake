{{ config(materialized='table') }}
select distinct pickup_date as date_key, year(pickup_date) as calendar_year,
       month(pickup_date) as calendar_month, dayofmonth(pickup_date) as calendar_day
from {{ ref('stg_trips') }}
where pickup_date is not null
