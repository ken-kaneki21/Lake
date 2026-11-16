{{ config(materialized='table') }}
select zone_id, borough, zone, service_zone
from lake.silver.zones
where not is_deleted
