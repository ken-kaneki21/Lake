{{ config(materialized='view') }}
select * from lake.silver.drivers where not is_deleted
