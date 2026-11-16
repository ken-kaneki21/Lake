with expected as (
    select count(*) as trips, coalesce(sum(total_amount), 0) as revenue
    from {{ ref('stg_trips') }} where not is_deleted
), observed as (
    select coalesce(sum(trips), 0) as trips, coalesce(sum(revenue), 0) as revenue
    from {{ ref('mart_revenue_zone_day') }}
)
select expected.* from expected cross join observed
where expected.trips <> observed.trips or expected.revenue <> observed.revenue
