with history as (
    select *, lead(valid_from) over (partition by driver_id order by valid_from) as next_start
    from {{ ref('dim_driver') }}
)
select * from history
where (valid_to is not null and valid_to <= valid_from)
    or (next_start is not null and (valid_to is null or valid_to > next_start))
