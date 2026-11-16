WITH trip_rows AS (
    SELECT trip_id, fare_amount, tip_amount, is_deleted,
           substring(md5(trip_id::text) for 2) AS bucket,
           (('x' || substring(md5(concat(
               trip_id::text, '|', coalesce(fare_amount::numeric(10,2)::text, '~'), '|',
               coalesce(tip_amount::numeric(10,2)::text, '~'), '|',
               CASE WHEN is_deleted THEN '1' ELSE '0' END
           )) for 8))::bit(32)::bigint) AS fingerprint
    FROM source_audit.trip_state
    WHERE pickup_ts >= %(window_start)s AND pickup_ts < %(window_end)s
), buckets AS (
    SELECT lpad(to_hex(i), 2, '0') AS bucket FROM generate_series(0, 255) AS s(i)
)
SELECT buckets.bucket,
       count(*) FILTER (WHERE NOT trip_rows.is_deleted) AS row_count,
       coalesce(sum(trip_rows.fare_amount * 100)
           FILTER (WHERE NOT trip_rows.is_deleted), 0)::numeric(38,0) AS fare_cents,
       coalesce(sum(trip_rows.tip_amount * 100)
           FILTER (WHERE NOT trip_rows.is_deleted), 0)::numeric(38,0) AS tip_cents,
       count(*) FILTER (WHERE trip_rows.is_deleted) AS deleted_count,
       coalesce(sum(trip_rows.fingerprint), 0)::numeric(38,0) AS fingerprint_sum
FROM buckets LEFT JOIN trip_rows USING (bucket)
GROUP BY buckets.bucket
ORDER BY buckets.bucket;
