WITH trip_rows AS (
    SELECT trip_id, fare_amount, tip_amount, is_deleted,
           substring(md5(CAST(trip_id AS STRING)), 1, 2) AS bucket,
           CAST(conv(substring(md5(concat(
               CAST(trip_id AS STRING), '|',
               coalesce(CAST(CAST(fare_amount AS DECIMAL(10,2)) AS STRING), '~'), '|',
               coalesce(CAST(CAST(tip_amount AS DECIMAL(10,2)) AS STRING), '~'), '|',
               CASE WHEN is_deleted THEN '1' ELSE '0' END
           )), 1, 8), 16, 10) AS DECIMAL(38,0)) AS fingerprint
    FROM lake.silver.trips
    WHERE pickup_ts >= :window_start AND pickup_ts < :window_end
), buckets AS (
    SELECT lpad(lower(hex(id)), 2, '0') AS bucket FROM range(256)
)
SELECT buckets.bucket,
       count_if(NOT trip_rows.is_deleted) AS row_count,
       coalesce(sum(CASE WHEN NOT is_deleted THEN CAST(fare_amount * 100 AS DECIMAL(38,0)) END), 0)
           AS fare_cents,
       coalesce(sum(CASE WHEN NOT is_deleted THEN CAST(tip_amount * 100 AS DECIMAL(38,0)) END), 0)
           AS tip_cents,
       count_if(trip_rows.is_deleted) AS deleted_count,
       coalesce(sum(fingerprint), 0) AS fingerprint_sum
FROM buckets LEFT JOIN trip_rows ON buckets.bucket = trip_rows.bucket
GROUP BY buckets.bucket
ORDER BY buckets.bucket;
