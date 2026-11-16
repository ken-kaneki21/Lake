BEGIN;
INSERT INTO app.zones VALUES (501, 'test', 'test', 'test');
INSERT INTO app.drivers (driver_id) VALUES (501);
INSERT INTO app.trips (trip_id, driver_id, pickup_ts, pu_zone_id, fare_amount, tip_amount, total_amount)
    VALUES (501, 501, '2024-01-01 00:00+00', 501, 10.05, 2.00, 12.05);
UPDATE app.trips SET fare_amount = 12.34 WHERE trip_id = 501;
SAVEPOINT before_delete;
DELETE FROM app.trips WHERE trip_id = 501;
DO $$
BEGIN
    IF (SELECT count(*) FROM source_audit.trip_state WHERE trip_id = 501) <> 1 OR
       NOT EXISTS (SELECT 1 FROM source_audit.trip_state
                   WHERE trip_id = 501 AND is_deleted AND fare_amount = 12.34) THEN
        RAISE EXCEPTION 'Hard delete did not preserve authoritative last state';
    END IF;
END;
$$;
ROLLBACK TO before_delete;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM source_audit.trip_state
                   WHERE trip_id = 501 AND NOT is_deleted AND fare_amount = 12.34) OR
       EXISTS (SELECT 1 FROM source_audit.trip_tombstones WHERE trip_id = 501) THEN
        RAISE EXCEPTION 'Rollback did not restore authoritative live state';
    END IF;
END;
$$;
DELETE FROM app.trips WHERE trip_id = 501;
INSERT INTO app.trips (trip_id, driver_id, pickup_ts, pu_zone_id, fare_amount, total_amount)
    VALUES (501, 501, '2024-01-01 00:01+00', 501, 15.00, 15.00);
DO $$
BEGIN
    IF (SELECT count(*) FROM source_audit.trip_state WHERE trip_id = 501) <> 1 OR
       NOT EXISTS (SELECT 1 FROM source_audit.trip_state
                   WHERE trip_id = 501 AND NOT is_deleted AND fare_amount = 15.00) THEN
        RAISE EXCEPTION 'Reinsertion did not remove the old tombstone';
    END IF;
END;
$$;
ROLLBACK;
