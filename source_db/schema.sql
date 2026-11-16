BEGIN;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS source_audit;

CREATE TABLE IF NOT EXISTS app.zones (
    zone_id integer PRIMARY KEY,
    borough text,
    zone text,
    service_zone text
);
CREATE TABLE IF NOT EXISTS app.drivers (
    driver_id bigint PRIMARY KEY,
    rating numeric(3,2),
    vehicle_type text,
    city text,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS app.trips (
    trip_id bigserial PRIMARY KEY,
    driver_id bigint REFERENCES app.drivers,
    pickup_ts timestamptz NOT NULL,
    dropoff_ts timestamptz,
    pu_zone_id integer REFERENCES app.zones,
    do_zone_id integer REFERENCES app.zones,
    trip_distance numeric(8,2),
    fare_amount numeric(10,2) NOT NULL,
    tip_amount numeric(10,2) NOT NULL DEFAULT 0,
    total_amount numeric(10,2) NOT NULL,
    status text NOT NULL DEFAULT 'completed',
    updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE app.trips REPLICA IDENTITY FULL;
ALTER TABLE app.drivers REPLICA IDENTITY FULL;
ALTER TABLE app.zones REPLICA IDENTITY FULL;

-- The trigger retains the last row before a hard delete. A reinserted key
-- clears its tombstone in the same transaction, preserving one current state.
CREATE TABLE IF NOT EXISTS source_audit.trip_tombstones (
    trip_id bigint PRIMARY KEY,
    pickup_ts timestamptz NOT NULL,
    fare_amount numeric(10,2) NOT NULL,
    tip_amount numeric(10,2) NOT NULL
);
CREATE OR REPLACE FUNCTION source_audit.track_trip_deletes()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO source_audit.trip_tombstones
            (trip_id, pickup_ts, fare_amount, tip_amount)
        VALUES (OLD.trip_id, OLD.pickup_ts, OLD.fare_amount, OLD.tip_amount)
        ON CONFLICT (trip_id) DO UPDATE SET pickup_ts = EXCLUDED.pickup_ts,
            fare_amount = EXCLUDED.fare_amount, tip_amount = EXCLUDED.tip_amount;
        RETURN OLD;
    END IF;
    DELETE FROM source_audit.trip_tombstones WHERE trip_id = NEW.trip_id;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trip_tombstones ON app.trips;
CREATE TRIGGER trip_tombstones AFTER INSERT OR DELETE ON app.trips
    FOR EACH ROW EXECUTE FUNCTION source_audit.track_trip_deletes();
CREATE INDEX IF NOT EXISTS trips_pickup_ts ON app.trips (pickup_ts);
CREATE INDEX IF NOT EXISTS trip_tombstones_pickup_ts ON source_audit.trip_tombstones (pickup_ts);

CREATE OR REPLACE VIEW source_audit.trip_state AS
    SELECT trip_id, pickup_ts, fare_amount, tip_amount, false AS is_deleted
    FROM app.trips
    UNION ALL
    SELECT trip_id, pickup_ts, fare_amount, tip_amount, true AS is_deleted
    FROM source_audit.trip_tombstones;
COMMIT;
