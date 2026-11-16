-- Run as the owner of the captured tables before registering the connector.
-- The dedicated replication role needs LOGIN, REPLICATION, schema USAGE
-- and SELECT on these three tables for its initial snapshot.
CREATE PUBLICATION paritylake FOR TABLE app.trips, app.drivers, app.zones;
