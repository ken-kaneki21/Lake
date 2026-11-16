CREATE SCHEMA IF NOT EXISTS recon;

CREATE TABLE IF NOT EXISTS recon.runs (
    run_id uuid PRIMARY KEY,
    scheduled_at timestamptz NOT NULL,
    snapshot_id bigint NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS recon.windows (
    run_id uuid NOT NULL REFERENCES recon.runs,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    source_buckets jsonb NOT NULL,
    lake_buckets jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('match', 'drift')),
    PRIMARY KEY (run_id, window_start)
);

CREATE TABLE IF NOT EXISTS recon.alerts (
    run_id uuid NOT NULL,
    window_start timestamptz NOT NULL,
    evidence jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, window_start),
    FOREIGN KEY (run_id, window_start) REFERENCES recon.windows
);
