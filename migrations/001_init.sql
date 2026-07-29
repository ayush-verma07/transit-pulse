-- 001_init.sql
-- Initial schema: raw feed archive, static schedule versioning, computed delay records.

-- Raw GTFS-Realtime payloads, archived BEFORE any parsing/reconciliation happens.
-- Why: if a bug is later found in the reconciliation logic (internal/reconcile/delay.go),
-- every derived row in delay_records can be recomputed from scratch from this table,
-- instead of the bug's effects being permanent because the raw input was discarded.
CREATE TABLE raw_feed_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    feed_url    TEXT NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL,
    payload     BYTEA NOT NULL, -- raw protobuf FeedMessage bytes, undecoded
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per download of the static GTFS schedule zip.
-- Why a separate version table at all: MTA republishes the static schedule periodically
-- (new trips, changed stop times), so a delay computed last week may have been measured
-- against a schedule that no longer exists today. Every delay record must point back to
-- the exact schedule version it was reconciled against, or historical comparisons become
-- meaningless once the schedule changes underneath them.
CREATE TABLE schedule_versions (
    id            SERIAL PRIMARY KEY,
    source_url    TEXT NOT NULL,
    downloaded_at TIMESTAMPTZ NOT NULL,
    content_hash  TEXT NOT NULL, -- sha256 of the zip; lets us detect "MTA re-served an identical file"
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per (trip, stop, scheduled arrival) actually observed and reconciled.
CREATE TABLE delay_records (
    id                  BIGSERIAL PRIMARY KEY,
    schedule_version_id INTEGER NOT NULL REFERENCES schedule_versions(id),
    trip_id             TEXT NOT NULL,
    route_id            TEXT NOT NULL,
    stop_id             TEXT NOT NULL,
    scheduled_time      TIMESTAMPTZ NOT NULL,
    actual_time         TIMESTAMPTZ NOT NULL,
    delay_seconds       INTEGER NOT NULL,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency key: the same (trip_id, stop_id, scheduled_time) triple identifies one
    -- real-world scheduled arrival, no matter how many 15s polls observe it before the
    -- vehicle passes. Re-processing the same prediction must UPDATE this row, not insert
    -- a duplicate. See internal/reconcile/delay.go for why this triple is safe to use as
    -- the natural key (and its known edge cases: recycled trip_ids across service days).
    UNIQUE (trip_id, stop_id, scheduled_time)
);

CREATE INDEX idx_delay_records_route_time ON delay_records (route_id, scheduled_time);
CREATE INDEX idx_delay_records_stop_time ON delay_records (stop_id, scheduled_time);
