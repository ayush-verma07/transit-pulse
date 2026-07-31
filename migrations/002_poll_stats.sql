-- 002_poll_stats.sql
-- Per-poll-cycle observability: how many trip updates were seen, how many
-- reconciled into saved delay records, and how many were skipped, broken
-- down by why. Lets reconciliation match-rate trends be queried directly
-- from Postgres instead of parsed out of Render's log output.
CREATE TABLE poll_stats (
    id                 BIGSERIAL PRIMARY KEY,
    polled_at          TIMESTAMPTZ NOT NULL, -- snapshot.FetchedAt for this cycle, not wall-clock insert time
    trip_updates_seen  INTEGER NOT NULL,
    delays_saved       INTEGER NOT NULL,
    skipped_bad_date   INTEGER NOT NULL, -- reconcile.ParseServiceDate failed (bad StartDate format)
    skipped_no_match   INTEGER NOT NULL, -- reconcile.Reconcile found no scheduled arrival within matchTolerance
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_poll_stats_polled_at ON poll_stats (polled_at);
