// Package store is the only place in this codebase allowed to import a
// Postgres driver or write SQL. Every other package works through the
// plain Go types defined in internal/gtfs and internal/reconcile.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver with database/sql; never referenced directly

	"github.com/ayush-verma07/transit-pulse/internal/gtfs"
	"github.com/ayush-verma07/transit-pulse/internal/reconcile"
)

type Store struct {
	db *sql.DB
}

// Open connects to Postgres and verifies the connection with a ping — a
// connection string can be well-formed and still point at a database that
// isn't accepting connections yet, and we want to fail fast at startup
// rather than on the first query.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: opening connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: pinging database: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// SaveRawSnapshot archives an undecoded protobuf payload BEFORE any parsing
// happens. This is what makes the rest of the pipeline recomputable: if a
// bug is later found in internal/reconcile, every derived delay_records row
// can be regenerated from these archived payloads instead of being
// permanently wrong.
func (s *Store) SaveRawSnapshot(ctx context.Context, feedURL string, fetchedAt time.Time, payload []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO raw_feed_snapshots (feed_url, fetched_at, payload) VALUES ($1, $2, $3)`,
		feedURL, fetchedAt, payload,
	)
	if err != nil {
		return fmt.Errorf("store: saving raw snapshot: %w", err)
	}
	return nil
}

// GetOrCreateScheduleVersion returns the id of an existing schedule_versions
// row matching this content hash, or inserts a new one. Deduping by content
// hash matters because the static schedule is re-downloaded periodically but
// doesn't change every time — without this, restarting the poller would
// keep inserting identical "new" schedule versions for the same actual
// schedule content.
func (s *Store) GetOrCreateScheduleVersion(ctx context.Context, v gtfs.ScheduleVersion) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id FROM schedule_versions WHERE content_hash = $1 ORDER BY id DESC LIMIT 1`,
		v.ContentHash,
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("store: looking up schedule version: %w", err)
	}

	err = s.db.QueryRowContext(ctx,
		`INSERT INTO schedule_versions (source_url, downloaded_at, content_hash) VALUES ($1, $2, $3) RETURNING id`,
		v.SourceURL, v.DownloadedAt, v.ContentHash,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: inserting schedule version: %w", err)
	}
	return id, nil
}

// SaveDelayRecord persists one reconciled delay, UPSERTing on the same
// (trip_id, stop_id, scheduled_time) key that reconcile.DelayRecord.Key()
// uses for in-memory dedup (see migrations/001_init.sql's UNIQUE
// constraint). Reprocessing the same live prediction across repeated 15s
// polls overwrites this row with the latest observed actual_time/delay
// instead of inserting a duplicate.
func (s *Store) SaveDelayRecord(ctx context.Context, scheduleVersionID int64, rec reconcile.DelayRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO delay_records (schedule_version_id, trip_id, route_id, stop_id, scheduled_time, actual_time, delay_seconds)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (trip_id, stop_id, scheduled_time)
		DO UPDATE SET
			actual_time = EXCLUDED.actual_time,
			delay_seconds = EXCLUDED.delay_seconds,
			schedule_version_id = EXCLUDED.schedule_version_id,
			computed_at = now()`,
		scheduleVersionID, rec.TripID, rec.RouteID, rec.StopID, rec.ScheduledTime, rec.ActualTime, rec.DelaySeconds,
	)
	if err != nil {
		return fmt.Errorf("store: saving delay record: %w", err)
	}
	return nil
}

// SavePollStats records the outcome of one poll cycle — how many trip
// updates came in, how many turned into saved delay records, and how many
// were skipped, broken down by reason. This is what lets match-rate trends
// be queried directly from Postgres instead of parsed out of Render's log
// output.
func (s *Store) SavePollStats(ctx context.Context, polledAt time.Time, tripUpdatesSeen, delaysSaved, skippedBadDate, skippedNoMatch int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO poll_stats (polled_at, trip_updates_seen, delays_saved, skipped_bad_date, skipped_no_match)
		VALUES ($1, $2, $3, $4, $5)`,
		polledAt, tripUpdatesSeen, delaysSaved, skippedBadDate, skippedNoMatch,
	)
	if err != nil {
		return fmt.Errorf("store: saving poll stats: %w", err)
	}
	return nil
}

// GetDelaysByStop returns delay records for a stop, scheduled at or after
// since, ordered chronologically.
func (s *Store) GetDelaysByStop(ctx context.Context, stopID string, since time.Time) ([]reconcile.DelayRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT trip_id, route_id, stop_id, scheduled_time, actual_time, delay_seconds
		FROM delay_records
		WHERE stop_id = $1 AND scheduled_time >= $2
		ORDER BY scheduled_time`,
		stopID, since,
	)
	if err != nil {
		return nil, fmt.Errorf("store: querying delays by stop: %w", err)
	}
	defer rows.Close()

	// Initialized (not nil) so a stop with zero records serializes to JSON
	// "[]" rather than "null" — callers iterating the response shouldn't
	// need a special case for "no data yet" vs. "some data."
	records := []reconcile.DelayRecord{}
	for rows.Next() {
		var rec reconcile.DelayRecord
		if err := rows.Scan(&rec.TripID, &rec.RouteID, &rec.StopID, &rec.ScheduledTime, &rec.ActualTime, &rec.DelaySeconds); err != nil {
			return nil, fmt.Errorf("store: scanning delay record: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating delay records: %w", err)
	}
	return records, nil
}

// GetDistinctStopsWithDelays returns every stop_id that has at least one
// delay record, so the frontend can populate a "pick a stop" list without
// offering ~1,500 stops that have no data behind them yet.
func (s *Store) GetDistinctStopsWithDelays(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT stop_id FROM delay_records ORDER BY stop_id`)
	if err != nil {
		return nil, fmt.Errorf("store: listing stops with delays: %w", err)
	}
	defer rows.Close()

	stopIDs := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scanning stop id: %w", err)
		}
		stopIDs = append(stopIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating stop ids: %w", err)
	}
	return stopIDs, nil
}

// RouteSummary is an aggregate on-time-performance snapshot for one route
// over a time window. Computed with a single SQL aggregate query — no
// precomputed materialized view yet, per the Phase 2 scope; that's a later
// optimization if these queries ever get slow at scale.
type RouteSummary struct {
	RouteID         string  `json:"route_id"`
	SampleSize      int     `json:"sample_size"`
	AvgDelaySeconds float64 `json:"avg_delay_seconds"`
	P90DelaySeconds float64 `json:"p90_delay_seconds"`
	// PctSevereDelay is the fraction (0..1) of arrivals at or above the
	// severe-delay threshold passed into GetRouteSummary.
	PctSevereDelay float64 `json:"pct_severe_delay"`
}

// GetRouteSummary computes on-time-performance stats for a route over
// records scheduled at or after since. severeDelay defines what counts as
// "badly late" for the PctSevereDelay figure (the frontend's "delayed 5+
// minutes on X% of arrivals" line).
func (s *Store) GetRouteSummary(ctx context.Context, routeID string, since time.Time, severeDelay time.Duration) (RouteSummary, error) {
	summary := RouteSummary{RouteID: routeID}
	err := s.db.QueryRowContext(ctx, `
		SELECT
			count(*),
			coalesce(avg(delay_seconds), 0),
			coalesce(percentile_cont(0.9) WITHIN GROUP (ORDER BY delay_seconds), 0),
			coalesce(avg((delay_seconds >= $3)::int::float8), 0)
		FROM delay_records
		WHERE route_id = $1 AND scheduled_time >= $2`,
		routeID, since, int(severeDelay.Seconds()),
	).Scan(&summary.SampleSize, &summary.AvgDelaySeconds, &summary.P90DelaySeconds, &summary.PctSevereDelay)
	if err != nil {
		return RouteSummary{}, fmt.Errorf("store: computing route summary: %w", err)
	}
	return summary, nil
}
