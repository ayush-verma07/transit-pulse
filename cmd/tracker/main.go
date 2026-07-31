// Command tracker is the transit-pulse binary. It only wires things
// together: load config, connect to Postgres, run the poll loop, shut down
// cleanly. All real logic lives in internal/gtfs, internal/reconcile, and
// internal/store.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ayush-verma07/transit-pulse/internal/api"
	"github.com/ayush-verma07/transit-pulse/internal/gtfs"
	"github.com/ayush-verma07/transit-pulse/internal/reconcile"
	"github.com/ayush-verma07/transit-pulse/internal/store"
)

// config holds everything main needs to wire the app together. It lives
// here, not its own package, because loading it is itself an orchestration
// concern — main.go's job — not business logic.
type config struct {
	realtimeFeedURL string
	staticFeedURL   string
	databaseDSN     string
	pollInterval    time.Duration
	pollTimeout     time.Duration
	httpAddr        string
	webDir          string
}

func loadConfig() config {
	return config{
		realtimeFeedURL: getenv("TRANSITPULSE_REALTIME_URL", "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"),
		staticFeedURL:   getenv("TRANSITPULSE_STATIC_URL", "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"),
		databaseDSN:     getenv("TRANSITPULSE_DATABASE_DSN", "postgres://transitpulse:transitpulse@localhost:5432/transitpulse?sslmode=disable"),
		pollInterval:    15 * time.Second,
		pollTimeout:     10 * time.Second,
		httpAddr:        getenv("TRANSITPULSE_HTTP_ADDR", ":8080"),
		webDir:          getenv("TRANSITPULSE_WEB_DIR", "web"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	cfg := loadConfig()

	// ctx is canceled on Ctrl+C (SIGINT) or SIGTERM, so the loop below stops
	// cleanly between polls instead of being killed mid-write.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.databaseDSN)
	if err != nil {
		log.Fatalf("tracker: connecting to database: %v", err)
	}
	defer db.Close()

	// The static schedule is fetched once at startup and held in memory for
	// the process lifetime — it changes on the order of weeks, not seconds,
	// so re-downloading it every 15s poll would be wasted bandwidth for no
	// benefit. A scheduled periodic refresh is a reasonable Phase 2 addition
	// if a long-running process needs to pick up a mid-run schedule change.
	log.Printf("tracker: downloading static schedule from %s", cfg.staticFeedURL)
	schedule, err := gtfs.FetchStatic(ctx, cfg.staticFeedURL)
	if err != nil {
		log.Fatalf("tracker: loading static schedule: %v", err)
	}
	scheduleVersionID, err := db.GetOrCreateScheduleVersion(ctx, schedule.Version)
	if err != nil {
		log.Fatalf("tracker: saving schedule version: %v", err)
	}
	log.Printf("tracker: schedule loaded (feed_version=%s, %d trips), schedule_version_id=%d",
		schedule.Version.FeedVersion, len(schedule.Trips), scheduleVersionID)

	vehicles := &api.LiveVehicles{}
	httpServer := &http.Server{
		Addr:    cfg.httpAddr,
		Handler: api.NewMux(&api.Handlers{Store: db, Schedule: schedule, Vehicles: vehicles}, cfg.webDir),
	}

	// The poll loop and the HTTP server are two independent goroutines
	// sharing the same db connection pool, in-memory schedule, and vehicle
	// cache. Both stop when ctx is canceled (Ctrl+C / SIGTERM); wg.Wait()
	// below ensures main doesn't return until both have actually finished,
	// not just been asked to.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		runPollLoop(ctx, cfg, db, schedule, scheduleVersionID, vehicles)
	}()

	go func() {
		defer wg.Done()
		log.Printf("tracker: serving HTTP on %s", cfg.httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("tracker: http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("tracker: shutdown signal received, stopping")

	// http.Server needs an explicit, separate Shutdown call — canceling ctx
	// doesn't stop ListenAndServe on its own. A fresh (non-canceled) context
	// with its own short timeout is used here specifically because ctx is
	// already canceled at this point.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("tracker: http server shutdown error: %v", err)
	}

	wg.Wait()
}

func runPollLoop(ctx context.Context, cfg config, db *store.Store, schedule *gtfs.Schedule, scheduleVersionID int64, vehicles *api.LiveVehicles) {
	ticker := time.NewTicker(cfg.pollInterval)
	defer ticker.Stop()

	log.Printf("tracker: polling %s every %s", cfg.realtimeFeedURL, cfg.pollInterval)
	poll(ctx, cfg, db, schedule, scheduleVersionID, vehicles) // run once immediately rather than waiting for the first tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll(ctx, cfg, db, schedule, scheduleVersionID, vehicles)
		}
	}
}

// poll runs one fetch -> reconcile -> store cycle. Errors within a cycle are
// logged, not fatal: a transient network blip on one poll shouldn't crash a
// long-running service that will just succeed on the next tick 15s later.
func poll(parent context.Context, cfg config, db *store.Store, schedule *gtfs.Schedule, scheduleVersionID int64, vehicles *api.LiveVehicles) {
	ctx, cancel := context.WithTimeout(parent, cfg.pollTimeout)
	defer cancel()

	snapshot, err := gtfs.FetchFeed(ctx, cfg.realtimeFeedURL)
	if err != nil {
		log.Printf("tracker: poll failed: %v", err)
		return
	}

	// Update the live-vehicle cache unconditionally, even if the reconcile
	// steps below fail — a live map showing slightly-stale positions is far
	// better than one that goes blank because an unrelated DB write hiccupped.
	vehicles.Set(snapshot.VehiclePositions, snapshot.FetchedAt)

	saved, skippedBadDate, skippedNoMatch := 0, 0, 0
	for _, tu := range snapshot.TripUpdates {
		serviceDate, err := reconcile.ParseServiceDate(tu.StartDate)
		if err != nil {
			skippedBadDate++
			continue
		}
		for _, st := range tu.StopTimes {
			rec, err := reconcile.Reconcile(schedule, serviceDate, tu, st)
			if err != nil {
				skippedNoMatch++
				continue
			}
			if err := db.SaveDelayRecord(ctx, scheduleVersionID, *rec); err != nil {
				log.Printf("tracker: saving delay record failed: %v", err)
				continue
			}
			saved++
		}
	}

	log.Printf("tracker: poll complete — %d trip updates, %d delays saved, %d skipped (bad date), %d skipped (no match within tolerance)",
		len(snapshot.TripUpdates), saved, skippedBadDate, skippedNoMatch)

	// Recorded with the same fetch timestamp used for the raw snapshot and
	// live-vehicle cache above, so poll_stats rows line up with the other
	// per-cycle data instead of drifting from wall-clock time.Now() calls.
	if err := db.SavePollStats(ctx, snapshot.FetchedAt, len(snapshot.TripUpdates), saved, skippedBadDate, skippedNoMatch); err != nil {
		log.Printf("tracker: saving poll stats failed: %v", err)
	}
}
