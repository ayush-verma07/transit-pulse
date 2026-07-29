package api

import (
	"sync"
	"time"

	"github.com/ayush-verma07/transit-pulse/internal/gtfs"
)

// LiveVehicles holds the most recently polled vehicle positions in memory.
//
// This is deliberately NOT persisted to Postgres or read from
// raw_feed_snapshots: a live map only ever needs "where is this vehicle
// right now," never historical positions, so writing ~100 rows to the
// database every 15s (and re-decoding a protobuf blob just to answer a
// request) would be pure overhead for data nobody queries historically.
// The tradeoff: this cache is empty for up to one poll interval after a
// restart, and wouldn't be shared across multiple replicas of this
// process — both fine for the current single-process deployment, and a
// contained change (e.g. Redis, or a real table) if that ever stops being true.
type LiveVehicles struct {
	mu        sync.RWMutex
	positions []gtfs.VehiclePosition
	updatedAt time.Time
}

// Set replaces the cached positions. Called once per poll cycle from
// cmd/tracker's poll loop, right after a successful fetch.
func (c *LiveVehicles) Set(positions []gtfs.VehiclePosition, updatedAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.positions = positions
	c.updatedAt = updatedAt
}

// Get returns the cached positions and when they were captured. Safe to
// call concurrently with Set from a different goroutine (the HTTP server
// and the poll loop run in parallel — see cmd/tracker/main.go).
func (c *LiveVehicles) Get() ([]gtfs.VehiclePosition, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.positions, c.updatedAt
}
