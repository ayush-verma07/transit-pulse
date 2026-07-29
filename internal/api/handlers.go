// Package api exposes delay data over HTTP. Handlers only translate between
// HTTP request/response and calls into internal/store — no business logic
// lives here (matching/dedup/aggregation is internal/reconcile's and
// internal/store's job).
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ayush-verma07/transit-pulse/internal/gtfs"
	"github.com/ayush-verma07/transit-pulse/internal/store"
)

// defaultWindow bounds how far back a request looks when the caller doesn't
// supply a "since" query parameter — without this, an empty query string
// would otherwise require scanning the entire delay_records table.
const defaultWindow = 24 * time.Hour

// severeDelayThreshold is what counts as "badly late" for the on-time
// summary the frontend shows ("delayed 5+ minutes on X% of arrivals").
const severeDelayThreshold = 5 * time.Minute

// Handlers holds everything the API needs to answer a request. All three
// fields are shared with cmd/tracker's poll loop — the schedule is loaded
// once at startup and never mutated, *store.Store is already safe for
// concurrent use (it just wraps *sql.DB, which pools connections), and
// *LiveVehicles guards its own state with a mutex.
type Handlers struct {
	Store    *store.Store
	Schedule *gtfs.Schedule
	Vehicles *LiveVehicles
}

// NewMux wires up the routes and also serves the static frontend from
// webDir at "/". Go's ServeMux (1.22+) resolves the most specific pattern
// for a given request regardless of registration order, so the literal
// API paths below and the catch-all static file handler can coexist
// without a separate URL prefix like "/api".
func NewMux(h *Handlers, webDir string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stops", h.listStops)
	mux.HandleFunc("GET /stops/{stop_id}/delays", h.stopDelays)
	mux.HandleFunc("GET /routes/{route_id}/summary", h.routeSummary)
	mux.HandleFunc("GET /vehicles/live", h.liveVehicles)
	mux.Handle("/", http.FileServer(http.Dir(webDir)))
	return mux
}

// listStops returns every stop that has at least one delay record, enriched
// with a human-readable name from the in-memory static schedule. This
// exists so the frontend has something to populate its stop picker with,
// rather than the ~1,500 stops in the full schedule, most of which have no
// data yet.
func (h *Handlers) listStops(w http.ResponseWriter, r *http.Request) {
	stopIDs, err := h.Store.GetDistinctStopsWithDelays(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	type stopInfo struct {
		StopID string `json:"stop_id"`
		Name   string `json:"name"`
	}
	out := make([]stopInfo, 0, len(stopIDs))
	for _, id := range stopIDs {
		name := id
		if s, ok := h.Schedule.Stops[id]; ok {
			name = s.Name
		}
		out = append(out, stopInfo{StopID: id, Name: name})
	}
	writeJSON(w, http.StatusOK, out)
}

// stopDelays returns recent delay records for one stop.
func (h *Handlers) stopDelays(w http.ResponseWriter, r *http.Request) {
	stopID := r.PathValue("stop_id")
	since := parseSince(r)

	records, err := h.Store.GetDelaysByStop(r.Context(), stopID, since)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, records)
}

// routeSummary returns aggregate on-time-performance stats for one route.
func (h *Handlers) routeSummary(w http.ResponseWriter, r *http.Request) {
	routeID := r.PathValue("route_id")
	since := parseSince(r)

	summary, err := h.Store.GetRouteSummary(r.Context(), routeID, since, severeDelayThreshold)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

// liveVehicles returns the most recently polled position of every tracked
// vehicle, from the in-memory cache (see vehicles.go for why this isn't
// backed by Postgres).
//
// POSITION IS APPROXIMATE, NOT GPS: MTA's subway feed never populates
// VehiclePosition's lat/lon (see the doc comment on gtfs.VehiclePosition —
// verified empirically, 0/87 in a sample poll). Instead, each vehicle is
// plotted at the coordinates of its current/next stop_id, looked up from the
// static schedule. A vehicle whose stop_id isn't in the schedule (shouldn't
// happen, but defensively) is dropped rather than plotted at (0,0).
func (h *Handlers) liveVehicles(w http.ResponseWriter, r *http.Request) {
	positions, updatedAt := h.Vehicles.Get()

	type vehicleOut struct {
		TripID    string    `json:"trip_id"`
		RouteID   string    `json:"route_id"`
		Color     string    `json:"color"` // hex, no "#"; empty if the route has none in routes.txt
		StopID    string    `json:"stop_id"`
		StopName  string    `json:"stop_name"`
		Status    string    `json:"status"` // "INCOMING_AT" / "STOPPED_AT" / "IN_TRANSIT_TO", relative to StopID
		Latitude  float64   `json:"latitude"`
		Longitude float64   `json:"longitude"`
		Timestamp time.Time `json:"timestamp"` // when THIS vehicle's status was last reported, per GTFS-RT
	}

	out := make([]vehicleOut, 0, len(positions))
	for _, p := range positions {
		stop, ok := h.Schedule.Stops[p.StopID]
		if !ok {
			continue
		}
		out = append(out, vehicleOut{
			TripID:    p.TripID,
			RouteID:   p.RouteID,
			Color:     h.Schedule.Routes[p.RouteID].Color,
			StopID:    p.StopID,
			StopName:  stop.Name,
			Status:    p.Status,
			Latitude:  stop.Latitude,
			Longitude: stop.Longitude,
			Timestamp: p.Timestamp,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"polled_at": updatedAt,
		"vehicles":  out,
	})
}

// parseSince reads an optional RFC3339 "since" query parameter, falling
// back to defaultWindow. A malformed value is treated the same as a missing
// one rather than a 400 — this is a read-only convenience endpoint, not a
// strict API contract, so failing open to a sane default is preferable to
// rejecting the request.
func parseSince(r *http.Request) time.Time {
	if raw := r.URL.Query().Get("since"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t
		}
	}
	return time.Now().Add(-defaultWindow)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
