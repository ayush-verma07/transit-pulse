// Package gtfs handles both halves of GTFS data: the realtime feed (this file)
// and the static schedule (static.go). Everything outside this package works
// with the plain Go structs defined below — no caller needs to know that the
// realtime feed is protobuf under the hood.
package gtfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	pb "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

// StopTimeUpdate is one predicted arrival/departure at one stop, for one trip.
type StopTimeUpdate struct {
	StopID       string
	StopSequence uint32
	// Arrival/Departure are pointers because MTA's feed frequently supplies
	// only one of the two (e.g. arrival but no departure for a terminal stop),
	// and zero-value time.Time would be indistinguishable from "midnight UTC".
	Arrival   *time.Time
	Departure *time.Time
}

// TripUpdate is one trip's full set of predicted stop times, as of one poll.
type TripUpdate struct {
	TripID    string
	RouteID   string
	StartDate string // service date the trip belongs to, format YYYYMMDD (see reconcile package for why this matters)
	StopTimes []StopTimeUpdate
}

// VehiclePosition is a vehicle's last-reported status, as of one poll.
//
// NOTE ON Latitude/Longitude: these decode whatever the feed's Position
// field contains, per the standard GTFS-RT schema — but MTA's subway feed
// never populates it (verified empirically: 0 of 87 live entities in a
// sample poll had nonzero lat/lon). This is because NYCT subway signaling
// is track-circuit based, not GPS; the feed only ever tells you a train's
// current/next StopID, not a coordinate. Both fields are kept here anyway
// since this package decodes the general GTFS-RT spec, not MTA subway
// specifically — a different feed (e.g. MTA's bus feed) may populate them.
// Callers working with the subway feed should approximate position from
// StopID via the static schedule's Stop.Latitude/Longitude instead (see
// internal/api's liveVehicles handler).
type VehiclePosition struct {
	TripID    string
	RouteID   string
	StopID    string // current or next stop, per GTFS-RT semantics
	Status    string // "INCOMING_AT", "STOPPED_AT", or "IN_TRANSIT_TO" relative to StopID
	Latitude  float32
	Longitude float32
	Timestamp time.Time
}

// FeedSnapshot is everything decoded from a single poll of the realtime feed.
type FeedSnapshot struct {
	FetchedAt        time.Time
	FeedURL          string
	RawPayload       []byte // undecoded protobuf bytes, kept so the caller can archive it before parsing
	TripUpdates      []TripUpdate
	VehiclePositions []VehiclePosition
}

// FetchFeed performs one HTTP GET against a GTFS-Realtime endpoint, decodes
// the protobuf FeedMessage, and returns clean Go structs plus the raw bytes.
//
// It deliberately does not loop or sleep — the 15-second polling cadence is
// an orchestration concern that belongs in cmd/tracker/main.go, not here.
// Keeping this function to "one fetch in, one snapshot out" is what makes it
// trivially testable later with an httptest.Server instead of a live call.
func FetchFeed(ctx context.Context, feedURL string) (*FeedSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("gtfs: building request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gtfs: fetching feed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gtfs: feed returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gtfs: reading response body: %w", err)
	}
	// Captured immediately after the read completes, so it reflects when we
	// actually received the data rather than when decoding finished.
	fetchedAt := time.Now()

	var feed pb.FeedMessage
	if err := proto.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("gtfs: decoding protobuf: %w", err)
	}

	snapshot := &FeedSnapshot{
		FetchedAt:  fetchedAt,
		FeedURL:    feedURL,
		RawPayload: body,
	}

	for _, entity := range feed.GetEntity() {
		if tu := entity.GetTripUpdate(); tu != nil {
			snapshot.TripUpdates = append(snapshot.TripUpdates, convertTripUpdate(tu))
		}
		if vp := entity.GetVehicle(); vp != nil {
			snapshot.VehiclePositions = append(snapshot.VehiclePositions, convertVehiclePosition(vp))
		}
	}

	return snapshot, nil
}

func convertTripUpdate(tu *pb.TripUpdate) TripUpdate {
	out := TripUpdate{
		TripID:    tu.GetTrip().GetTripId(),
		RouteID:   tu.GetTrip().GetRouteId(),
		StartDate: tu.GetTrip().GetStartDate(),
	}
	for _, stu := range tu.GetStopTimeUpdate() {
		out.StopTimes = append(out.StopTimes, StopTimeUpdate{
			StopID:       stu.GetStopId(),
			StopSequence: stu.GetStopSequence(),
			Arrival:      stopTimeEventToTime(stu.GetArrival()),
			Departure:    stopTimeEventToTime(stu.GetDeparture()),
		})
	}
	return out
}

// stopTimeEventToTime returns nil when the event itself is absent, rather
// than a zero time.Time, so callers can tell "not predicted" apart from
// "predicted at the Unix epoch."
func stopTimeEventToTime(ev *pb.TripUpdate_StopTimeEvent) *time.Time {
	if ev == nil {
		return nil
	}
	t := time.Unix(ev.GetTime(), 0)
	return &t
}

func convertVehiclePosition(vp *pb.VehiclePosition) VehiclePosition {
	return VehiclePosition{
		TripID:    vp.GetTrip().GetTripId(),
		RouteID:   vp.GetTrip().GetRouteId(),
		StopID:    vp.GetStopId(),
		Status:    vp.GetCurrentStatus().String(),
		Latitude:  vp.GetPosition().GetLatitude(),
		Longitude: vp.GetPosition().GetLongitude(),
		Timestamp: time.Unix(int64(vp.GetTimestamp()), 0),
	}
}
