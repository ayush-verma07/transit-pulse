// Package reconcile computes on-time performance by matching a live GTFS-RT
// stop-time prediction against MTA's static schedule and measuring the gap.
//
// KNOWN EDGE CASES (Phase 1 — flagged, not fully solved):
//
//  1. Trip ID recycling across service days: NYCT's realtime trip_id format
//     is only unique within a single service day; the same trip_id string
//     can legitimately reappear tomorrow for an unrelated trip. DelayRecord's
//     Key() includes ScheduledTime specifically so that a recycled trip_id
//     on a different day produces a different key rather than colliding.
//     A bare trip_id is NOT safe as a long-lived identifier on its own.
//
//  2. Vehicles disappearing/reappearing: if a vehicle drops off the feed for
//     one or more 15s polls and then reappears with a revised prediction for
//     the same stop, Reconcile has no memory of the earlier poll — it just
//     computes a fresh DelayRecord each time. Correctness here relies on the
//     caller UPSERTing by Key() (see migrations/001_init.sql's UNIQUE
//     constraint) so the most recent prediction wins, not on this package
//     detecting the gap.
//
//  3. Ambiguous nearest-time matches: on a sufficiently delayed or
//     bunched-up route, two distinct physical trips can both have scheduled
//     arrivals close to the same live prediction, and NearestScheduledArrival
//     could pick the wrong one. Not solved in Phase 1 — would need a
//     persistent per-vehicle trip identity carried across polls to resolve.
//
//  4. Calendar exceptions unhandled: internal/gtfs.Schedule's route+stop
//     index pools scheduled arrivals across ALL service_ids (weekday,
//     Saturday, Sunday, and any calendar_dates.txt exceptions), rather than
//     filtering to just the service_id active on the given date. On routes
//     with a materially different weekend schedule, this can pick a
//     scheduled time from the wrong day type.
package reconcile

import (
	"fmt"
	"time"

	_ "time/tzdata" // embed the IANA timezone database in the binary, so LoadLocation below works even on minimal deploy images without system tzdata

	"github.com/ayush-verma07/transit-pulse/internal/gtfs"
)

// matchTolerance bounds how far from a live prediction we'll search for a
// scheduled arrival before concluding "no match" rather than risk pairing a
// prediction with the wrong trip. Subway headways are usually well under
// this window even when delayed, so most live predictions have exactly one
// plausible candidate; this exists to reject the rare case of none.
const matchTolerance = 20 * time.Minute

// nyLocation is where MTA's static schedule times are meant to be
// interpreted — GTFS stop_times are local wall-clock times for the transit
// agency's operating region, not UTC.
var nyLocation = func() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic("reconcile: could not load America/New_York timezone: " + err.Error())
	}
	return loc
}()

// DelayRecord is the result of reconciling one live stop-time prediction
// against the static schedule.
type DelayRecord struct {
	TripID        string    `json:"trip_id"` // the REALTIME trip_id — see Key() for why this is safe to use despite not matching the static schedule's trip_id
	RouteID       string    `json:"route_id"`
	StopID        string    `json:"stop_id"`
	ScheduledTime time.Time `json:"scheduled_time"`
	ActualTime    time.Time `json:"actual_time"`
	DelaySeconds  int       `json:"delay_seconds"`
}

// Key is the natural idempotency key for this record. The same
// (trip_id, stop_id, scheduled_time) triple identifies one real-world
// scheduled arrival, no matter how many 15-second polls observe a
// prediction for it before the vehicle actually passes — so reprocessing the
// same prediction should overwrite the existing row for this key (an
// UPSERT), not insert a duplicate. See migrations/001_init.sql.
//
// Why trip_id is safe to use here despite NOT matching the static schedule's
// trip_id (see package doc): this key only needs to be stable across
// repeated polls of the *same* live prediction, not globally unique or tied
// to any external identifier. NYCT's realtime trip_id is stable for the
// lifetime of one trip within one service day — exactly the guarantee dedup
// needs. Including ScheduledTime (which is anchored to a specific calendar
// date, not just a time-of-day) is what protects this key from the trip_id
// recycling edge case described in the package doc: the same trip_id on a
// different day produces a different ScheduledTime and therefore a
// different key.
func (d DelayRecord) Key() string {
	return fmt.Sprintf("%s|%s|%d", d.TripID, d.StopID, d.ScheduledTime.Unix())
}

// ParseServiceDate turns a GTFS-RT TripDescriptor.StartDate (format
// YYYYMMDD) into midnight of that service day in the agency's local
// timezone — the reference point that scheduled arrival offsets (which can
// exceed 24h, see gtfs.StopTime) are measured from.
func ParseServiceDate(startDate string) (time.Time, error) {
	t, err := time.ParseInLocation("20060102", startDate, nyLocation)
	if err != nil {
		return time.Time{}, fmt.Errorf("reconcile: parsing service date %q: %w", startDate, err)
	}
	return t, nil
}

// Reconcile matches one live stop-time prediction against the static
// schedule and computes the delay between them. See the package doc for the
// matching strategy and its known edge cases.
func Reconcile(schedule *gtfs.Schedule, serviceDate time.Time, tu gtfs.TripUpdate, st gtfs.StopTimeUpdate) (*DelayRecord, error) {
	if st.Arrival == nil {
		return nil, fmt.Errorf("reconcile: stop %s on trip %s has no arrival prediction", st.StopID, tu.TripID)
	}
	actual := *st.Arrival
	actualOffset := actual.Sub(serviceDate)

	scheduledOffset, ok := schedule.NearestScheduledArrival(tu.RouteID, st.StopID, actualOffset, matchTolerance)
	if !ok {
		return nil, fmt.Errorf("reconcile: no scheduled arrival within %s for route %s stop %s", matchTolerance, tu.RouteID, st.StopID)
	}
	scheduledTime := serviceDate.Add(scheduledOffset)

	return &DelayRecord{
		TripID:        tu.TripID,
		RouteID:       tu.RouteID,
		StopID:        st.StopID,
		ScheduledTime: scheduledTime,
		ActualTime:    actual,
		DelaySeconds:  int(actual.Sub(scheduledTime).Seconds()),
	}, nil
}
