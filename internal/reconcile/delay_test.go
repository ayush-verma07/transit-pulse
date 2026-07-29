package reconcile

import (
	"testing"
	"time"

	"github.com/ayush-verma07/transit-pulse/internal/gtfs"
)

// fakeSchedule builds a minimal in-memory schedule by hand — no zip, no
// network, no DB — exercising the same Schedule type FetchStatic returns.
func fakeSchedule() *gtfs.Schedule {
	return &gtfs.Schedule{
		Trips: map[string]gtfs.Trip{
			"STATIC_TRIP_1": {TripID: "STATIC_TRIP_1", RouteID: "2"},
		},
		StopTimes: map[string][]gtfs.StopTime{
			"STATIC_TRIP_1": {
				{StopID: "127S", StopSequence: 1, Arrival: 22*time.Hour + 15*time.Minute},
			},
		},
	}
}

func TestReconcile_ComputesPositiveDelayWhenLate(t *testing.T) {
	schedule := fakeSchedule()
	serviceDate, err := ParseServiceDate("20260728")
	if err != nil {
		t.Fatalf("ParseServiceDate: %v", err)
	}

	// Live prediction arrives 90 seconds after the scheduled 22:15:00.
	actual := serviceDate.Add(22*time.Hour + 16*time.Minute + 30*time.Second)
	tu := gtfs.TripUpdate{TripID: "127900_5..S08X013", RouteID: "2", StartDate: "20260728"}
	st := gtfs.StopTimeUpdate{StopID: "127S", Arrival: &actual}

	rec, err := Reconcile(schedule, serviceDate, tu, st)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rec.DelaySeconds != 90 {
		t.Errorf("DelaySeconds = %d, want 90", rec.DelaySeconds)
	}
	// The realtime trip_id is preserved on the record even though it never
	// matched the static schedule's trip_id — that's the whole point of
	// matching by (route, stop) instead.
	if rec.TripID != "127900_5..S08X013" {
		t.Errorf("TripID = %q, want the realtime trip_id preserved", rec.TripID)
	}
}

func TestReconcile_ComputesNegativeDelayWhenEarly(t *testing.T) {
	schedule := fakeSchedule()
	serviceDate, _ := ParseServiceDate("20260728")

	actual := serviceDate.Add(22*time.Hour + 14*time.Minute) // 60s early
	tu := gtfs.TripUpdate{RouteID: "2"}
	st := gtfs.StopTimeUpdate{StopID: "127S", Arrival: &actual}

	rec, err := Reconcile(schedule, serviceDate, tu, st)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rec.DelaySeconds != -60 {
		t.Errorf("DelaySeconds = %d, want -60", rec.DelaySeconds)
	}
}

func TestReconcile_NoScheduledArrivalWithinTolerance(t *testing.T) {
	schedule := fakeSchedule()
	serviceDate, _ := ParseServiceDate("20260728")

	// 3 hours away from the only scheduled arrival at this stop — well
	// outside matchTolerance, so this should NOT be forced into a match.
	actual := serviceDate.Add(1 * time.Hour)
	tu := gtfs.TripUpdate{RouteID: "2"}
	st := gtfs.StopTimeUpdate{StopID: "127S", Arrival: &actual}

	if _, err := Reconcile(schedule, serviceDate, tu, st); err == nil {
		t.Fatal("expected an error for a prediction with no plausible scheduled match, got nil")
	}
}

func TestReconcile_MissingArrivalPrediction(t *testing.T) {
	schedule := fakeSchedule()
	serviceDate, _ := ParseServiceDate("20260728")

	// Some stop-time updates only carry a departure (e.g. origin terminals);
	// Reconcile should reject these rather than silently treat delay as 0.
	st := gtfs.StopTimeUpdate{StopID: "127S", Arrival: nil}
	tu := gtfs.TripUpdate{RouteID: "2"}

	if _, err := Reconcile(schedule, serviceDate, tu, st); err == nil {
		t.Fatal("expected an error when arrival prediction is nil, got nil")
	}
}

func TestDelayRecord_KeyDistinguishesRecycledTripIDAcrossDays(t *testing.T) {
	// Same trip_id, same stop, but two different service days — this is the
	// "trip ID recycling" edge case from the package doc. The keys must
	// differ, or a delay record from today would silently overwrite one
	// from a completely unrelated trip yesterday.
	day1, _ := ParseServiceDate("20260728")
	day2, _ := ParseServiceDate("20260729")

	rec1 := DelayRecord{TripID: "127900_5..S08X013", StopID: "127S", ScheduledTime: day1.Add(22 * time.Hour)}
	rec2 := DelayRecord{TripID: "127900_5..S08X013", StopID: "127S", ScheduledTime: day2.Add(22 * time.Hour)}

	if rec1.Key() == rec2.Key() {
		t.Fatal("Key() collided across different service days for a recycled trip_id")
	}
}

func TestDelayRecord_KeyStableAcrossRepeatedPollsOfSamePrediction(t *testing.T) {
	// The whole point of Key(): the same prediction observed on two
	// different 15s polls must dedupe to the same key so storage can UPSERT
	// instead of inserting a duplicate row.
	serviceDate, _ := ParseServiceDate("20260728")
	scheduled := serviceDate.Add(22 * time.Hour)

	pollOne := DelayRecord{TripID: "127900_5..S08X013", StopID: "127S", ScheduledTime: scheduled, DelaySeconds: 30}
	pollTwo := DelayRecord{TripID: "127900_5..S08X013", StopID: "127S", ScheduledTime: scheduled, DelaySeconds: 45}

	if pollOne.Key() != pollTwo.Key() {
		t.Fatal("Key() did not dedupe two polls of the same real-world prediction")
	}
}
