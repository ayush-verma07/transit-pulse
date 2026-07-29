package gtfs

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Route is one row of routes.txt — the subset of columns we actually use.
type Route struct {
	RouteID   string
	ShortName string
	LongName  string
	Color     string // hex, no leading "#" (GTFS convention), e.g. "EE352E" for the 1/2/3 red. Empty if the agency didn't set one.
}

// Trip is one row of trips.txt: which route a trip belongs to, and which
// calendar service_id determines the days it actually runs.
type Trip struct {
	TripID      string
	RouteID     string
	ServiceID   string
	Headsign    string
	DirectionID string
}

// StopTime is one scheduled stop within a trip. Arrival/Departure are stored
// as an offset from midnight rather than time.Time: GTFS explicitly allows
// values past 24:00:00 (e.g. "25:30:00") to represent a trip that starts
// before midnight and continues into the next service day without being
// considered a "new day" for scheduling purposes. A time.Duration preserves
// that value correctly; a time.Time would force an awkward day rollover.
type StopTime struct {
	StopID       string
	StopSequence int
	Arrival      time.Duration
	Departure    time.Duration
}

// Stop is one row of stops.txt.
type Stop struct {
	StopID    string
	Name      string
	ParentID  string
	Latitude  float64
	Longitude float64
}

// CalendarService is one row of calendar.txt: which weekdays a service_id is active on.
type CalendarService struct {
	ServiceID string
	Weekday   [7]bool // index 0 = Monday ... 6 = Sunday, matching GTFS calendar.txt column order
	StartDate string  // YYYYMMDD
	EndDate   string  // YYYYMMDD
}

// ScheduleVersion identifies exactly which download of the static schedule
// a Schedule was built from. MTA republishes this feed periodically, and a
// delay computed against last month's schedule is meaningless once trip
// times have changed — every computed delay must reference the version that
// was active when it was computed.
type ScheduleVersion struct {
	SourceURL    string
	DownloadedAt time.Time
	ContentHash  string // sha256 of the raw zip bytes; detects "MTA re-served an identical file" vs. an actual change
	FeedVersion  string // from feed_info.txt, e.g. "20260526" — MTA's own version label, when present
}

// Schedule is the in-memory, queryable form of one static GTFS download.
// In-memory (rather than DB-backed) is a deliberate Phase 1 simplification:
// the subway schedule is ~40MB uncompressed, which comfortably fits in
// memory, and keeping lookups as plain map access avoids adding schedule
// queries to the DB's critical path on every reconciliation. If the dataset
// ever grows past what's comfortable in memory, this is the seam to swap for
// a DB-backed lookup without changing callers.
type Schedule struct {
	Version   ScheduleVersion
	Routes    map[string]Route
	Trips     map[string]Trip
	StopTimes map[string][]StopTime // keyed by trip_id, sorted by StopSequence
	Stops     map[string]Stop
	Services  map[string]CalendarService

	// routeStopIndex and indexOnce back NearestScheduledArrival. It's built
	// lazily rather than eagerly in FetchStatic so that Schedule values
	// constructed by hand in tests (see internal/reconcile) get the same
	// indexed lookup behavior without needing to know this exists.
	indexOnce      sync.Once
	routeStopIndex map[string][]time.Duration // key: routeID + "|" + stopID
}

// ScheduledTime returns the scheduled arrival offset for a given trip/stop,
// by exact trip_id. NOTE: for MTA's subway realtime feed this is USELESS as
// a way to match a live prediction to its schedule, because realtime and
// static trip_ids use entirely different, non-overlapping formats (verified
// empirically: 0 of 158 live trip_ids matched a static trip_id in a sample
// poll). It's kept because it's still meaningful if trip_id ever IS shared
// between a caller's realtime and static data (e.g. a different feed, or a
// unit test using synthetic matching IDs). For subway reconciliation, see
// NearestScheduledArrival instead, and internal/reconcile/delay.go for why.
func (s *Schedule) ScheduledTime(tripID, stopID string) (time.Duration, bool) {
	for _, st := range s.StopTimes[tripID] {
		if st.StopID == stopID {
			return st.Arrival, true
		}
	}
	return 0, false
}

// NearestScheduledArrival returns the scheduled arrival offset (from
// midnight of the service day) on the given route+stop that is closest to
// actualOffset, provided it's within tolerance. It returns ok=false if no
// scheduled arrival for that route+stop exists within tolerance at all.
//
// Matching by (route_id, stop_id) rather than trip_id is deliberate: NYCT
// stop_ids already encode platform direction as an N/S suffix (e.g. "127S"
// vs "127N" are different stop_ids for the two directions at the same
// physical station), so route+stop is already direction-specific without
// needing TripDescriptor.DirectionId, which NYCT's feed doesn't reliably
// populate anyway.
func (s *Schedule) NearestScheduledArrival(routeID, stopID string, actualOffset, tolerance time.Duration) (time.Duration, bool) {
	s.buildRouteStopIndex()

	candidates := s.routeStopIndex[routeID+"|"+stopID]
	if len(candidates) == 0 {
		return 0, false
	}

	best := candidates[0]
	bestDiff := absDuration(actualOffset - best)
	for _, c := range candidates[1:] {
		if diff := absDuration(actualOffset - c); diff < bestDiff {
			best, bestDiff = c, diff
		}
	}
	if bestDiff > tolerance {
		return 0, false
	}
	return best, true
}

func (s *Schedule) buildRouteStopIndex() {
	s.indexOnce.Do(func() {
		s.routeStopIndex = make(map[string][]time.Duration)
		for tripID, stops := range s.StopTimes {
			routeID := s.Trips[tripID].RouteID
			for _, st := range stops {
				key := routeID + "|" + st.StopID
				s.routeStopIndex[key] = append(s.routeStopIndex[key], st.Arrival)
			}
		}
		// Sorted mainly so debugging/printing candidate lists reads sanely;
		// the lookup itself is a linear scan since per-stop candidate counts
		// are small (a handful of scheduled trips per route per day).
		for k := range s.routeStopIndex {
			sort.Slice(s.routeStopIndex[k], func(i, j int) bool {
				return s.routeStopIndex[k][i] < s.routeStopIndex[k][j]
			})
		}
	})
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// FetchStatic downloads MTA's static GTFS schedule zip, parses it in memory,
// and returns a Schedule tagged with the version metadata needed to keep
// future delay comparisons honest about which schedule they were measured
// against.
func FetchStatic(ctx context.Context, sourceURL string) (*Schedule, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, fmt.Errorf("gtfs: building request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gtfs: fetching static schedule: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gtfs: static schedule returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gtfs: reading static schedule body: %w", err)
	}
	downloadedAt := time.Now()

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("gtfs: opening zip: %w", err)
	}

	hash := sha256.Sum256(body)
	schedule := &Schedule{
		Version: ScheduleVersion{
			SourceURL:    sourceURL,
			DownloadedAt: downloadedAt,
			ContentHash:  hex.EncodeToString(hash[:]),
		},
		Routes:    make(map[string]Route),
		Trips:     make(map[string]Trip),
		StopTimes: make(map[string][]StopTime),
		Stops:     make(map[string]Stop),
		Services:  make(map[string]CalendarService),
	}

	if err := parseCSVFile(zr, "feed_info.txt", func(row map[string]string) error {
		schedule.Version.FeedVersion = row["feed_version"]
		return nil
	}); err != nil && !isMissingFile(err) {
		// feed_info.txt is optional in the GTFS spec, but MTA includes it;
		// only a real parse error should fail the whole load.
		return nil, err
	}

	if err := parseCSVFile(zr, "routes.txt", func(row map[string]string) error {
		schedule.Routes[row["route_id"]] = Route{
			RouteID:   row["route_id"],
			ShortName: row["route_short_name"],
			LongName:  row["route_long_name"],
			Color:     row["route_color"],
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseCSVFile(zr, "trips.txt", func(row map[string]string) error {
		schedule.Trips[row["trip_id"]] = Trip{
			TripID:      row["trip_id"],
			RouteID:     row["route_id"],
			ServiceID:   row["service_id"],
			Headsign:    row["trip_headsign"],
			DirectionID: row["direction_id"],
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseCSVFile(zr, "stops.txt", func(row map[string]string) error {
		lat, err := strconv.ParseFloat(row["stop_lat"], 64)
		if err != nil {
			return fmt.Errorf("gtfs: parsing stop_lat %q: %w", row["stop_lat"], err)
		}
		lon, err := strconv.ParseFloat(row["stop_lon"], 64)
		if err != nil {
			return fmt.Errorf("gtfs: parsing stop_lon %q: %w", row["stop_lon"], err)
		}
		schedule.Stops[row["stop_id"]] = Stop{
			StopID:    row["stop_id"],
			Name:      row["stop_name"],
			ParentID:  row["parent_station"],
			Latitude:  lat,
			Longitude: lon,
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseCSVFile(zr, "calendar.txt", func(row map[string]string) error {
		schedule.Services[row["service_id"]] = CalendarService{
			ServiceID: row["service_id"],
			Weekday: [7]bool{
				row["monday"] == "1",
				row["tuesday"] == "1",
				row["wednesday"] == "1",
				row["thursday"] == "1",
				row["friday"] == "1",
				row["saturday"] == "1",
				row["sunday"] == "1",
			},
			StartDate: row["start_date"],
			EndDate:   row["end_date"],
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := parseCSVFile(zr, "stop_times.txt", func(row map[string]string) error {
		seq, err := strconv.Atoi(row["stop_sequence"])
		if err != nil {
			return fmt.Errorf("gtfs: parsing stop_sequence %q: %w", row["stop_sequence"], err)
		}
		arrival, err := parseGTFSTime(row["arrival_time"])
		if err != nil {
			return fmt.Errorf("gtfs: parsing arrival_time %q: %w", row["arrival_time"], err)
		}
		departure, err := parseGTFSTime(row["departure_time"])
		if err != nil {
			return fmt.Errorf("gtfs: parsing departure_time %q: %w", row["departure_time"], err)
		}
		tripID := row["trip_id"]
		schedule.StopTimes[tripID] = append(schedule.StopTimes[tripID], StopTime{
			StopID:       row["stop_id"],
			StopSequence: seq,
			Arrival:      arrival,
			Departure:    departure,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	// stop_times.txt is conventionally ordered by trip then sequence, but
	// that ordering isn't part of the GTFS spec guarantee — sort explicitly
	// so downstream code can rely on StopTimes[tripID] being in stop order.
	for tripID, stops := range schedule.StopTimes {
		sort.Slice(stops, func(i, j int) bool {
			return stops[i].StopSequence < stops[j].StopSequence
		})
		schedule.StopTimes[tripID] = stops
	}

	return schedule, nil
}

type missingFileError struct{ name string }

func (e missingFileError) Error() string { return fmt.Sprintf("gtfs: %s not found in zip", e.name) }

func isMissingFile(err error) bool {
	_, ok := err.(missingFileError)
	return ok
}

// parseCSVFile reads one file from the GTFS zip and invokes fn once per data
// row, with columns keyed by header name. Keying by name (rather than
// assuming column order) is deliberate: GTFS doesn't guarantee column order,
// and agencies add/reorder optional columns over time.
func parseCSVFile(zr *zip.Reader, name string, fn func(row map[string]string) error) error {
	var f *zip.File
	for _, candidate := range zr.File {
		if candidate.Name == name {
			f = candidate
			break
		}
	}
	if f == nil {
		return missingFileError{name: name}
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("gtfs: opening %s: %w", name, err)
	}
	defer rc.Close()

	r := csv.NewReader(rc)
	r.ReuseRecord = true

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("gtfs: reading %s header: %w", name, err)
	}
	// Copy header since ReuseRecord means r.Read() reuses header's backing array.
	columns := append([]string(nil), header...)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("gtfs: reading %s row: %w", name, err)
		}

		row := make(map[string]string, len(columns))
		for i, col := range columns {
			if i < len(record) {
				row[col] = record[i]
			}
		}
		if err := fn(row); err != nil {
			return fmt.Errorf("gtfs: processing %s row: %w", name, err)
		}
	}

	return nil
}

// parseGTFSTime parses GTFS's "HH:MM:SS" time-of-day format, which allows
// hours >= 24 for trips that run past midnight (see StopTime doc comment).
// time.ParseDuration can't be used directly since it doesn't accept this format.
func parseGTFSTime(s string) (time.Duration, error) {
	var h, m, sec int
	if _, err := fmt.Sscanf(s, "%d:%d:%d", &h, &m, &sec); err != nil {
		return 0, err
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(sec)*time.Second, nil
}
