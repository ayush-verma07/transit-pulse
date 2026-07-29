// Live vehicle-position map. Kept in its own file, separate from script.js's
// stop-lookup page, since the two features don't share any state or DOM.

const REFRESH_MS = 20000; // a bit slower than the backend's 15s poll cycle, deliberately not in lockstep with it
const DEFAULT_MARKER_COLOR = "#6b7280"; // used when a route has no color set in routes.txt

const mapStatusEl = document.getElementById("map-status");

// CARTO's "Positron" basemap style: free, vector tiles, explicitly built for
// use with MapLibre GL JS, no API key or account required.
// https://carto.com/basemaps
const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  center: [-73.98, 40.75], // roughly midtown Manhattan
  zoom: 11,
});
map.addControl(new maplibregl.NavigationControl(), "top-right");

// One MapLibre Marker per vehicle, keyed by trip_id, reused across refreshes
// so markers move smoothly instead of being destroyed and recreated (which
// would make them flicker) every 20 seconds.
const markersByTripId = new Map();

refreshVehicles();
setInterval(refreshVehicles, REFRESH_MS);

async function refreshVehicles() {
  let data;
  try {
    const res = await fetch("/vehicles/live");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (err) {
    console.error("refreshVehicles failed:", err);
    mapStatusEl.textContent = "Couldn't load live vehicle positions.";
    return;
  }

  const seenTripIds = new Set();

  for (const v of data.vehicles) {
    seenTripIds.add(v.trip_id);
    // Multiple vehicles can legitimately share the same stop_id at once
    // (e.g. two trains queued at the same station), which would otherwise
    // stack their markers exactly on top of each other. A small, stable
    // per-trip offset spreads them out visually without meaningfully
    // exaggerating the inaccuracy already inherent in "approximate to
    // nearest stop" (see the note above the map).
    const [dLng, dLat] = jitter(v.trip_id);
    const lngLat = [v.longitude + dLng, v.latitude + dLat];
    const color = v.color ? `#${v.color}` : DEFAULT_MARKER_COLOR;

    let marker = markersByTripId.get(v.trip_id);
    if (!marker) {
      const el = document.createElement("div");
      el.className = "vehicle-marker";
      const popup = new maplibregl.Popup({ offset: 12 }).setHTML(popupHTML(v));
      marker = new maplibregl.Marker({ element: el }).setLngLat(lngLat).setPopup(popup).addTo(map);
      markersByTripId.set(v.trip_id, marker);
    } else {
      marker.setLngLat(lngLat);
      marker.getPopup().setHTML(popupHTML(v));
    }
    marker.getElement().style.backgroundColor = color;
  }

  // Vehicles that disappeared from the feed (trip ended, or dropped off —
  // see internal/reconcile's package doc on this exact edge case) get their
  // markers removed rather than left stale on the map forever.
  for (const [tripId, marker] of markersByTripId) {
    if (!seenTripIds.has(tripId)) {
      marker.remove();
      markersByTripId.delete(tripId);
    }
  }

  const polledAt = new Date(data.polled_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  mapStatusEl.textContent = `${data.vehicles.length} vehicles · last updated ${polledAt}`;
}

const STATUS_LABELS = {
  INCOMING_AT: "Approaching",
  STOPPED_AT: "Stopped at",
  IN_TRANSIT_TO: "In transit to",
};

function popupHTML(v) {
  const time = new Date(v.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const statusLabel = STATUS_LABELS[v.status] || v.status;
  return `<strong>Route ${v.route_id}</strong><br>${statusLabel} ${v.stop_name}<br>reported ${time}`;
}

// Deterministic per-trip offset (not random) so a marker's jitter direction
// stays fixed across refreshes instead of visibly jumping every 20s.
function jitter(tripId) {
  let hash = 0;
  for (let i = 0; i < tripId.length; i++) {
    hash = (hash * 31 + tripId.charCodeAt(i)) | 0;
  }
  const angle = (hash % 360) * (Math.PI / 180);
  const radius = 0.0006; // degrees; roughly 50-60m at NYC's latitude
  return [Math.cos(angle) * radius, Math.sin(angle) * radius];
}
