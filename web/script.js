const stopSelect = document.getElementById("stop-select");
const summaryEl = document.getElementById("summary");
const weekdayMorningEl = document.getElementById("weekday-morning-summary");
const routeSummariesEl = document.getElementById("route-summaries");
const tableCaptionEl = document.getElementById("table-caption");
const tableEl = document.getElementById("delay-table");
const tableBody = tableEl.querySelector("tbody");
const emptyStateEl = document.getElementById("empty-state");

const SEVERE_DELAY_SECONDS = 300; // must match internal/api's severeDelayThreshold
const HISTORY_WINDOW_DAYS = 7; // wide enough to accumulate a handful of weekday mornings, not just today
const MAX_TABLE_ROWS = 50; // the table is a recent-activity view, not a full export
const MIN_WEEKDAY_MORNING_SAMPLES = 5; // below this, a percentage is more misleading than informative
const API_BASE = "https://transit-pulse.onrender.com";

// "Morning" is defined as 6:00am-9:59am, and always evaluated in the
// transit agency's own timezone (America/New_York) — NOT the viewer's
// browser timezone. Without this, someone opening the dashboard from
// another timezone would get silently wrong weekday/hour buckets, since
// scheduled_time's hour-of-day depends entirely on which timezone you read
// it in. Intl.DateTimeFormat with an explicit timeZone is what makes this
// correct regardless of where the browser is.
const nyPartsFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  weekday: "short",
  hour: "numeric",
  hourCycle: "h23",
});

init();

async function init() {
  const stops = await fetchJSON("${API_BASE}/stops");
  stopSelect.innerHTML = "";

  if (!stops || stops.length === 0) {
    const opt = document.createElement("option");
    opt.textContent = "No stops with data yet";
    stopSelect.appendChild(opt);
    return;
  }

  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select a stop…";
  stopSelect.appendChild(placeholder);

  for (const stop of stops) {
    const opt = document.createElement("option");
    opt.value = stop.stop_id;
    opt.textContent = `${stop.name} (${stop.stop_id})`;
    stopSelect.appendChild(opt);
  }

  stopSelect.addEventListener("change", () => {
    if (stopSelect.value) loadStop(stopSelect.value);
  });
}

async function loadStop(stopId) {
  const since = new Date(Date.now() - HISTORY_WINDOW_DAYS * 24 * 60 * 60 * 1000).toISOString();
  const delays = await fetchJSON(`${API_BASE}/stops/${encodeURIComponent(stopId)}/delays?since=${encodeURIComponent(since)}`);

  if (!delays || delays.length === 0) {
    summaryEl.classList.add("hidden");
    weekdayMorningEl.innerHTML = "";
    routeSummariesEl.innerHTML = "";
    tableCaptionEl.classList.add("hidden");
    tableEl.classList.add("hidden");
    emptyStateEl.classList.remove("hidden");
    return;
  }
  emptyStateEl.classList.add("hidden");

  renderSummary(delays);
  renderWeekdayMorningSummary(delays);
  await renderRouteSummaries(delays);
  renderTable(delays);
}

function renderSummary(delays) {
  const severeCount = delays.filter((d) => d.delay_seconds >= SEVERE_DELAY_SECONDS).length;
  const pct = Math.round((severeCount / delays.length) * 100);
  summaryEl.textContent = `Over the last ${HISTORY_WINDOW_DAYS} days, this stop was delayed 5+ minutes on ${pct}% of ${delays.length} recorded arrivals (all routes, all times of day).`;
  summaryEl.classList.remove("hidden");
}

// isWeekdayMorningNY reports whether an ISO timestamp falls on a Monday-Friday,
// 6:00am-9:59am, evaluated in America/New_York regardless of the viewer's
// own timezone.
function isWeekdayMorningNY(iso) {
  const parts = nyPartsFormatter.formatToParts(new Date(iso));
  const weekday = parts.find((p) => p.type === "weekday").value; // "Mon".."Sun"
  const hour = parseInt(parts.find((p) => p.type === "hour").value, 10);
  const isWeekday = weekday !== "Sat" && weekday !== "Sun";
  const isMorning = hour >= 6 && hour < 10;
  return isWeekday && isMorning;
}

// renderWeekdayMorningSummary computes, per route serving this stop:
//   (# weekday-morning arrivals delayed 5+ minutes) / (# weekday-morning arrivals)
// over the last HISTORY_WINDOW_DAYS days, where "weekday morning" is
// Mon-Fri 6:00-9:59am America/New_York (see isWeekdayMorningNY). Routes with
// fewer than MIN_WEEKDAY_MORNING_SAMPLES qualifying arrivals show an
// explicit "not enough data" line instead of a percentage computed from a
// handful of points.
function renderWeekdayMorningSummary(delays) {
  const byRoute = new Map();
  for (const d of delays) {
    if (!isWeekdayMorningNY(d.scheduled_time)) continue;
    if (!byRoute.has(d.route_id)) byRoute.set(d.route_id, []);
    byRoute.get(d.route_id).push(d);
  }

  weekdayMorningEl.innerHTML = "";
  const routeIds = [...byRoute.keys()].sort();

  if (routeIds.length === 0) {
    const li = document.createElement("li");
    li.className = "insufficient-data";
    li.textContent = `No weekday-morning (6-10am Mon-Fri) arrivals recorded yet for any route at this stop, in the last ${HISTORY_WINDOW_DAYS} days.`;
    weekdayMorningEl.appendChild(li);
    return;
  }

  for (const routeId of routeIds) {
    const records = byRoute.get(routeId);
    const li = document.createElement("li");
    if (records.length < MIN_WEEKDAY_MORNING_SAMPLES) {
      li.className = "insufficient-data";
      li.textContent = `Route ${routeId}: not enough weekday-morning data yet (only ${records.length} recorded arrival${records.length === 1 ? "" : "s"} in the last ${HISTORY_WINDOW_DAYS} days).`;
    } else {
      const severeCount = records.filter((d) => d.delay_seconds >= SEVERE_DELAY_SECONDS).length;
      const pct = Math.round((severeCount / records.length) * 100);
      li.textContent = `Route ${routeId} arrivals are 5+ minutes late on ${pct}% of recent weekday mornings (${severeCount} of ${records.length} arrivals, 6-10am Mon-Fri, last ${HISTORY_WINDOW_DAYS} days).`;
    }
    weekdayMorningEl.appendChild(li);
  }
}

async function renderRouteSummaries(delays) {
  const routeIds = [...new Set(delays.map((d) => d.route_id))];
  const summaries = await Promise.all(
    routeIds.map((id) => fetchJSON(`${API_BASE}/routes/${encodeURIComponent(id)}/summary`))
  );

  routeSummariesEl.innerHTML = "";
  summaries.forEach((summary, i) => {
    if (!summary) return;
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <div class="label">Route ${routeIds[i]} — avg delay</div>
      <div class="value">${formatDelay(Math.round(summary.avg_delay_seconds))}</div>
      <div class="label">p90: ${formatDelay(Math.round(summary.p90_delay_seconds))} · ${Math.round(summary.pct_severe_delay * 100)}% severe · n=${summary.sample_size}</div>
    `;
    routeSummariesEl.appendChild(card);
  });
}

function renderTable(delays) {
  tableBody.innerHTML = "";
  // Most recent first — this is a dashboard, not a schedule.
  const sorted = [...delays].sort((a, b) => new Date(b.scheduled_time) - new Date(a.scheduled_time));
  const shown = sorted.slice(0, MAX_TABLE_ROWS);

  for (const d of shown) {
    const tr = document.createElement("tr");
    const delayClass = d.delay_seconds > 0 ? "delay-late" : d.delay_seconds < 0 ? "delay-early" : "";
    tr.innerHTML = `
      <td>${formatTime(d.scheduled_time)}</td>
      <td>${formatTime(d.actual_time)}</td>
      <td>${d.route_id}</td>
      <td class="${delayClass}">${formatDelay(d.delay_seconds)}</td>
    `;
    tableBody.appendChild(tr);
  }

  tableCaptionEl.textContent = sorted.length > MAX_TABLE_ROWS
    ? `Showing the ${MAX_TABLE_ROWS} most recent of ${sorted.length} arrivals in the last ${HISTORY_WINDOW_DAYS} days.`
    : `${sorted.length} arrivals in the last ${HISTORY_WINDOW_DAYS} days.`;
  tableCaptionEl.classList.remove("hidden");
  tableEl.classList.remove("hidden");
}

function formatDelay(seconds) {
  const sign = seconds > 0 ? "+" : seconds < 0 ? "-" : "";
  const abs = Math.abs(seconds);
  const m = Math.floor(abs / 60);
  const s = abs % 60;
  return m > 0 ? `${sign}${m}m${s}s` : `${sign}${s}s`;
}

function formatTime(iso) {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

async function fetchJSON(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url}: ${res.status}`);
    return await res.json();
  } catch (err) {
    console.error(err);
    return null;
  }
}

const wakeNoticeEl = document.getElementById("wake-notice");

init();

async function init() {
  const stops = await fetchJSON("${API_BASE}/stops");
  // The first successful response means the backend is awake — whether
  // it took 200ms (already warm) or 45s (cold start), we only care that
  // it's done now, so the notice can go away.
  wakeNoticeEl.classList.add("wake-notice--done");

  stopSelect.innerHTML = "";
  if (!stops || stops.length === 0) {
    const opt = document.createElement("option");
    opt.textContent = "No stops with data yet";
    stopSelect.appendChild(opt);
    return;
  }
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select a stop…";
  stopSelect.appendChild(placeholder);
  for (const stop of stops) {
    const opt = document.createElement("option");
    opt.value = stop.stop_id;
    opt.textContent = `${stop.name} (${stop.stop_id})`;
    stopSelect.appendChild(opt);
  }
  stopSelect.addEventListener("change", () => {
    if (stopSelect.value) loadStop(stopSelect.value);
  });
}
