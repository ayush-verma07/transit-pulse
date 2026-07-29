# TransitPulse

**Real-Time Transit Reliability Tracker**

TransitPulse answers a question no transit agency publishes directly: *is my bus actually on time?*

It polls a transit agency's free GTFS-Realtime feed at regular intervals, decodes live vehicle positions and trip updates, and reconciles each one against the agency's static published schedule to compute real delay statistics — by stop, by route, and by hour of day.

## The Problem

Transit agencies publish two separate data feeds:
- **Static GTFS** — the official, scheduled timetable
- **GTFS-Realtime** — live vehicle positions and trip updates, refreshed every few seconds

Neither feed tells you the thing you actually want to know: *how late is this route usually, and is it running late right now?* TransitPulse bridges that gap by continuously reconciling the two feeds and surfacing real reliability data.

## What Makes This Hard

The core engineering challenge isn't fetching data — it's reconciliation:

- **Deduplication**: consecutive polls repeat the same vehicle predictions and must be deduplicated idempotently on a natural key, or delay stats get double-counted.
- **Vehicle churn**: vehicles disappear and reappear on the feed, and trip IDs get recycled across service days.
- **Schedule drift**: agencies republish their static schedule every few weeks. Historical comparisons have to reference the schedule version that was active at the time, or old data silently becomes invalid.
- **Raw data preservation**: every raw protobuf payload is archived before parsing, so the entire derived dataset can be recomputed from scratch if a bug is found in the reconciliation logic.

## Tech Stack

- **Go** — feed polling, protobuf decoding, reconciliation logic
- **PostgreSQL** — stores raw feed snapshots and derived delay records
- **Protocol Buffers** — GTFS-Realtime's native encoding format
- **React** — frontend dashboard for live delay visualization

## Status

Project is now live!

## Running Locally

### Prerequisites
- [Go](https://go.dev/dl/) 1.2x or later
- [Docker](https://www.docker.com/products/docker-desktop/) (for local Postgres)
- `psql` (PostgreSQL client) — install via `brew install postgresql@16` on macOS

### Setup

1. Clone the repo:
```bash
   git clone https://github.com/ayush-verma07/transitpulse.git
   cd transitpulse
```

2. Start a local Postgres instance:
```bash
   docker-compose up -d
```

3. Run the database migrations:
```bash
   psql "postgres://transitpulse:transitpulse@localhost:5432/transitpulse?sslmode=disable" -f migrations/001_init.sql
```

4. Run the backend:
```bash
   go run cmd/tracker/main.go
```
   By default, this connects to the local Postgres instance from step 2. To point at a different database, set the `TRANSITPULSE_DATABASE_DSN` environment variable:
```bash
   TRANSITPULSE_DATABASE_DSN="postgres://user:pass@host:5432/dbname" go run cmd/tracker/main.go
```

5. Open the frontend:
   Open `web/index.html` directly in your browser, or serve it locally:
```bash
   cd web
   python3 -m http.server 8000
```
   Then visit `http://localhost:8000`.

   Note: the frontend's `API_BASE` constant in `script.js` and `map.js` is currently set to the deployed Render URL. To test against your local backend instead, temporarily change `API_BASE` to `http://localhost:PORT` (whatever port your Go server listens on).

### Live Demo
A live version is deployed at [Live Project](https://transit-pulse-alpha.vercel.app/) — frontend on Vercel, backend on Render, database on Neon.
