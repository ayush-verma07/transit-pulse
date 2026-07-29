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

🚧 In active development. See [project board / milestones] for current progress.

## Running Locally

```bash
# instructions coming as the project develops
```
