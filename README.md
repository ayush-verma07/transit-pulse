# transit-pulse
Real-time transit reliability tracker that polls GTFS-Realtime feeds, reconciles live vehicle positions against static schedules, and computes on-time performance by stop, route, and hour. Built in Go with PostgreSQL, featuring idempotent delay deduplication and historical schedule versioning.
