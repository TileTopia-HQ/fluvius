# Fluvius

Real-time geospatial stream processor. Sub-second latency processing for continuous spatial data streams — GPS tracks, IoT sensors, vehicle telemetry, drone feeds.

## Features

- **Geofencing** — Enter/exit detection for polygon boundaries
- **Proximity alerts** — Distance-based triggers between moving entities
- **Trajectory analysis** — Smoothing, anomaly detection, statistics
- **Windowing** — Tumbling, sliding, session, and count-based windows
- **Watermarks** — Event-time processing with late event handling
- **State management** — Thread-safe operator state with DashMap
- **Connectors** — WebSocket source/sink, JSON lines file I/O
- **Pipeline** — Composable operator chains with async Tokio runtime

## Quick Start

```bash
# Process events from a file
fluvius run --input events.jsonl --output results.jsonl --min-speed 5.0

# Geofence monitoring
fluvius geofence --input events.jsonl --bounds "-74.0,40.7,-73.9,40.8" --zone-name manhattan

# Proximity detection
fluvius proximity --input events.jsonl --threshold 100.0

# Trajectory analysis
fluvius trajectory --input events.jsonl --max-speed 50.0

# Live WebSocket processing
fluvius serve --source-bind 127.0.0.1:9001 --sink-bind 127.0.0.1:9002
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Sources    │────▶│  Operators   │────▶│    Sinks     │
│             │     │              │     │              │
│ • WebSocket │     │ • Geofence   │     │ • WebSocket  │
│ • File      │     │ • Proximity  │     │ • File       │
│ • MQTT      │     │ • Trajectory │     │ • Console    │
│ • Kafka     │     │ • Filter     │     │              │
│             │     │ • Transform  │     │              │
└─────────────┘     │ • RateLimit  │     └──────────────┘
                    │ • Window     │
                    └──────────────┘
```

## License

AGPL-3.0-or-later
