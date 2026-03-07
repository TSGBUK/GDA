# RoCoF-App (WebSocket Streaming Version)

This is the app-focused version of the RoCoF dashboard with the same layout pattern as `RoCoF-Reply`, but data is streamed over WebSockets.

## What it does

- Accepts a user-selected start date/time.
- Streams data at 1-second cadence until stopped.
- Uses parquet-backed inputs (frequency, demand, inertia, generation).
- Supports live playback speed updates (no reconnect/restart).
- Supports quick seek-back controls (`-60s`, `-15m`, `-30m`).
- Keeps client memory bounded by trimming chart histories to ~60 seconds.

## 1) Ensure parquet data exists

These were run for the required datasets:

- `Frequency/Processors/parquet_data_conversion.py`
- `DemandData/Processors/parquet_data_conversion.py`
- `Inertia/Processors/parquet_data_conversion.py`
- `HistoricalGenerationData/Processors/parquet_data_conversion.py`

## 2) Run server

From repo root:

```bash
c:/Users/shir1/Documents/GitHub/GDA/.venv/Scripts/python.exe DataVisualizations/RoCoF-App/server.py
```

Server URL:

- `http://127.0.0.1:8765/`
- WebSocket endpoint: `ws://127.0.0.1:8765/ws/replay`

## 3) Use the app

- Open the page URL above.
- Set `Start Date`.
- Click `Connect`, then `Start`.
- Adjust base FPS and speed buttons while streaming (applies immediately).
- Use seek-back buttons to jump backward while staying connected.
- Click `Stop` to halt playback.

## Memory behavior

The client intentionally flushes old data to keep browser memory low:

- RoCoF/Frequency/RPM/Torque histories keep approximately the latest 60 seconds.
- Instant balance sparkline keeps approximately the latest 60 seconds.
- Event console lines are capped.
- No full-frame buffer is retained on the client.
