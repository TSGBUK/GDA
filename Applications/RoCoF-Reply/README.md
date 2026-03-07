# RoCoF-Reply Dashboard

This dashboard replays sample-to-sample RoCoF frames from `DeriveRoCoF` replay JSON.

## 1) Generate replay JSON

Run:

```bash
python Random/DeriveRoCoF.py --max-files 1 --row-stride 1 --timestamp-mode midpoint --snapshot-seconds 900 --fps 30 --output-replay-json DataVisualizations/RoCoF-Reply/derived_rocof_replay.json --output-json Random/derived_rocof_sample_summary.json --output-csv Random/derived_rocof_sample.csv
```

## 2) Open dashboard

Open `DataVisualizations/RoCoF-Reply/index.html`.

If default JSON is not auto-loaded, use the JSON file picker and select `derived_rocof_replay.json`.

## Layout

- Left sidebar: replay control menu (GVA, H constants, FPS, play/pause, JSON picker) + generation/interconnector dials
- Center workspace: 2x2 real-time charts (RoCoF, Frequency, RPM, Torque) with timeline and event console
- Right sidebar: system total cards (generation, demand, flow, inertia, frame)
- Left and right sidebars can be collapsed to maximize chart area

## Notes

- Replay FPS is configurable and defaults to 30.
- Snapshot mode above uses native 1-second samples and a 15-minute window (`900` seconds).
- `estimated_demand_mw` is sourced from nearest demand (`ND` then `TSD`) and falls back to generation if demand is unavailable.
- Current transport is JSON replay; UI is structured so a future SignalR/WebSocket stream can replace static frame loading without changing chart/status components.
