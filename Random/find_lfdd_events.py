#!/usr/bin/env python3
"""Find LFDD candidate events from GB frequency data.

Definition used here:
- LFDD candidate window = contiguous period where frequency is strictly below 49.00 Hz.

This script scans `GDA/DataSources/NESO/Frequency/*.csv`, detects windows, and reports
start/end/duration/severity metrics.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


@dataclass
class LfddEvent:
    start: str
    end: str
    duration_seconds: float
    points: int
    min_hz: float
    max_hz: float
    worst_hz: float


def project_root() -> Path:
    return next(p for p in Path(__file__).resolve().parents if p.name == "GDA")


def sort_key_for_file(path: Path) -> tuple[int, int, str]:
    match = re.search(r"f-(\d{4})-(\d{1,2})\.csv$", path.name)
    if match:
        return int(match.group(1)), int(match.group(2)), path.name
    return (9999, 99, path.name)


def list_frequency_files(freq_dir: Path) -> list[Path]:
    files = [path for path in freq_dir.glob("*.csv") if path.is_file()]
    files.sort(key=sort_key_for_file)
    return files


def detect_columns(file_path: Path) -> tuple[str, str]:
    preview = pd.read_csv(file_path, nrows=0)
    cols = list(preview.columns)
    lowered = [str(column).strip().lower() for column in cols]

    if "dtm" in lowered and "f" in lowered:
        return cols[lowered.index("dtm")], cols[lowered.index("f")]

    if "date" in lowered and "value" in lowered:
        return cols[lowered.index("date")], cols[lowered.index("value")]

    if len(cols) >= 2:
        return cols[0], cols[1]

    raise ValueError(f"Unable to detect timestamp/value columns in {file_path}")


def read_frequency_file(file_path: Path) -> pd.DataFrame:
    date_col, value_col = detect_columns(file_path)
    df = pd.read_csv(file_path, usecols=[date_col, value_col])
    df.columns = ["Date", "Value"]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    return df.dropna(subset=["Date", "Value"]).sort_values("Date")


def finalize_event(active: dict, min_duration_seconds: float) -> Optional[LfddEvent]:
    duration = max((active["end_ts"] - active["start_ts"]).total_seconds(), 0.0)
    if duration < min_duration_seconds:
        return None

    return LfddEvent(
        start=str(active["start_ts"]),
        end=str(active["end_ts"]),
        duration_seconds=duration,
        points=int(active["points"]),
        min_hz=float(active["min_hz"]),
        max_hz=float(active["max_hz"]),
        worst_hz=float(active["min_hz"]),
    )


def find_lfdd_events(
    files: Iterable[Path],
    threshold_hz: float,
    min_duration_seconds: float,
) -> list[LfddEvent]:
    events: list[LfddEvent] = []
    active: Optional[dict] = None

    for file_path in files:
        df = read_frequency_file(file_path)
        if df.empty:
            continue

        for row in df.itertuples(index=False):
            ts = row.Date
            val = float(row.Value)
            below = val < threshold_hz

            if active is None:
                if below:
                    active = {
                        "start_ts": ts,
                        "end_ts": ts,
                        "points": 1,
                        "min_hz": val,
                        "max_hz": val,
                    }
                continue

            if below:
                active["end_ts"] = ts
                active["points"] += 1
                if val < active["min_hz"]:
                    active["min_hz"] = val
                if val > active["max_hz"]:
                    active["max_hz"] = val
            else:
                event = finalize_event(active, min_duration_seconds)
                if event is not None:
                    events.append(event)
                active = None

    if active is not None:
        event = finalize_event(active, min_duration_seconds)
        if event is not None:
            events.append(event)

    return events


def summarize(events: list[LfddEvent]) -> dict:
    return {
        "total_events": len(events),
        "total_duration_seconds": float(sum(event.duration_seconds for event in events)),
        "worst_hz": min((event.worst_hz for event in events), default=None),
        "longest_duration_seconds": max((event.duration_seconds for event in events), default=0.0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Find low-frequency demand disconnection candidate events")
    parser.add_argument(
        "--root",
        default=str(project_root()),
        help="Project root path (defaults to detected GDA root)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=49.0,
        help="LFDD threshold in Hz (default: 49.0)",
    )
    parser.add_argument(
        "--min-duration-seconds",
        type=float,
        default=0.0,
        help="Minimum event duration to keep (default: 0)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        help="Optional limit of input files for quick testing",
    )
    parser.add_argument("--json", action="store_true", help="Print full payload as JSON")
    parser.add_argument("--output-json", help="Optional path to save full JSON payload")
    parser.add_argument("--output-csv", help="Optional path to save events CSV")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    freq_dir = root / "DataSources" / "NESO" / "Frequency"
    if not freq_dir.exists():
        print(f"[error] Frequency folder not found: {freq_dir}")
        return 1

    files = list_frequency_files(freq_dir)
    if args.max_files is not None:
        files = files[: max(args.max_files, 0)]

    if not files:
        print("[error] No frequency CSV files found")
        return 1

    events = find_lfdd_events(files, threshold_hz=args.threshold, min_duration_seconds=args.min_duration_seconds)
    summary = summarize(events)

    payload = {
        "definition": {
            "event": "contiguous period where frequency is strictly below threshold",
            "threshold_hz": args.threshold,
            "default_threshold_hz": 49.0,
        },
        "scan_parameters": {
            "root": str(root),
            "files_scanned": len(files),
            "min_duration_seconds": args.min_duration_seconds,
        },
        "summary": summary,
        "events": [asdict(event) for event in events],
    }

    if args.output_csv:
        out_csv = Path(args.output_csv)
        pd.DataFrame([asdict(event) for event in events]).to_csv(out_csv, index=False)

    if args.output_json:
        out_json = Path(args.output_json)
        out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print("=== LFDD Candidate Event Scan (<49.00 Hz) ===")
        print(f"Threshold used: {args.threshold:.3f} Hz")
        print(f"Files scanned: {len(files)}")
        print(f"Total events: {summary['total_events']}")
        print(f"Total event duration (s): {summary['total_duration_seconds']:.1f}")
        print(f"Worst observed Hz: {summary['worst_hz']}")
        print(f"Longest event (s): {summary['longest_duration_seconds']:.1f}")

        if events:
            print("\nTop 10 longest LFDD candidate events:")
            top = sorted(events, key=lambda item: item.duration_seconds, reverse=True)[:10]
            for event in top:
                print(
                    f"- {event.start} -> {event.end} "
                    f"({event.duration_seconds:.1f}s, worst={event.worst_hz:.3f} Hz, points={event.points})"
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
