#!/usr/bin/env python3
"""Scan GB frequency data for statutory limit breaches.

Statutory basis used by default:
- ESQCR 2002 (Regulation 27): declared frequency 50 Hz with permitted
  variation not exceeding ±1% unless otherwise agreed.
- Default breach thresholds in this script: 49.5 Hz to 50.5 Hz.

Source: https://www.legislation.gov.uk/uksi/2002/2665/regulation/27
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
class BreachWindow:
    breach_type: str
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
    m = re.search(r"f-(\d{4})-(\d{1,2})\.csv$", path.name)
    if m:
        return int(m.group(1)), int(m.group(2)), path.name
    return (9999, 99, path.name)


def list_frequency_files(freq_dir: Path) -> list[Path]:
    files = [p for p in freq_dir.glob("*.csv") if p.is_file()]
    files.sort(key=sort_key_for_file)
    return files


def detect_columns(file_path: Path) -> tuple[str, str]:
    preview = pd.read_csv(file_path, nrows=0)
    cols = list(preview.columns)
    lowered = [str(c).strip().lower() for c in cols]

    if "dtm" in lowered and "f" in lowered:
        return cols[lowered.index("dtm")], cols[lowered.index("f")]

    if "date" in lowered and "value" in lowered:
        return cols[lowered.index("date")], cols[lowered.index("value")]

    if len(cols) >= 2:
        return cols[0], cols[1]

    raise ValueError(f"Unable to detect frequency columns in {file_path}")


def read_frequency_file(file_path: Path) -> pd.DataFrame:
    date_col, value_col = detect_columns(file_path)
    df = pd.read_csv(file_path, usecols=[date_col, value_col])
    df.columns = ["Date", "Value"]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    df = df.dropna(subset=["Date", "Value"]).sort_values("Date")
    return df


def classify(value: float, low: float, high: float) -> Optional[str]:
    if value < low:
        return "under"
    if value > high:
        return "over"
    return None


def finalize_window(window: dict, min_duration_seconds: float) -> Optional[BreachWindow]:
    duration = max((window["end_ts"] - window["start_ts"]).total_seconds(), 0.0)
    if duration < min_duration_seconds:
        return None

    min_hz = float(window["min_hz"])
    max_hz = float(window["max_hz"])
    worst_hz = min_hz if window["type"] == "under" else max_hz

    return BreachWindow(
        breach_type=window["type"],
        start=str(window["start_ts"]),
        end=str(window["end_ts"]),
        duration_seconds=duration,
        points=int(window["points"]),
        min_hz=min_hz,
        max_hz=max_hz,
        worst_hz=float(worst_hz),
    )


def scan_breaches(
    files: Iterable[Path],
    low: float,
    high: float,
    min_duration_seconds: float,
) -> list[BreachWindow]:
    windows: list[BreachWindow] = []
    active: Optional[dict] = None

    for file_path in files:
        df = read_frequency_file(file_path)
        if df.empty:
            continue

        for row in df.itertuples(index=False):
            ts = row.Date
            val = float(row.Value)
            breach = classify(val, low, high)

            if active is None:
                if breach is not None:
                    active = {
                        "type": breach,
                        "start_ts": ts,
                        "end_ts": ts,
                        "points": 1,
                        "min_hz": val,
                        "max_hz": val,
                    }
                continue

            if breach is None:
                finalized = finalize_window(active, min_duration_seconds)
                if finalized is not None:
                    windows.append(finalized)
                active = None
                continue

            if breach == active["type"]:
                active["end_ts"] = ts
                active["points"] += 1
                if val < active["min_hz"]:
                    active["min_hz"] = val
                if val > active["max_hz"]:
                    active["max_hz"] = val
            else:
                finalized = finalize_window(active, min_duration_seconds)
                if finalized is not None:
                    windows.append(finalized)
                active = {
                    "type": breach,
                    "start_ts": ts,
                    "end_ts": ts,
                    "points": 1,
                    "min_hz": val,
                    "max_hz": val,
                }

    if active is not None:
        finalized = finalize_window(active, min_duration_seconds)
        if finalized is not None:
            windows.append(finalized)

    return windows


def summarize(windows: list[BreachWindow]) -> dict:
    under = [w for w in windows if w.breach_type == "under"]
    over = [w for w in windows if w.breach_type == "over"]

    def total_duration(items: list[BreachWindow]) -> float:
        return float(sum(w.duration_seconds for w in items))

    return {
        "total_windows": len(windows),
        "under_windows": len(under),
        "over_windows": len(over),
        "total_duration_seconds": total_duration(windows),
        "under_duration_seconds": total_duration(under),
        "over_duration_seconds": total_duration(over),
        "worst_under_hz": min((w.worst_hz for w in under), default=None),
        "worst_over_hz": max((w.worst_hz for w in over), default=None),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan frequency data for statutory breach windows")
    parser.add_argument(
        "--root",
        default=str(project_root()),
        help="Project root path (defaults to detected GDA root)",
    )
    parser.add_argument("--low", type=float, default=49.5, help="Lower statutory threshold (default 49.5 Hz)")
    parser.add_argument("--high", type=float, default=50.5, help="Upper statutory threshold (default 50.5 Hz)")
    parser.add_argument(
        "--min-duration-seconds",
        type=float,
        default=0.0,
        help="Minimum breach window duration to keep (default 0)",
    )
    parser.add_argument("--max-files", type=int, help="Optional limit for number of input files (for quick tests)")
    parser.add_argument("--json", action="store_true", help="Print full output as JSON")
    parser.add_argument("--output-json", help="Optional path to save full JSON output")
    parser.add_argument("--output-csv", help="Optional path to save breach windows as CSV")
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

    windows = scan_breaches(files, args.low, args.high, args.min_duration_seconds)
    summary = summarize(windows)

    payload = {
        "statutory_basis": {
            "declared_frequency_hz": 50.0,
            "permitted_variation_percent": 1.0,
            "default_band_hz": [49.5, 50.5],
            "reference": "https://www.legislation.gov.uk/uksi/2002/2665/regulation/27",
        },
        "scan_parameters": {
            "root": str(root),
            "files_scanned": len(files),
            "low_hz": args.low,
            "high_hz": args.high,
            "min_duration_seconds": args.min_duration_seconds,
        },
        "summary": summary,
        "breach_windows": [asdict(w) for w in windows],
    }

    if args.output_csv:
        out_csv = Path(args.output_csv)
        pd.DataFrame([asdict(w) for w in windows]).to_csv(out_csv, index=False)

    if args.output_json:
        out_json = Path(args.output_json)
        out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print("=== UK Statutory Frequency Breach Scan ===")
        print("Reference: ESQCR Reg. 27 (50 Hz ±1%)")
        print(f"Band used: {args.low:.3f} to {args.high:.3f} Hz")
        print(f"Files scanned: {len(files)}")
        print(f"Total windows: {summary['total_windows']}")
        print(f"Under-frequency windows: {summary['under_windows']}")
        print(f"Over-frequency windows: {summary['over_windows']}")
        print(f"Total breach duration (s): {summary['total_duration_seconds']:.1f}")
        print(f"Worst under-frequency (Hz): {summary['worst_under_hz']}")
        print(f"Worst over-frequency (Hz): {summary['worst_over_hz']}")
        if windows:
            print("\nTop 10 longest windows:")
            top = sorted(windows, key=lambda w: w.duration_seconds, reverse=True)[:10]
            for window in top:
                print(
                    f"- {window.breach_type:5s} {window.start} -> {window.end} "
                    f"({window.duration_seconds:.1f}s, worst={window.worst_hz:.3f} Hz, points={window.points})"
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
