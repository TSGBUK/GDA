#!/usr/bin/env python3
"""Derive sample-to-sample RoCoF and align with online generation.

This script scans Frequency CSV files, computes RoCoF between consecutive
samples, aligns each RoCoF point timestamp (start/end/midpoint), then joins
the nearest HistoricalGenerationData row so the output shows online
generation context for each RoCoF sample.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


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


def read_frequency_file(file_path: Path, row_stride: int = 1) -> pd.DataFrame:
    date_col, value_col = detect_columns(file_path)
    frame = pd.read_csv(file_path, usecols=[date_col, value_col])
    if row_stride > 1:
        frame = frame.iloc[::row_stride].copy()

    frame.columns = ["Date", "f"]
    frame["Date"] = pd.to_datetime(
        frame["Date"],
        errors="coerce",
        utc=True,
        format="%d/%m/%Y %H:%M:%S",
    )
    if frame["Date"].isna().mean() > 0.2:
        frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce", utc=True, dayfirst=True)
    frame["f"] = pd.to_numeric(frame["f"], errors="coerce")
    frame = frame.dropna(subset=["Date", "f"]).sort_values("Date")
    return frame


def load_frequency(files: list[Path], row_stride: int = 1) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for file_path in files:
        part = read_frequency_file(file_path, row_stride=row_stride)
        if not part.empty:
            parts.append(part)

    if not parts:
        raise ValueError("No valid frequency rows were parsed from input files")

    frame = pd.concat(parts, ignore_index=True)
    frame = frame.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    return frame


def derive_rocof_samples(freq: pd.DataFrame, timestamp_mode: str = "midpoint") -> pd.DataFrame:
    out = freq.copy()
    out["sample_start_ts"] = out["Date"].shift(1)
    out["f_start_hz"] = out["f"].shift(1)
    out = out.rename(columns={"Date": "sample_end_ts", "f": "f_end_hz"})

    dt = (out["sample_end_ts"] - out["sample_start_ts"]).dt.total_seconds()
    out["delta_t_s"] = dt
    out["delta_f_hz"] = out["f_end_hz"] - out["f_start_hz"]
    out["rocof_hz_per_s"] = out["delta_f_hz"] / out["delta_t_s"]
    out = out.replace([np.inf, -np.inf], np.nan)

    out = out.dropna(subset=["sample_start_ts", "sample_end_ts", "f_start_hz", "f_end_hz", "delta_t_s", "rocof_hz_per_s"])
    out = out[out["delta_t_s"] > 0]

    if timestamp_mode == "start":
        out["rocof_timestamp"] = out["sample_start_ts"]
    elif timestamp_mode == "end":
        out["rocof_timestamp"] = out["sample_end_ts"]
    else:
        out["rocof_timestamp"] = out["sample_start_ts"] + pd.to_timedelta(out["delta_t_s"] / 2.0, unit="s")

    out = out[
        [
            "sample_start_ts",
            "sample_end_ts",
            "rocof_timestamp",
            "f_start_hz",
            "f_end_hz",
            "delta_t_s",
            "delta_f_hz",
            "rocof_hz_per_s",
        ]
    ].sort_values("rocof_timestamp")
    return out


def load_generation_context(root: Path) -> pd.DataFrame:
    generation_file = root / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"
    if not generation_file.exists():
        raise FileNotFoundError(f"Generation file not found: {generation_file}")

    gen = pd.read_csv(generation_file, parse_dates=["DATETIME"])
    gen["DATETIME"] = pd.to_datetime(gen["DATETIME"], errors="coerce", utc=True)
    gen = gen.dropna(subset=["DATETIME"]).sort_values("DATETIME")

    keep_cols = [
        "DATETIME",
        "GENERATION",
        "GAS",
        "COAL",
        "NUCLEAR",
        "WIND",
        "WIND_EMB",
        "SOLAR",
        "HYDRO",
        "BIOMASS",
        "STORAGE",
        "IMPORTS",
        "OTHER",
        "CARBON_INTENSITY",
        "FOSSIL",
        "RENEWABLE",
        "LOW_CARBON",
        "ZERO_CARBON",
    ]
    cols = [column for column in keep_cols if column in gen.columns]
    gen = gen[cols].copy()

    fuel_cols = [
        column
        for column in ["GAS", "COAL", "NUCLEAR", "WIND", "WIND_EMB", "SOLAR", "HYDRO", "BIOMASS", "STORAGE", "IMPORTS", "OTHER"]
        if column in gen.columns
    ]
    if fuel_cols:
        gen["MODELLED_ONLINE_MW"] = gen[fuel_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, skipna=True)

    return gen


def _settlement_to_datetime_utc(date_series: pd.Series, period_series: pd.Series) -> pd.Series:
    base = pd.to_datetime(date_series, errors="coerce", utc=True, format="mixed", dayfirst=True)
    period = pd.to_numeric(period_series, errors="coerce").fillna(1).astype(int)
    period = period.clip(lower=1)
    return base + pd.to_timedelta((period - 1) * 30, unit="m")


def load_demand_context(root: Path) -> pd.DataFrame:
    demand_files = sorted([path for path in (root / "DataSources" / "NESO" / "DemandData").glob("demanddata_*.csv") if path.is_file()])
    if not demand_files:
        return pd.DataFrame(columns=["demand_ts", "ND", "TSD"])

    parts: list[pd.DataFrame] = []
    for file_path in demand_files:
        frame = pd.read_csv(file_path)
        required = {"SETTLEMENT_DATE", "SETTLEMENT_PERIOD"}
        if not required.issubset(frame.columns):
            continue

        frame["demand_ts"] = _settlement_to_datetime_utc(frame["SETTLEMENT_DATE"], frame["SETTLEMENT_PERIOD"])
        flow_cols = [column for column in frame.columns if column.endswith("_FLOW")]
        cols = [column for column in ["demand_ts", "ND", "TSD", *flow_cols] if column in frame.columns]
        if len(cols) < 2:
            continue

        parts.append(frame[cols])

    if not parts:
        return pd.DataFrame(columns=["demand_ts", "ND", "TSD"])

    demand = pd.concat(parts, ignore_index=True)
    demand["demand_ts"] = pd.to_datetime(demand["demand_ts"], errors="coerce", utc=True)
    demand = demand.dropna(subset=["demand_ts"]).sort_values("demand_ts")
    for col in ["ND", "TSD"]:
        if col in demand.columns:
            demand[col] = pd.to_numeric(demand[col], errors="coerce")

    flow_cols = [column for column in demand.columns if column.endswith("_FLOW")]
    for col in flow_cols:
        demand[col] = pd.to_numeric(demand[col], errors="coerce")
    if flow_cols:
        demand["NET_INTERCONNECTOR_FLOW"] = demand[flow_cols].sum(axis=1, skipna=True)
    return demand


def to_ns_epoch_key(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    try:
        dt = dt.astype("datetime64[ns, UTC]")
    except (TypeError, ValueError):
        pass
    return dt.astype("int64")


def align_with_generation(rocof: pd.DataFrame, generation: pd.DataFrame, tolerance_minutes: int) -> pd.DataFrame:
    left = rocof.sort_values("rocof_timestamp").copy()
    right = generation.sort_values("DATETIME").copy()

    left["_join_ts_ns"] = to_ns_epoch_key(left["rocof_timestamp"])
    right["_join_ts_ns"] = to_ns_epoch_key(right["DATETIME"])
    left = left.sort_values("_join_ts_ns")
    right = right.sort_values("_join_ts_ns")

    aligned = pd.merge_asof(
        left,
        right,
        on="_join_ts_ns",
        direction="nearest",
        tolerance=int(pd.Timedelta(minutes=tolerance_minutes).value),
    )
    aligned = aligned.drop(columns=["_join_ts_ns"], errors="ignore")
    aligned = aligned.rename(columns={"DATETIME": "matched_generation_ts"})

    if "matched_generation_ts" in aligned.columns:
        aligned["generation_match_offset_s"] = (
            aligned["rocof_timestamp"] - aligned["matched_generation_ts"]
        ).dt.total_seconds().abs()

    return aligned


def align_with_demand(frame: pd.DataFrame, demand: pd.DataFrame, tolerance_minutes: int) -> pd.DataFrame:
    if demand.empty or "demand_ts" not in demand.columns:
        out = frame.copy()
        out["matched_demand_ts"] = pd.NaT
        return out

    left = frame.sort_values("rocof_timestamp").copy()
    right = demand.sort_values("demand_ts").copy()

    left["_join_ts_ns"] = to_ns_epoch_key(left["rocof_timestamp"])
    right["_join_ts_ns"] = to_ns_epoch_key(right["demand_ts"])
    left = left.sort_values("_join_ts_ns")
    right = right.sort_values("_join_ts_ns")

    aligned = pd.merge_asof(
        left,
        right,
        on="_join_ts_ns",
        direction="nearest",
        tolerance=int(pd.Timedelta(minutes=tolerance_minutes).value),
    )
    aligned = aligned.drop(columns=["_join_ts_ns"], errors="ignore")
    aligned = aligned.rename(columns={"demand_ts": "matched_demand_ts"})
    if "matched_demand_ts" in aligned.columns:
        aligned["demand_match_offset_s"] = (
            aligned["rocof_timestamp"] - aligned["matched_demand_ts"]
        ).dt.total_seconds().abs()
    return aligned


def add_estimated_demand(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "ND" in out.columns:
        out["estimated_demand_mw"] = pd.to_numeric(out["ND"], errors="coerce")
    else:
        out["estimated_demand_mw"] = np.nan

    if "TSD" in out.columns:
        out["estimated_demand_mw"] = out["estimated_demand_mw"].fillna(pd.to_numeric(out["TSD"], errors="coerce"))

    if "GENERATION" in out.columns:
        out["estimated_demand_mw"] = out["estimated_demand_mw"].fillna(pd.to_numeric(out["GENERATION"], errors="coerce"))
    elif "MODELLED_ONLINE_MW" in out.columns:
        out["estimated_demand_mw"] = out["estimated_demand_mw"].fillna(pd.to_numeric(out["MODELLED_ONLINE_MW"], errors="coerce"))

    out["total_generation_mw"] = np.nan
    if "GENERATION" in out.columns:
        out["total_generation_mw"] = pd.to_numeric(out["GENERATION"], errors="coerce")
    if "MODELLED_ONLINE_MW" in out.columns:
        out["total_generation_mw"] = out["total_generation_mw"].fillna(pd.to_numeric(out["MODELLED_ONLINE_MW"], errors="coerce"))
    return out


def build_replay_payload(frame: pd.DataFrame, summary: dict[str, Any], fps: int) -> dict[str, Any]:
    fuel_cols = [
        col
        for col in ["GAS", "COAL", "NUCLEAR", "WIND", "WIND_EMB", "SOLAR", "HYDRO", "BIOMASS", "STORAGE", "IMPORTS", "OTHER"]
        if col in frame.columns
    ]

    interconnector_cols = [
        col for col in frame.columns if col.endswith("_FLOW") or col == "NET_INTERCONNECTOR_FLOW"
    ]

    keep_cols = [
        "rocof_timestamp",
        "rocof_hz_per_s",
        "f_start_hz",
        "f_end_hz",
        "delta_t_s",
        "total_generation_mw",
        "estimated_demand_mw",
        "MODELLED_ONLINE_MW",
        "GENERATION",
        "matched_generation_ts",
        "matched_demand_ts",
    ] + fuel_cols + interconnector_cols

    data = frame[keep_cols].copy()
    for c in ["rocof_timestamp", "matched_generation_ts", "matched_demand_ts"]:
        if c in data.columns:
            data[c] = pd.to_datetime(data[c], errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            data[c] = data[c].fillna("")

    numeric_cols = [
        c
        for c in data.columns
        if c not in {"rocof_timestamp", "matched_generation_ts", "matched_demand_ts"}
    ]
    for c in numeric_cols:
        data[c] = pd.to_numeric(data[c], errors="coerce")

    data = data.replace([np.inf, -np.inf], np.nan)
    data = data.astype(object).where(pd.notna(data), None)
    frames = data.to_dict(orient="records")

    payload: dict[str, Any] = {
        "schema": "derive-rocof-replay-v1",
        "fps": fps,
        "summary": summary,
        "frames": frames,
        "available_fuels": fuel_cols,
        "available_interconnectors": interconnector_cols,
    }
    return payload


def downsample_for_replay(frame: pd.DataFrame, max_frames: int | None) -> pd.DataFrame:
    if max_frames is None or max_frames <= 0 or len(frame) <= max_frames:
        return frame
    idx = np.linspace(0, len(frame) - 1, num=max_frames, dtype=int)
    return frame.iloc[idx].copy()


def select_snapshot_window(
    frame: pd.DataFrame,
    snapshot_seconds: int | None,
    snapshot_start_ts: str | None,
) -> pd.DataFrame:
    if snapshot_seconds is None or snapshot_seconds <= 0 or frame.empty:
        return frame

    ordered = frame.sort_values("rocof_timestamp").copy()
    start = ordered["rocof_timestamp"].min()

    if snapshot_start_ts:
        parsed = pd.to_datetime(snapshot_start_ts, errors="coerce", utc=True)
        if pd.notna(parsed):
            start = parsed

    end = start + pd.Timedelta(seconds=snapshot_seconds)
    sliced = ordered[(ordered["rocof_timestamp"] >= start) & (ordered["rocof_timestamp"] < end)].copy()
    return sliced if not sliced.empty else ordered


def summarize(frame: pd.DataFrame, files_scanned: int, timestamp_mode: str, tolerance_minutes: int) -> dict:
    matched = frame["matched_generation_ts"].notna().sum() if "matched_generation_ts" in frame.columns else 0
    abs_rocof = frame["rocof_hz_per_s"].abs()
    return {
        "files_scanned": files_scanned,
        "rows": int(len(frame)),
        "timestamp_mode": timestamp_mode,
        "generation_tolerance_minutes": tolerance_minutes,
        "matched_generation_rows": int(matched),
        "match_rate": float(matched / len(frame)) if len(frame) else 0.0,
        "time_start": str(frame["rocof_timestamp"].min()) if len(frame) else None,
        "time_end": str(frame["rocof_timestamp"].max()) if len(frame) else None,
        "rocof_mean_hz_per_s": float(frame["rocof_hz_per_s"].mean()) if len(frame) else None,
        "rocof_std_hz_per_s": float(frame["rocof_hz_per_s"].std()) if len(frame) else None,
        "rocof_abs_p95_hz_per_s": float(abs_rocof.quantile(0.95)) if len(frame) else None,
        "rocof_abs_max_hz_per_s": float(abs_rocof.max()) if len(frame) else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive sample-to-sample RoCoF and align to generation")
    parser.add_argument("--root", default=str(project_root()), help="Project root path (defaults to detected GDA root)")
    parser.add_argument("--frequency-file", help="Optional single frequency CSV path")
    parser.add_argument("--max-files", type=int, help="Optional limit of frequency files for quick runs")
    parser.add_argument("--row-stride", type=int, default=1, help="Read every Nth frequency row to reduce runtime")
    parser.add_argument(
        "--timestamp-mode",
        choices=["start", "midpoint", "end"],
        default="midpoint",
        help="Where to place RoCoF timestamp between sample pairs",
    )
    parser.add_argument(
        "--generation-tolerance-minutes",
        type=int,
        default=40,
        help="Nearest-match tolerance for aligning to historical generation",
    )
    parser.add_argument(
        "--demand-tolerance-minutes",
        type=int,
        default=45,
        help="Nearest-match tolerance for aligning to demand data",
    )
    parser.add_argument("--fps", type=int, default=30, help="Suggested replay FPS to embed in output replay JSON")
    parser.add_argument("--output-csv", help="Optional CSV output path for aligned sample RoCoF")
    parser.add_argument("--output-json", help="Optional JSON summary output path")
    parser.add_argument("--output-replay-json", help="Optional JSON output containing all replay frames")
    parser.add_argument(
        "--snapshot-seconds",
        type=int,
        help="Optional contiguous replay window length in seconds (e.g. 900 for 15 minutes)",
    )
    parser.add_argument(
        "--snapshot-start-ts",
        help="Optional replay window start timestamp in UTC (ISO-8601), defaults to first sample",
    )
    parser.add_argument(
        "--max-replay-frames",
        type=int,
        help="Optional max frame count for replay JSON (evenly sampled across run)",
    )
    parser.add_argument("--json", action="store_true", help="Print summary as JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()

    if args.frequency_file:
        files = [Path(args.frequency_file).resolve()]
    else:
        freq_dir = root / "DataSources" / "NESO" / "Frequency"
        if not freq_dir.exists():
            print(f"[error] Frequency folder not found: {freq_dir}")
            return 1
        files = list_frequency_files(freq_dir)

    if args.max_files is not None:
        files = files[: max(args.max_files, 0)]

    if not files:
        print("[error] No frequency input files selected")
        return 1

    freq = load_frequency(files, row_stride=max(args.row_stride, 1))
    rocof = derive_rocof_samples(freq, timestamp_mode=args.timestamp_mode)

    generation = load_generation_context(root)
    aligned = align_with_generation(rocof, generation, tolerance_minutes=max(args.generation_tolerance_minutes, 1))
    demand = load_demand_context(root)
    aligned = align_with_demand(aligned, demand, tolerance_minutes=max(args.demand_tolerance_minutes, 1))
    aligned = add_estimated_demand(aligned)

    summary = summarize(
        aligned,
        files_scanned=len(files),
        timestamp_mode=args.timestamp_mode,
        tolerance_minutes=max(args.generation_tolerance_minutes, 1),
    )

    if args.output_csv:
        out_csv = Path(args.output_csv).resolve()
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        aligned.to_csv(out_csv, index=False)

    if args.output_json:
        out_json = Path(args.output_json).resolve()
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if args.output_replay_json:
        out_replay_json = Path(args.output_replay_json).resolve()
        out_replay_json.parent.mkdir(parents=True, exist_ok=True)
        replay_frame = select_snapshot_window(
            aligned,
            snapshot_seconds=args.snapshot_seconds,
            snapshot_start_ts=args.snapshot_start_ts,
        )
        replay_frame = downsample_for_replay(replay_frame, max_frames=args.max_replay_frames)
        payload = build_replay_payload(replay_frame, summary=summary, fps=max(args.fps, 1))
        out_replay_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print("=== DeriveRoCoF complete ===")
        print(f"Files scanned: {summary['files_scanned']}")
        print(f"Rows: {summary['rows']}")
        print(f"Time window: {summary['time_start']} -> {summary['time_end']}")
        print(f"Timestamp mode: {summary['timestamp_mode']}")
        print(f"Generation match rate: {summary['match_rate']:.3f}")
        print(f"RoCoF |abs| p95: {summary['rocof_abs_p95_hz_per_s']}")
        print(f"RoCoF |abs| max: {summary['rocof_abs_max_hz_per_s']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
