#!/usr/bin/env python3
"""Estimate GB grid inertia (GVA·s) from online generation.

Features:
- Best timestamp match against DataSources/NESO/HistoricalGenerationData/df_fuel_ckan.csv
- Full fuel-input model (coal, gas, nuclear, hydro, biomass, storage,
  wind, wind_emb, solar, imports, other)
- Defensible default H ranges per fuel (low/best/high)
- Optional per-fuel H overrides via JSON
- Optional comparison vs reported inertia from DataSources/NESO/Inertia/*.csv
- Optional empirical calibration to reported outturn inertia
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


H_MODEL_SECONDS: dict[str, tuple[float, float, float]] = {
    "COAL": (4.0, 5.0, 6.0),
    "GAS": (3.0, 4.0, 5.0),
    "NUCLEAR": (5.0, 6.0, 7.0),
    "HYDRO": (5.0, 6.0, 7.0),
    "BIOMASS": (3.0, 4.0, 5.0),
    "STORAGE": (4.0, 5.0, 6.0),
    "WIND": (0.0, 0.3, 0.6),
    "WIND_EMB": (0.0, 0.3, 0.6),
    "SOLAR": (0.0, 0.2, 0.4),
    "IMPORTS": (0.0, 0.5, 1.0),
    "OTHER": (0.5, 1.5, 3.0),
}


@dataclass
class EstimateResult:
    target_timestamp: str
    matched_timestamp: str
    match_offset_seconds: float
    inputs_mw: dict[str, float]
    renewables_mw: float
    modelled_generation_mw: float
    total_generation_mw: float | None
    modelled_share_of_total: float | None
    inertia_low_gvas: float
    inertia_best_gvas: float
    inertia_high_gvas: float
    h_model_seconds: dict[str, list[float]]

    reported_outturn_gvas: float | None = None
    reported_market_gvas: float | None = None
    reported_timestamp: str | None = None
    reported_offset_seconds: float | None = None
    best_vs_reported_ratio: float | None = None

    calibrated_inertia_low_gvas: float | None = None
    calibrated_inertia_best_gvas: float | None = None
    calibrated_inertia_high_gvas: float | None = None
    calibration_slope: float | None = None
    calibration_intercept_gvas: float | None = None
    calibration_r2: float | None = None
    calibration_points: int | None = None
    calibration_window_start: str | None = None
    calibration_window_end: str | None = None


def project_root() -> Path:
    return next(p for p in Path(__file__).resolve().parents if p.name == "GDA")


def load_h_model(overrides_path: str | None) -> dict[str, tuple[float, float, float]]:
    model = {k: tuple(v) for k, v in H_MODEL_SECONDS.items()}
    if not overrides_path:
        return model

    path = Path(overrides_path)
    if not path.exists():
        raise FileNotFoundError(f"H override file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("H override file must be a JSON object")

    for key, value in data.items():
        if key not in model:
            continue
        if not isinstance(value, (list, tuple)) or len(value) != 3:
            raise ValueError(f"Override for {key} must be [low, best, high]")
        low, best, high = map(float, value)
        if not (low <= best <= high):
            raise ValueError(f"Override for {key} must satisfy low <= best <= high")
        model[key] = (low, best, high)

    return model


def load_generation(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["DATETIME"])
    return df.sort_values("DATETIME").set_index("DATETIME")


def settlement_to_datetime(date_series: pd.Series, period_series: pd.Series) -> pd.Series:
    date = pd.to_datetime(date_series, errors="coerce", utc=True)
    offset = (pd.to_numeric(period_series, errors="coerce") - 1) * 30
    return date + pd.to_timedelta(offset, unit="m")


def load_inertia_reference(root: Path) -> pd.DataFrame:
    files = sorted(root.glob("DataSources/NESO/Inertia/*.csv"))
    frames = []
    for file_path in files:
        try:
            frame = pd.read_csv(
                file_path,
                usecols=["Settlement Date", "Settlement Period", "Outturn Inertia", "Market Provided Inertia"],
                dtype={"Settlement Date": "string", "Settlement Period": "float64"},
            )
        except Exception:
            continue

        frame["DatetimeUTC"] = settlement_to_datetime(frame["Settlement Date"], frame["Settlement Period"])
        frame["Outturn Inertia"] = pd.to_numeric(frame["Outturn Inertia"], errors="coerce")
        frame["Market Provided Inertia"] = pd.to_numeric(frame["Market Provided Inertia"], errors="coerce")
        frame = frame.dropna(subset=["DatetimeUTC", "Outturn Inertia"])
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame()

    all_data = pd.concat(frames, ignore_index=True)
    all_data = all_data.sort_values("DatetimeUTC").drop_duplicates(subset=["DatetimeUTC"], keep="last")
    return all_data.set_index("DatetimeUTC")


def parse_timestamp(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts


def nearest_row(df: pd.DataFrame, target_ts: pd.Timestamp) -> tuple[pd.Timestamp, pd.Series]:
    if df.empty:
        raise ValueError("Dataframe is empty")
    pos = df.index.get_indexer([target_ts], method="nearest")
    if pos[0] < 0:
        raise ValueError("Unable to find nearest timestamp match")
    matched_ts = df.index[pos[0]]
    return matched_ts, df.iloc[pos[0]]


def nearest_row_tz(df: pd.DataFrame, target_ts: pd.Timestamp) -> tuple[pd.Timestamp, pd.Series]:
    if df.empty:
        raise ValueError("Reference dataframe is empty")
    target_tz = target_ts.tz_localize("UTC") if target_ts.tzinfo is None else target_ts.tz_convert("UTC")
    pos = df.index.get_indexer([target_tz], method="nearest")
    if pos[0] < 0:
        raise ValueError("Unable to find nearest timestamp match")
    matched_ts = df.index[pos[0]]
    return matched_ts, df.iloc[pos[0]]


def safe_value(row: pd.Series, col: str) -> float:
    value = row[col] if col in row.index else 0.0
    value = float(value) if pd.notna(value) else 0.0
    return max(value, 0.0)


def collect_inputs(row: pd.Series) -> dict[str, float]:
    return {
        "COAL": safe_value(row, "COAL"),
        "GAS": safe_value(row, "GAS"),
        "NUCLEAR": safe_value(row, "NUCLEAR"),
        "HYDRO": safe_value(row, "HYDRO"),
        "BIOMASS": safe_value(row, "BIOMASS"),
        "STORAGE": safe_value(row, "STORAGE"),
        "WIND": safe_value(row, "WIND"),
        "WIND_EMB": safe_value(row, "WIND_EMB"),
        "SOLAR": safe_value(row, "SOLAR"),
        "IMPORTS": safe_value(row, "IMPORTS"),
        "OTHER": safe_value(row, "OTHER"),
    }


def compute_inertia_mws(
    inputs_mw: dict[str, float], h_model: dict[str, tuple[float, float, float]]
) -> tuple[float, float, float]:
    low = 0.0
    best = 0.0
    high = 0.0
    for fuel, mw in inputs_mw.items():
        h_low, h_best, h_high = h_model[fuel]
        low += mw * h_low
        best += mw * h_best
        high += mw * h_high
    return low, best, high


def estimate_inertia(
    row: pd.Series,
    target_ts: pd.Timestamp,
    matched_ts: pd.Timestamp,
    h_model: dict[str, tuple[float, float, float]],
) -> EstimateResult:
    inputs_mw = collect_inputs(row)
    renewables_mw = inputs_mw["WIND"] + inputs_mw["WIND_EMB"] + inputs_mw["SOLAR"]
    modelled_generation_mw = sum(inputs_mw.values())

    total_generation_mw = None
    if "GENERATION" in row.index and pd.notna(row["GENERATION"]):
        total_generation_mw = max(float(row["GENERATION"]), 0.0)

    modelled_share_of_total = None
    if total_generation_mw and total_generation_mw > 0:
        modelled_share_of_total = modelled_generation_mw / total_generation_mw

    low_mws, best_mws, high_mws = compute_inertia_mws(inputs_mw, h_model)

    return EstimateResult(
        target_timestamp=str(target_ts),
        matched_timestamp=str(matched_ts),
        match_offset_seconds=abs((matched_ts - target_ts).total_seconds()),
        inputs_mw=inputs_mw,
        renewables_mw=renewables_mw,
        modelled_generation_mw=modelled_generation_mw,
        total_generation_mw=total_generation_mw,
        modelled_share_of_total=modelled_share_of_total,
        inertia_low_gvas=low_mws / 1000.0,
        inertia_best_gvas=best_mws / 1000.0,
        inertia_high_gvas=high_mws / 1000.0,
        h_model_seconds={k: [v[0], v[1], v[2]] for k, v in h_model.items()},
    )


def add_reported_comparison(result: EstimateResult, ref_row: pd.Series, ref_ts: pd.Timestamp) -> None:
    outturn = ref_row.get("Outturn Inertia")
    market = ref_row.get("Market Provided Inertia")
    outturn_val = float(outturn) if pd.notna(outturn) else None
    market_val = float(market) if pd.notna(market) else None

    result.reported_outturn_gvas = outturn_val
    result.reported_market_gvas = market_val
    result.reported_timestamp = str(ref_ts)

    target_ts = pd.Timestamp(result.matched_timestamp)
    target_ts = target_ts.tz_localize("UTC") if target_ts.tzinfo is None else target_ts.tz_convert("UTC")
    result.reported_offset_seconds = abs((ref_ts - target_ts).total_seconds())

    if outturn_val and outturn_val > 0:
        result.best_vs_reported_ratio = result.inertia_best_gvas / outturn_val


def build_calibration_series(
    generation_df: pd.DataFrame,
    inertia_df: pd.DataFrame,
    h_model: dict[str, tuple[float, float, float]],
    target_ts: pd.Timestamp,
    days: int,
) -> pd.DataFrame:
    start_ts = target_ts - pd.Timedelta(days=days)
    end_ts = target_ts

    gen_window = generation_df[(generation_df.index >= start_ts) & (generation_df.index <= end_ts)]
    if gen_window.empty:
        return pd.DataFrame()

    calc_rows = []
    for ts, row in gen_window.iterrows():
        inputs = collect_inputs(row)
        _, best_mws, _ = compute_inertia_mws(inputs, h_model)
        calc_rows.append({"timestamp": ts, "calc_best_gvas": best_mws / 1000.0})

    calc_df = pd.DataFrame(calc_rows)
    if calc_df.empty:
        return pd.DataFrame()

    calc_df["timestamp"] = pd.to_datetime(calc_df["timestamp"], utc=True)
    calc_df = calc_df.sort_values("timestamp")

    ref_df = inertia_df[["Outturn Inertia"]].dropna().copy()
    ref_df = ref_df.sort_index().reset_index().rename(
        columns={"DatetimeUTC": "timestamp", "Outturn Inertia": "outturn_gvas"}
    )
    ref_df["timestamp"] = pd.to_datetime(ref_df["timestamp"], utc=True)

    merged = pd.merge_asof(
        calc_df,
        ref_df,
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=20),
    )
    return merged.dropna(subset=["calc_best_gvas", "outturn_gvas"])


def apply_calibration(result: EstimateResult, merged: pd.DataFrame, min_points: int) -> None:
    if merged.empty or len(merged) < min_points:
        return

    x = merged["calc_best_gvas"].to_numpy(dtype=float)
    y = merged["outturn_gvas"].to_numpy(dtype=float)

    if np.allclose(np.std(x), 0.0):
        return

    slope, intercept = np.polyfit(x, y, 1)
    y_hat = intercept + slope * x
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else None

    result.calibration_slope = float(slope)
    result.calibration_intercept_gvas = float(intercept)
    result.calibration_r2 = r2
    result.calibration_points = int(len(merged))
    result.calibration_window_start = str(merged["timestamp"].min())
    result.calibration_window_end = str(merged["timestamp"].max())

    result.calibrated_inertia_low_gvas = max(0.0, float(intercept + slope * result.inertia_low_gvas))
    result.calibrated_inertia_best_gvas = max(0.0, float(intercept + slope * result.inertia_best_gvas))
    result.calibrated_inertia_high_gvas = max(0.0, float(intercept + slope * result.inertia_high_gvas))


def print_result(result: EstimateResult) -> None:
    print("=== Generation-Based Inertia Estimate (GVA·s) ===")
    print(f"Target timestamp:  {result.target_timestamp}")
    print(f"Matched timestamp: {result.matched_timestamp}")
    print(f"Match offset (s):  {result.match_offset_seconds:.1f}")
    print()

    print("Model inputs (MW):")
    for fuel in H_MODEL_SECONDS:
        print(f"  {fuel:10s} {result.inputs_mw.get(fuel, 0.0):,.1f}")
    print(f"  {'RENEWABLES':10s} {result.renewables_mw:,.1f} (WIND + WIND_EMB + SOLAR)")
    print(f"  {'MODELLED MW':10s} {result.modelled_generation_mw:,.1f}")

    if result.total_generation_mw is not None:
        share = (result.modelled_share_of_total or 0.0) * 100.0
        print(f"  TOTAL MW:    {result.total_generation_mw:,.1f}")
        print(f"  COVERAGE:    {share:.2f}% of total generation")

    print()
    print("Inertia constants used (H, s):")
    for fuel, h_values in result.h_model_seconds.items():
        print(f"  {fuel}: {h_values[0]}..{h_values[2]} (best {h_values[1]})")

    print()
    print("Estimated inertia (GVA·s):")
    print(f"  LOW:   {result.inertia_low_gvas:,.2f}")
    print(f"  BEST:  {result.inertia_best_gvas:,.2f}")
    print(f"  HIGH:  {result.inertia_high_gvas:,.2f}")

    if result.calibrated_inertia_best_gvas is not None:
        print()
        print("Calibrated estimate vs historical outturn fit (GVA·s):")
        print(f"  LOW*:  {result.calibrated_inertia_low_gvas:,.2f}")
        print(f"  BEST*: {result.calibrated_inertia_best_gvas:,.2f}")
        print(f"  HIGH*: {result.calibrated_inertia_high_gvas:,.2f}")
        print("  Fit details:")
        print(f"    slope:     {result.calibration_slope:.4f}")
        print(f"    intercept: {result.calibration_intercept_gvas:,.2f} GVA·s")
        if result.calibration_r2 is not None:
            print(f"    r2:        {result.calibration_r2:.4f}")
        print(f"    points:    {result.calibration_points}")
        print(f"    window:    {result.calibration_window_start} -> {result.calibration_window_end}")

    if result.reported_outturn_gvas is not None:
        print()
        print("Reported inertia comparison (Inertia dataset):")
        print(f"  REPORTED TS:      {result.reported_timestamp}")
        print(f"  REPORTED OFFSET:  {result.reported_offset_seconds:.1f} s")
        print(f"  OUTTURN GVA·s:    {result.reported_outturn_gvas:,.2f}")
        if result.reported_market_gvas is not None:
            print(f"  MARKET GVA·s:     {result.reported_market_gvas:,.2f}")
        if result.best_vs_reported_ratio is not None:
            print(f"  BEST/OUTTURN:     {result.best_vs_reported_ratio:.3f}")
            print(f"  GAP TO OUTTURN:   {result.reported_outturn_gvas - result.inertia_best_gvas:,.2f} GVA·s")


def to_dict(result: EstimateResult) -> dict:
    return {
        "target_timestamp": result.target_timestamp,
        "matched_timestamp": result.matched_timestamp,
        "match_offset_seconds": result.match_offset_seconds,
        "inputs_mw": {
            "coal": result.inputs_mw["COAL"],
            "gas": result.inputs_mw["GAS"],
            "nuclear": result.inputs_mw["NUCLEAR"],
            "hydro": result.inputs_mw["HYDRO"],
            "biomass": result.inputs_mw["BIOMASS"],
            "storage": result.inputs_mw["STORAGE"],
            "wind": result.inputs_mw["WIND"],
            "wind_emb": result.inputs_mw["WIND_EMB"],
            "solar": result.inputs_mw["SOLAR"],
            "imports": result.inputs_mw["IMPORTS"],
            "other": result.inputs_mw["OTHER"],
            "renewables": result.renewables_mw,
            "modelled_generation": result.modelled_generation_mw,
            "total_generation": result.total_generation_mw,
            "modelled_share_of_total": result.modelled_share_of_total,
        },
        "h_model_seconds": result.h_model_seconds,
        "inertia_gvas": {
            "low": result.inertia_low_gvas,
            "best": result.inertia_best_gvas,
            "high": result.inertia_high_gvas,
        },
        "calibrated_inertia_gvas": {
            "low": result.calibrated_inertia_low_gvas,
            "best": result.calibrated_inertia_best_gvas,
            "high": result.calibrated_inertia_high_gvas,
        },
        "calibration": {
            "slope": result.calibration_slope,
            "intercept_gvas": result.calibration_intercept_gvas,
            "r2": result.calibration_r2,
            "points": result.calibration_points,
            "window_start": result.calibration_window_start,
            "window_end": result.calibration_window_end,
        },
        "reported_inertia": {
            "timestamp": result.reported_timestamp,
            "offset_seconds": result.reported_offset_seconds,
            "outturn_gvas": result.reported_outturn_gvas,
            "market_gvas": result.reported_market_gvas,
            "best_vs_reported_ratio": result.best_vs_reported_ratio,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate grid inertia from historical generation")
    parser.add_argument("--timestamp", help="Target timestamp (e.g. 2024-11-01 18:00:00). Defaults to latest.")
    parser.add_argument(
        "--max-offset-minutes",
        type=float,
        default=120.0,
        help="Warn if nearest generation match is further than this many minutes (default: 120).",
    )
    parser.add_argument("--output-json", help="Optional path to write machine-readable output JSON.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON to stdout.")
    parser.add_argument(
        "--skip-inertia-reference",
        action="store_true",
        help="Skip matching against DataSources/NESO/Inertia/*.csv reported values.",
    )
    parser.add_argument(
        "--h-overrides",
        help="Optional JSON file with per-fuel H overrides: {\"FUEL\": [low,best,high], ...}",
    )
    parser.add_argument(
        "--calibrate",
        action="store_true",
        help="Fit a linear correction to reported outturn inertia over a historical window.",
    )
    parser.add_argument(
        "--calibration-days",
        type=int,
        default=60,
        help="Lookback window in days for calibration fit (default: 60).",
    )
    parser.add_argument(
        "--calibration-min-points",
        type=int,
        default=200,
        help="Minimum matched points required to apply calibration (default: 200).",
    )
    args = parser.parse_args()

    root = project_root()
    generation_csv = root / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"

    if not generation_csv.exists():
        print(f"[error] Missing generation file: {generation_csv}")
        return 1

    try:
        h_model = load_h_model(args.h_overrides)
    except Exception as exc:
        print(f"[error] Failed to load H model overrides: {exc}")
        return 1

    generation_df = load_generation(generation_csv)
    if generation_df.empty:
        print("[error] No generation data available")
        return 1

    target_ts = parse_timestamp(args.timestamp) if args.timestamp else generation_df.index.max()
    matched_ts, row = nearest_row(generation_df, target_ts)
    result = estimate_inertia(row, target_ts, matched_ts, h_model)

    inertia_df = pd.DataFrame()
    if not args.skip_inertia_reference:
        inertia_df = load_inertia_reference(root)
        if not inertia_df.empty:
            try:
                ref_ts, ref_row = nearest_row_tz(inertia_df, matched_ts)
                add_reported_comparison(result, ref_row, ref_ts)
            except Exception:
                pass

    if args.calibrate and not inertia_df.empty:
        try:
            merged = build_calibration_series(generation_df, inertia_df, h_model, matched_ts, args.calibration_days)
            apply_calibration(result, merged, args.calibration_min_points)
        except Exception:
            pass

    if result.match_offset_seconds > args.max_offset_minutes * 60:
        print(f"[warning] Nearest match is {result.match_offset_seconds / 60:.1f} minutes away from target")

    payload = to_dict(result)
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print_result(result)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if not args.json:
            print(f"\nSaved JSON output to: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
