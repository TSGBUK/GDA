#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from train_system_state_monster import (
    add_expert_features,
    add_frequency_window_features,
    add_time_features,
    asof_join,
    find_frequency_files,
    load_bsad_aggregated,
    load_bsad_forward,
    load_balancing,
    load_carbon_balancing,
    load_demand,
    load_eac_auction,
    load_ecbr_auction,
    load_generation_with_curtailment,
    load_gridwatch,
    load_inertia,
    load_weather,
    project_root,
    read_frequency_raw,
)


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def build_fusion_frame(
    frequency_dir: Path,
    weather_parquet_dir: Path,
    inertia_parquet_dir: Path,
    demand_parquet_dir: Path,
    balancing_parquet_dir: Path,
    gridwatch_parquet_dir: Path,
    generation_csv_path: Path,
    bsad_aggregated_parquet_dir: Path,
    bsad_forward_parquet_dir: Path,
    carbon_balancing_parquet_dir: Path,
    eac_parquet_dir: Path,
    ecbr_parquet_dir: Path,
    experts_dir: Path,
    resolution: str,
    join_tolerance: str,
    max_frequency_files: int,
    row_stride: int,
    max_rows: int,
    window_seconds: list[int],
    disable_expert_features: bool,
) -> tuple[pd.DataFrame, list[str], list[Path]]:
    freq_files = find_frequency_files(frequency_dir)
    if not freq_files:
        raise RuntimeError(f"No raw frequency files found in: {frequency_dir}")
    if max_frequency_files > 0:
        freq_files = freq_files[-max_frequency_files:]

    freq_df = read_frequency_raw(freq_files)
    freq_df = (
        freq_df.set_index("timestamp")
        .resample(resolution)
        .mean(numeric_only=True)
        .dropna(subset=["frequency_hz"])
        .reset_index()
    )

    fusion = add_time_features(freq_df)
    fusion = add_frequency_window_features(fusion, window_seconds)

    weather = load_weather(weather_parquet_dir)
    inertia = load_inertia(inertia_parquet_dir)
    demand = load_demand(demand_parquet_dir)
    balancing = load_balancing(balancing_parquet_dir)
    gridwatch = load_gridwatch(gridwatch_parquet_dir)
    generation = load_generation_with_curtailment(generation_csv_path)
    bsad_aggregated = load_bsad_aggregated(bsad_aggregated_parquet_dir)
    bsad_forward = load_bsad_forward(bsad_forward_parquet_dir)
    carbon_balancing = load_carbon_balancing(carbon_balancing_parquet_dir)
    eac = load_eac_auction(eac_parquet_dir)
    ecbr = load_ecbr_auction(ecbr_parquet_dir)

    for frame in [
        weather,
        inertia,
        demand,
        balancing,
        gridwatch,
        generation,
        bsad_aggregated,
        bsad_forward,
        carbon_balancing,
        eac,
        ecbr,
    ]:
        fusion = asof_join(fusion, frame, tolerance=join_tolerance)

    if disable_expert_features:
        used_artifacts: list[str] = []
    else:
        fusion, used_artifacts = add_expert_features(fusion, experts_dir)

    fusion = fusion.sort_values("timestamp")
    if row_stride > 1:
        fusion = fusion.iloc[::row_stride].copy()
    if max_rows > 0 and len(fusion) > max_rows:
        fusion = fusion.iloc[-max_rows:].copy()

    fusion = fusion.replace([np.inf, -np.inf], np.nan)
    return fusion, used_artifacts, freq_files


def predict_one_member(
    member: dict[str, Any],
    fusion: pd.DataFrame,
) -> tuple[pd.DataFrame, float]:
    model_path = Path(member["model_file"]).resolve()
    payload = joblib.load(model_path)

    model = payload.get("model")
    feature_columns = payload.get("feature_columns", [])
    target_columns = payload.get("target_columns", [])

    if model is None or not hasattr(model, "predict"):
        raise RuntimeError(f"Model is not runnable for member file: {model_path}")

    x = pd.DataFrame(index=fusion.index)
    for col in feature_columns:
        if col in fusion.columns:
            x[col] = pd.to_numeric(fusion[col], errors="coerce")
        else:
            x[col] = 0.0

    x = x.replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0.0)

    pred = model.predict(x)
    pred_np = np.asarray(pred)
    if pred_np.ndim == 1:
        pred_np = pred_np.reshape(-1, 1)

    if not target_columns:
        target_columns = [f"target_{idx}" for idx in range(pred_np.shape[1])]

    pred_df = pd.DataFrame(pred_np, columns=target_columns, index=fusion.index)
    weight = float(member.get("weight", 0.0))
    return pred_df, weight


def blend_predictions(weighted_preds: list[tuple[pd.DataFrame, float]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not weighted_preds:
        raise RuntimeError("No member predictions available for blending")

    all_targets = sorted({col for pred_df, _ in weighted_preds for col in pred_df.columns})
    index = weighted_preds[0][0].index

    weighted_sum = pd.DataFrame(0.0, index=index, columns=all_targets)
    weight_total = 0.0

    member_matrix: list[pd.DataFrame] = []

    for pred_df, weight in weighted_preds:
        aligned = pred_df.reindex(columns=all_targets).astype(float)
        aligned = aligned.ffill().bfill().fillna(0.0)
        weighted_sum += aligned * weight
        weight_total += weight
        member_matrix.append(aligned)

    if weight_total <= 0:
        weight_total = 1.0
    blended = weighted_sum / weight_total

    stacked = np.stack([m.values for m in member_matrix], axis=0)
    std_vals = np.std(stacked, axis=0)
    uncertainty = pd.DataFrame(std_vals, index=index, columns=[f"{col}_std" for col in all_targets])

    return blended, uncertainty


def main() -> int:
    root = project_root()

    parser = argparse.ArgumentParser(description="Run inference using the system-state monster ensemble manifest")
    parser.add_argument(
        "--manifest-path",
        default=str(root / "MachineLearning" / "experts" / "pre-trained-experts" / "monster" / "monster_ensemble_manifest.json"),
    )

    parser.add_argument("--frequency-dir", default=str(root / "DataSources" / "NESO" / "Frequency"))
    parser.add_argument("--weather-parquet-dir", default=str(root / "DataSources" / "Weather" / "Parquet"))
    parser.add_argument("--inertia-parquet-dir", default=str(root / "DataSources" / "NESO" / "Inertia" / "Parquet"))
    parser.add_argument("--demand-parquet-dir", default=str(root / "DataSources" / "NESO" / "DemandData" / "Parquet"))
    parser.add_argument("--balancing-parquet-dir", default=str(root / "DataSources" / "NESO" / "BalancingServices" / "Parquet"))
    parser.add_argument("--gridwatch-parquet-dir", default=str(root / "DataSources" / "GridWatch" / "Parquet"))
    parser.add_argument("--generation-csv-path", default=str(root / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"))
    parser.add_argument("--bsad-aggregated-parquet-dir", default=str(root / "DataSources" / "NESO" / "BSAD_AggregatedData" / "Parquet"))
    parser.add_argument("--bsad-forward-parquet-dir", default=str(root / "DataSources" / "NESO" / "BSAD_ForwardContracts" / "Parquet"))
    parser.add_argument(
        "--carbon-balancing-parquet-dir",
        default=str(root / "DataSources" / "NESO" / "CarbonIntensityOfBalancingActions" / "Parquet"),
    )
    parser.add_argument("--eac-parquet-dir", default=str(root / "DataSources" / "NESO" / "EACEnduringAuctionCapability" / "Parquet"))
    parser.add_argument("--ecbr-parquet-dir", default=str(root / "DataSources" / "NESO" / "EC-BR_AuctionResults" / "Parquet"))
    parser.add_argument("--experts-dir", default=str(root / "MachineLearning" / "experts" / "pre-trained-experts"))

    parser.add_argument("--resolution", default="1S")
    parser.add_argument("--join-tolerance", default="35m")
    parser.add_argument("--max-frequency-files", type=int, default=0)
    parser.add_argument("--row-stride", type=int, default=1)
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--window-seconds", default="5,30,120,600")
    parser.add_argument("--disable-expert-features", action="store_true")

    parser.add_argument(
        "--output-csv",
        default=str(root / "MachineLearning" / "experts" / "pre-trained-experts" / "monster" / "monster_inference.csv"),
    )
    parser.add_argument(
        "--output-json",
        default=str(root / "MachineLearning" / "experts" / "pre-trained-experts" / "monster" / "monster_inference_summary.json"),
    )

    args = parser.parse_args()

    manifest_path = Path(args.manifest_path).resolve()
    output_csv = Path(args.output_csv).resolve()
    output_json = Path(args.output_json).resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(manifest_path)
    members: list[dict[str, Any]] = list(manifest.get("members", []))
    if not members:
        print(f"[error] No members found in manifest: {manifest_path}")
        return 1

    windows = [int(x.strip()) for x in args.window_seconds.split(",") if x.strip()]

    fusion, used_artifacts, freq_files = build_fusion_frame(
        frequency_dir=Path(args.frequency_dir).resolve(),
        weather_parquet_dir=Path(args.weather_parquet_dir).resolve(),
        inertia_parquet_dir=Path(args.inertia_parquet_dir).resolve(),
        demand_parquet_dir=Path(args.demand_parquet_dir).resolve(),
        balancing_parquet_dir=Path(args.balancing_parquet_dir).resolve(),
        gridwatch_parquet_dir=Path(args.gridwatch_parquet_dir).resolve(),
        generation_csv_path=Path(args.generation_csv_path).resolve(),
        bsad_aggregated_parquet_dir=Path(args.bsad_aggregated_parquet_dir).resolve(),
        bsad_forward_parquet_dir=Path(args.bsad_forward_parquet_dir).resolve(),
        carbon_balancing_parquet_dir=Path(args.carbon_balancing_parquet_dir).resolve(),
        eac_parquet_dir=Path(args.eac_parquet_dir).resolve(),
        ecbr_parquet_dir=Path(args.ecbr_parquet_dir).resolve(),
        experts_dir=Path(args.experts_dir).resolve(),
        resolution=args.resolution,
        join_tolerance=args.join_tolerance,
        max_frequency_files=args.max_frequency_files,
        row_stride=args.row_stride,
        max_rows=args.max_rows,
        window_seconds=windows,
        disable_expert_features=args.disable_expert_features,
    )

    if fusion.empty:
        print("[error] Fusion frame is empty after preprocessing.")
        return 1

    weighted_preds: list[tuple[pd.DataFrame, float]] = []
    failed_members: list[str] = []
    for member in members:
        try:
            pred_df, weight = predict_one_member(member, fusion)
            weighted_preds.append((pred_df, weight))
        except Exception:
            failed_members.append(str(member.get("model_file", "unknown")))
            continue

    if not weighted_preds:
        print("[error] Failed to run all ensemble members.")
        return 1

    blended, uncertainty = blend_predictions(weighted_preds)

    out = pd.DataFrame({"timestamp": fusion["timestamp"].values}, index=fusion.index)
    for col in blended.columns:
        out[f"pred_{col}"] = blended[col].values
    for col in uncertainty.columns:
        out[col] = uncertainty[col].values

    out.to_csv(output_csv, index=False)

    summary = {
        "manifest_path": str(manifest_path),
        "output_csv": str(output_csv),
        "rows": int(len(out)),
        "prediction_columns": [c for c in out.columns if c.startswith("pred_")],
        "uncertainty_columns": [c for c in out.columns if c.endswith("_std")],
        "members_total": len(members),
        "members_used": len(weighted_preds),
        "members_failed": failed_members,
        "frequency_files_used": [str(p) for p in freq_files],
        "used_expert_artifacts": used_artifacts,
        "resolution": args.resolution,
        "join_tolerance": args.join_tolerance,
        "row_stride": args.row_stride,
        "max_rows": args.max_rows,
    }
    output_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"[done] Inference rows: {len(out):,}")
    print(f"[done] Saved predictions CSV: {output_csv}")
    print(f"[done] Saved summary JSON: {output_json}")
    if failed_members:
        print(f"[warn] Some members failed and were skipped: {len(failed_members)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
