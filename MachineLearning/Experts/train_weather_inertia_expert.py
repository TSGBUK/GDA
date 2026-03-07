#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from common_trainer import (
    build_performance_metrics,
    print_training_summary,
    resolve_backend,
    save_artifacts,
    summarize_feature_weights,
    train_multioutput,
)


WEATHER_COLUMNS = ["Temperature_C", "Wind_Speed_100m_kph", "Solar_Radiation_W_m2"]
INERTIA_TARGET_COLUMNS = ["Outturn Inertia", "Market Provided Inertia"]


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def load_parquet(parquet_dir: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()


def prepare_weather(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Date"] = pd.to_datetime(out.get("Date"), utc=True, errors="coerce")
    for col in WEATHER_COLUMNS:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    keep_cols = ["Date", *WEATHER_COLUMNS]
    out = out[keep_cols].dropna(subset=["Date"]).sort_values("Date")
    out = out.rename(columns={"Date": "timestamp"})
    return out


def prepare_inertia(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["DatetimeUTC"] = pd.to_datetime(out.get("DatetimeUTC"), utc=True, errors="coerce")
    out["Settlement Period"] = pd.to_numeric(out.get("Settlement Period"), errors="coerce")
    for col in INERTIA_TARGET_COLUMNS:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    keep_cols = ["DatetimeUTC", "Settlement Period", *INERTIA_TARGET_COLUMNS]
    out = out[keep_cols].dropna(subset=["DatetimeUTC"]).sort_values("DatetimeUTC")
    out = out.rename(columns={"DatetimeUTC": "timestamp", "Settlement Period": "settlement_period"})
    return out


def merge_weather_inertia(weather_df: pd.DataFrame, inertia_df: pd.DataFrame, max_gap_minutes: int) -> pd.DataFrame:
    merged = pd.merge_asof(
        inertia_df.sort_values("timestamp"),
        weather_df.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=max_gap_minutes),
    )
    return merged


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = df["timestamp"]
    features = pd.DataFrame(index=df.index)

    features["year"] = dt.dt.year.astype(np.int16)
    features["month"] = dt.dt.month.astype(np.int8)
    features["day"] = dt.dt.day.astype(np.int8)
    features["dayofweek"] = dt.dt.dayofweek.astype(np.int8)
    features["hour"] = dt.dt.hour.astype(np.int8)
    features["minute"] = dt.dt.minute.astype(np.int8)

    features["hour_sin"] = np.sin(2 * np.pi * features["hour"] / 24.0)
    features["hour_cos"] = np.cos(2 * np.pi * features["hour"] / 24.0)
    features["dow_sin"] = np.sin(2 * np.pi * features["dayofweek"] / 7.0)
    features["dow_cos"] = np.cos(2 * np.pi * features["dayofweek"] / 7.0)
    features["month_sin"] = np.sin(2 * np.pi * features["month"] / 12.0)
    features["month_cos"] = np.cos(2 * np.pi * features["month"] / 12.0)

    for col in WEATHER_COLUMNS:
        features[col] = pd.to_numeric(df.get(col), errors="coerce")

    features["settlement_period"] = pd.to_numeric(df.get("settlement_period"), errors="coerce")
    return features


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(
        description="Train Weather+Inertia expert model (weather as exogenous input, inertia as targets)"
    )
    parser.add_argument("--weather-parquet-dir", default=str(project_root() / "DataSources" / "Weather" / "Parquet"))
    parser.add_argument("--inertia-parquet-dir", default=str(project_root() / "DataSources" / "NESO" / "Inertia" / "Parquet"))
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "experts" / "pre-trained-experts"),
    )
    parser.add_argument("--max-merge-gap-minutes", type=int, default=45)
    parser.add_argument("--n-estimators", type=int, default=400)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    args = parser.parse_args()

    weather_parquet_dir = Path(args.weather_parquet_dir).resolve()
    inertia_parquet_dir = Path(args.inertia_parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not weather_parquet_dir.exists() or not list_parquet_files(weather_parquet_dir):
        print(f"[info] Weather parquet not ready: {weather_parquet_dir}")
        return 0

    if not inertia_parquet_dir.exists() or not list_parquet_files(inertia_parquet_dir):
        print(f"[info] Inertia parquet not ready: {inertia_parquet_dir}")
        return 0

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    print(f"[load] Reading weather parquet dataset from: {weather_parquet_dir}")
    weather_df = prepare_weather(load_parquet(weather_parquet_dir))
    print(f"[load] Reading inertia parquet dataset from: {inertia_parquet_dir}")
    inertia_df = prepare_inertia(load_parquet(inertia_parquet_dir))

    if weather_df.empty or inertia_df.empty:
        print("[error] One or more source datasets are empty after preprocessing.")
        return 1

    merged = merge_weather_inertia(
        weather_df=weather_df,
        inertia_df=inertia_df,
        max_gap_minutes=args.max_merge_gap_minutes,
    )

    merged = merged.dropna(subset=[*WEATHER_COLUMNS, *INERTIA_TARGET_COLUMNS]).sort_values("timestamp")
    if merged.empty:
        print("[error] No merged rows after applying timestamp tolerance and null filtering.")
        return 1

    features = build_features(merged)
    target = merged[INERTIA_TARGET_COLUMNS].copy()

    print(f"[train] Training weather+inertia expert with {len(merged):,} rows on backend: {backend}")
    model, metrics, feature_cols = train_multioutput(
        features=features,
        target=target,
        target_cols=INERTIA_TARGET_COLUMNS,
        n_estimators=args.n_estimators,
        random_state=args.random_state,
        train_fraction=args.train_fraction,
        backend=backend,
    )
    performance = build_performance_metrics(metrics, started_at=started_at)
    weights = summarize_feature_weights(model, feature_cols, INERTIA_TARGET_COLUMNS)
    metrics["performance"] = performance
    metrics["weights"] = weights
    print_training_summary("Weather+Inertia expert", backend, metrics, performance, weights)

    model_path = output_dir / "weather_inertia_expert_model.joblib"
    metrics_path = output_dir / "weather_inertia_expert_metrics.json"

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=INERTIA_TARGET_COLUMNS,
        feature_columns=feature_cols,
        metadata={
            "dataset": "Weather+Inertia",
            "weather_parquet_dir": str(weather_parquet_dir),
            "inertia_parquet_dir": str(inertia_parquet_dir),
            "max_merge_gap_minutes": args.max_merge_gap_minutes,
            "n_estimators": args.n_estimators,
            "train_fraction": args.train_fraction,
            "random_state": args.random_state,
            "backend": backend,
            "device_arg": args.device,
        },
        metrics=metrics,
    )

    print(f"[done] Model saved: {model_path}")
    print(f"[done] Metrics saved: {metrics_path}")
    print(
        "[done] Overall test metrics -> "
        f"RMSE(mean): {metrics['overall']['rmse_mean']:.3f}, "
        f"MAE(mean): {metrics['overall']['mae_mean']:.3f}, "
        f"R2(mean): {metrics['overall']['r2_mean']:.3f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
