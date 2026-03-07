#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import deque
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
GEN_EXCLUDE_COLUMNS = {"DATETIME", "year"}
GEN_EXCLUDE_TARGET_SUFFIXES = ("_perc",)


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def load_weather_parquet(parquet_dir: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()


def load_generation_parquet(parquet_dir: Path, batch_size: int = 250_000) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    scanner = dataset.scanner(batch_size=batch_size)

    chunks: deque[pd.DataFrame] = deque()
    scanned_rows = 0
    for batch in scanner.to_batches():
        chunk = batch.to_pandas()
        if chunk.empty:
            continue
        scanned_rows += len(chunk)
        chunks.append(chunk)

    if not chunks:
        return pd.DataFrame(columns=["DATETIME"])

    df = pd.concat(list(chunks), ignore_index=True)
    print(f"[load] Generation scanned rows: {scanned_rows:,}")
    return df


def prepare_weather(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Date"] = pd.to_datetime(out.get("Date"), utc=True, errors="coerce")
    for col in WEATHER_COLUMNS:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    keep_cols = ["Date", *WEATHER_COLUMNS]
    out = out[keep_cols].dropna(subset=["Date"]).sort_values("Date")
    out = out.rename(columns={"Date": "timestamp"})
    return out


def pick_generation_target_columns(df: pd.DataFrame) -> list[str]:
    target_cols: list[str] = []
    for col in df.columns:
        if col in GEN_EXCLUDE_COLUMNS:
            continue
        if any(col.endswith(suffix) for suffix in GEN_EXCLUDE_TARGET_SUFFIXES):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            target_cols.append(col)
    return target_cols


def prepare_generation(df: pd.DataFrame, target_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    out["DATETIME"] = pd.to_datetime(out.get("DATETIME"), utc=True, errors="coerce")
    for col in target_cols:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    keep_cols = ["DATETIME", *target_cols]
    out = out[keep_cols].dropna(subset=["DATETIME"]).sort_values("DATETIME")
    out = out.rename(columns={"DATETIME": "timestamp"})
    return out


def merge_weather_generation(weather_df: pd.DataFrame, generation_df: pd.DataFrame, max_gap_minutes: int) -> pd.DataFrame:
    merged = pd.merge_asof(
        generation_df.sort_values("timestamp"),
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

    return features


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(
        description="Train Weather+HistoricalGeneration expert model (weather as exogenous input, generation as targets)"
    )
    parser.add_argument("--weather-parquet-dir", default=str(project_root() / "DataSources" / "Weather" / "Parquet"))
    parser.add_argument(
        "--generation-parquet-dir",
        default=str(project_root() / "DataSources" / "NESO" / "HistoricalGenerationData" / "Parquet"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "experts" / "pre-trained-experts"),
    )
    parser.add_argument("--max-merge-gap-minutes", type=int, default=60)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Maximum merged rows to keep for training (0 means all rows).",
    )
    parser.add_argument("--batch-size", type=int, default=250_000)
    parser.add_argument("--n-estimators", type=int, default=400)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    args = parser.parse_args()

    weather_parquet_dir = Path(args.weather_parquet_dir).resolve()
    generation_parquet_dir = Path(args.generation_parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not weather_parquet_dir.exists() or not list_parquet_files(weather_parquet_dir):
        print(f"[info] Weather parquet not ready: {weather_parquet_dir}")
        return 0

    if not generation_parquet_dir.exists() or not list_parquet_files(generation_parquet_dir):
        print(f"[info] Generation parquet not ready: {generation_parquet_dir}")
        return 0

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    print(f"[load] Reading weather parquet dataset from: {weather_parquet_dir}")
    weather_df = prepare_weather(load_weather_parquet(weather_parquet_dir))

    print(f"[load] Reading generation parquet dataset from: {generation_parquet_dir}")
    generation_raw = load_generation_parquet(generation_parquet_dir, batch_size=args.batch_size)
    target_cols = pick_generation_target_columns(generation_raw)
    if not target_cols:
        print("[error] No numeric generation target columns found.")
        return 1
    print(f"[info] Generation target columns ({len(target_cols)}): {target_cols}")
    generation_df = prepare_generation(generation_raw, target_cols)

    if weather_df.empty or generation_df.empty:
        print("[error] One or more source datasets are empty after preprocessing.")
        return 1

    merged = merge_weather_generation(
        weather_df=weather_df,
        generation_df=generation_df,
        max_gap_minutes=args.max_merge_gap_minutes,
    )

    merged = merged.dropna(subset=[*WEATHER_COLUMNS, *target_cols]).sort_values("timestamp")
    if merged.empty:
        print("[error] No merged rows after applying timestamp tolerance and null filtering.")
        return 1

    if args.max_rows > 0 and len(merged) > args.max_rows:
        merged = merged.iloc[-args.max_rows:].copy()
        print(f"[load] Capped merged rows to most recent {len(merged):,}")

    features = build_features(merged)
    target = merged[target_cols].copy()

    print(f"[train] Training weather+generation expert with {len(merged):,} rows on backend: {backend}")
    model, metrics, feature_cols = train_multioutput(
        features=features,
        target=target,
        target_cols=target_cols,
        n_estimators=args.n_estimators,
        random_state=args.random_state,
        train_fraction=args.train_fraction,
        backend=backend,
    )
    performance = build_performance_metrics(metrics, started_at=started_at)
    weights = summarize_feature_weights(model, feature_cols, target_cols)
    metrics["performance"] = performance
    metrics["weights"] = weights
    print_training_summary("Weather+Generation expert", backend, metrics, performance, weights)

    model_path = output_dir / "weather_generation_expert_model.joblib"
    metrics_path = output_dir / "weather_generation_expert_metrics.json"

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=target_cols,
        feature_columns=feature_cols,
        metadata={
            "dataset": "Weather+HistoricalGenerationData",
            "weather_parquet_dir": str(weather_parquet_dir),
            "generation_parquet_dir": str(generation_parquet_dir),
            "max_merge_gap_minutes": args.max_merge_gap_minutes,
            "n_estimators": args.n_estimators,
            "train_fraction": args.train_fraction,
            "random_state": args.random_state,
            "backend": backend,
            "device_arg": args.device,
            "max_rows": args.max_rows,
            "batch_size": args.batch_size,
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
