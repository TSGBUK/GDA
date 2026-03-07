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


TARGET_COLUMNS = ["Outturn Inertia", "Market Provided Inertia"]


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def load_inertia_parquet(parquet_dir: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()


def ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["DatetimeUTC"] = pd.to_datetime(out.get("DatetimeUTC"), utc=True, errors="coerce")
    for col in TARGET_COLUMNS:
        out[col] = pd.to_numeric(out.get(col), errors="coerce")
    out = out.dropna(subset=["DatetimeUTC", *TARGET_COLUMNS]).sort_values("DatetimeUTC")
    return out


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = df["DatetimeUTC"]
    features = pd.DataFrame(index=df.index)
    features["year"] = dt.dt.year.astype(np.int16)
    features["month"] = dt.dt.month.astype(np.int8)
    features["day"] = dt.dt.day.astype(np.int8)
    features["dayofweek"] = dt.dt.dayofweek.astype(np.int8)
    features["hour"] = dt.dt.hour.astype(np.int8)
    features["minute"] = dt.dt.minute.astype(np.int8)
    features["settlement_period"] = pd.to_numeric(df.get("Settlement Period"), errors="coerce").fillna(0).astype(np.int16)

    features["hour_sin"] = np.sin(2 * np.pi * features["hour"] / 24.0)
    features["hour_cos"] = np.cos(2 * np.pi * features["hour"] / 24.0)
    features["dow_sin"] = np.sin(2 * np.pi * features["dayofweek"] / 7.0)
    features["dow_cos"] = np.cos(2 * np.pi * features["dayofweek"] / 7.0)
    features["month_sin"] = np.sin(2 * np.pi * features["month"] / 12.0)
    features["month_cos"] = np.cos(2 * np.pi * features["month"] / 12.0)
    return features


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(description="Train Inertia expert model from parquet")
    parser.add_argument("--parquet-dir", default=str(project_root() / "DataSources" / "NESO" / "Inertia" / "Parquet"))
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "experts" / "pre-trained-experts"),
    )
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not parquet_dir.exists() or not list_parquet_files(parquet_dir):
        print(f"[info] Inertia parquet not ready: {parquet_dir}")
        return 0

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    print(f"[load] Reading inertia parquet dataset from: {parquet_dir}")
    df = load_inertia_parquet(parquet_dir)
    if df.empty:
        print("[error] Loaded dataframe is empty.")
        return 1

    df = ensure_datetime(df)
    features = build_features(df)
    target = df[TARGET_COLUMNS].copy()

    print(f"[train] Training inertia expert with {len(df):,} rows on backend: {backend}")
    model, metrics, feature_cols = train_multioutput(
        features=features,
        target=target,
        target_cols=TARGET_COLUMNS,
        n_estimators=args.n_estimators,
        random_state=args.random_state,
        train_fraction=args.train_fraction,
        backend=backend,
    )
    performance = build_performance_metrics(metrics, started_at=started_at)
    weights = summarize_feature_weights(model, feature_cols, TARGET_COLUMNS)
    metrics["performance"] = performance
    metrics["weights"] = weights
    print_training_summary("Inertia expert", backend, metrics, performance, weights)

    model_path = output_dir / "inertia_expert_model.joblib"
    metrics_path = output_dir / "inertia_expert_metrics.json"

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=TARGET_COLUMNS,
        feature_columns=feature_cols,
        metadata={
            "dataset": "Inertia",
            "parquet_dir": str(parquet_dir),
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
