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


EXCLUDE_COLUMNS = {"DATETIME", "year"}
EXCLUDE_TARGET_SUFFIXES = ("_perc",)


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def load_generation_parquet(parquet_dir: Path, max_rows: int = 0, batch_size: int = 250_000) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    scanner = dataset.scanner(batch_size=batch_size)

    chunks: deque[pd.DataFrame] = deque()
    total_rows = 0
    scanned_rows = 0

    for batch in scanner.to_batches():
        chunk = batch.to_pandas()
        if chunk.empty:
            continue

        scanned_rows += len(chunk)
        chunks.append(chunk)
        total_rows += len(chunk)

        if max_rows > 0:
            while total_rows > max_rows and chunks:
                overflow = total_rows - max_rows
                left = chunks[0]
                if len(left) <= overflow:
                    total_rows -= len(left)
                    chunks.popleft()
                else:
                    chunks[0] = left.iloc[overflow:].reset_index(drop=True)
                    total_rows -= overflow

    if not chunks:
        return pd.DataFrame(columns=["DATETIME"])

    df = pd.concat(list(chunks), ignore_index=True)
    print(f"[load] Scanned rows: {scanned_rows:,}; using rows in memory: {len(df):,}")
    return df.sort_values("DATETIME").reset_index(drop=True)


def pick_target_columns(df: pd.DataFrame) -> list[str]:
    targets: list[str] = []
    for column in df.columns:
        if column in EXCLUDE_COLUMNS:
            continue
        if any(column.endswith(suffix) for suffix in EXCLUDE_TARGET_SUFFIXES):
            continue
        if pd.api.types.is_numeric_dtype(df[column]):
            targets.append(column)
    return targets


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce")
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
    return features


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(description="Train HistoricalGeneration expert model")
    parser.add_argument(
        "--parquet-dir",
        default=str(project_root() / "DataSources" / "NESO" / "HistoricalGenerationData" / "Parquet"),
        help="Path to historical generation parquet directory",
    )
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "experts" / "pre-trained-experts"),
    )
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Maximum rows to keep in memory for training (0 means all rows).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250_000,
        help="Arrow scanner batch size while streaming parquet data.",
    )
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not parquet_dir.exists() or not list_parquet_files(parquet_dir):
        print(f"[info] Generation parquet not ready: {parquet_dir}")
        return 0

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    print(f"[load] Reading generation parquet dataset from: {parquet_dir}")
    df = load_generation_parquet(parquet_dir, max_rows=args.max_rows, batch_size=args.batch_size)
    if df.empty:
        print("[error] Loaded dataframe is empty.")
        return 1

    target_cols = pick_target_columns(df)
    if not target_cols:
        print("[error] No numeric target columns found.")
        return 1
    print(f"[info] Target columns ({len(target_cols)}): {target_cols}")

    features = build_features(df)
    target = df[target_cols].copy()

    print(f"[train] Training generation expert with {len(df):,} rows on backend: {backend}")
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
    print_training_summary("Generation expert", backend, metrics, performance, weights)

    model_path = output_dir / "generation_expert_model.joblib"
    metrics_path = output_dir / "generation_expert_metrics.json"

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=target_cols,
        feature_columns=feature_cols,
        metadata={
            "dataset": "HistoricalGenerationData",
            "source": "parquet",
            "parquet_dir": str(parquet_dir),
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
