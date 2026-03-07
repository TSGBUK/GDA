#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import deque
from pathlib import Path
import time
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.types as pat

from common_trainer import (
    build_performance_metrics,
    metric_summary,
    print_training_summary,
    resolve_backend,
    save_artifacts,
    summarize_feature_weights,
    train_multioutput,
)


EXCLUDE_COLUMNS = {"timestamp", "id", "year"}


class WeightedAverageMultiOutputRegressor:
    def __init__(
        self,
        model_bundles: list[dict[str, Any]],
        target_cols: list[str],
        weights: list[float] | None = None,
    ) -> None:
        self.model_bundles = model_bundles
        self.target_cols = target_cols
        raw_weights = np.asarray(weights if weights is not None else [1.0] * len(model_bundles), dtype=float)
        self.weights = raw_weights if raw_weights.size > 0 else np.array([], dtype=float)

    def predict(self, x: pd.DataFrame | np.ndarray) -> np.ndarray:
        if not self.model_bundles:
            return np.zeros((len(x), len(self.target_cols)), dtype=float)

        x_df = pd.DataFrame(x)
        preds_by_bundle: list[np.ndarray] = []

        try:
            import cudf

            x_gpu = cudf.DataFrame.from_pandas(x_df.reset_index(drop=True))
            for bundle in self.model_bundles:
                per_target: list[np.ndarray] = []
                for target_col in self.target_cols:
                    model = bundle[target_col]
                    pred_gpu = model.predict(x_gpu)
                    pred_np = pred_gpu.to_numpy() if hasattr(pred_gpu, "to_numpy") else np.asarray(pred_gpu)
                    per_target.append(np.asarray(pred_np, dtype=float).reshape(-1))
                preds_by_bundle.append(np.column_stack(per_target))
        except Exception:
            for bundle in self.model_bundles:
                per_target = []
                for target_col in self.target_cols:
                    model = bundle[target_col]
                    pred = model.predict(x_df)
                    per_target.append(np.asarray(pred, dtype=float).reshape(-1))
                preds_by_bundle.append(np.column_stack(per_target))

        if len(preds_by_bundle) == 1:
            return preds_by_bundle[0]

        stack = np.stack(preds_by_bundle, axis=0)
        if self.weights.size == len(preds_by_bundle) and self.weights.sum() > 0:
            normalized = self.weights / self.weights.sum()
            return np.tensordot(normalized, stack, axes=(0, 0))
        return stack.mean(axis=0)


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def load_gridwatch_parquet(parquet_dir: Path, max_rows: int = 0, batch_size: int = 250_000) -> pd.DataFrame:
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
        return pd.DataFrame()

    df = pd.concat(list(chunks), ignore_index=True)
    print(f"[load] Scanned rows: {scanned_rows:,}; using rows in memory: {len(df):,}")
    return df


def ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out.get("timestamp"), utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp")
    return out


def pick_target_columns(df: pd.DataFrame) -> list[str]:
    targets: list[str] = []
    for column in df.columns:
        if column in EXCLUDE_COLUMNS:
            continue
        if pd.api.types.is_numeric_dtype(df[column]):
            targets.append(column)
    return targets


def pick_target_columns_from_schema(parquet_dir: Path) -> list[str]:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    targets: list[str] = []
    for field in dataset.schema:
        if field.name in EXCLUDE_COLUMNS:
            continue
        if pat.is_integer(field.type) or pat.is_floating(field.type) or pat.is_decimal(field.type):
            targets.append(field.name)
    return targets


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
    return features


def iter_gridwatch_chunks(parquet_dir: Path, batch_size: int):
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    scanner = dataset.scanner(batch_size=batch_size)
    for batch in scanner.to_batches():
        chunk = batch.to_pandas()
        if chunk.empty:
            continue
        yield chunk


def train_cuda_sharded_gridwatch(
    parquet_dir: Path,
    batch_size: int,
    train_fraction: float,
    random_state: int,
    n_estimators: int,
    target_cols: list[str],
    shard_rows: int,
    progress_every_chunks: int,
) -> tuple[WeightedAverageMultiOutputRegressor, dict, list[str]]:
    try:
        import cudf
        from cuml.ensemble import RandomForestRegressor as CuRandomForestRegressor
    except Exception as exc:
        raise ValueError(f"CUDA sharded mode requires cuDF/cuML: {exc}") from exc

    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    total_rows = int(dataset.count_rows())
    if total_rows < 100:
        raise ValueError("Not enough rows in dataset to train (need at least 100).")

    train_row_cutoff = max(1, int(total_rows * train_fraction))
    effective_shard_rows = int(shard_rows) if int(shard_rows) > 0 else 400_000
    print(
        f"[info] Gridwatch CUDA-sharded rows={total_rows:,}, train cutoff={train_row_cutoff:,}, "
        f"shard_rows={effective_shard_rows:,}"
    )

    shard_x_parts: list[np.ndarray] = []
    shard_y_parts: list[np.ndarray] = []
    shard_rows_accum = 0

    model_bundles: list[dict[str, Any]] = []
    model_weights: list[float] = []

    seen_rows = 0
    trained_rows = 0
    test_rows = 0
    feature_cols: list[str] | None = None
    chunk_count = 0
    started = time.monotonic()

    sum_abs = np.zeros(len(target_cols), dtype=np.float64)
    sum_sq = np.zeros(len(target_cols), dtype=np.float64)
    sum_y = np.zeros(len(target_cols), dtype=np.float64)
    sum_y2 = np.zeros(len(target_cols), dtype=np.float64)

    def fit_one_shard() -> None:
        nonlocal shard_x_parts, shard_y_parts, shard_rows_accum
        if not shard_x_parts:
            return

        x_np = np.concatenate(shard_x_parts, axis=0)
        y_np = np.concatenate(shard_y_parts, axis=0)
        shard_x_parts = []
        shard_y_parts = []
        shard_rows_accum = 0

        if len(y_np) < 100:
            return

        x_gpu = cudf.DataFrame.from_pandas(pd.DataFrame(x_np, columns=feature_cols))
        bundle: dict[str, Any] = {}
        for target_idx, target_col in enumerate(target_cols):
            y_gpu = cudf.Series(y_np[:, target_idx])
            model = CuRandomForestRegressor(
                n_estimators=n_estimators,
                random_state=random_state + len(model_bundles),
                max_depth=16,
                min_samples_leaf=2,
            )
            model.fit(x_gpu, y_gpu)
            bundle[target_col] = model

        model_bundles.append(bundle)
        model_weights.append(float(len(y_np)))

    for raw_chunk in iter_gridwatch_chunks(parquet_dir, batch_size=batch_size):
        chunk_count += 1
        prepared = ensure_datetime(raw_chunk)
        if prepared.empty:
            continue

        features = build_features(prepared)
        target = prepared[target_cols].apply(pd.to_numeric, errors="coerce")

        mask = features.notna().all(axis=1) & target.notna().all(axis=1)
        features = features.loc[mask]
        target = target.loc[mask]
        if features.empty:
            continue

        if feature_cols is None:
            feature_cols = list(features.columns)

        x_chunk = features.to_numpy(dtype=np.float32)
        y_chunk = target.to_numpy(dtype=np.float64)
        chunk_rows = len(y_chunk)

        train_take = max(0, min(chunk_rows, train_row_cutoff - seen_rows))
        test_take = chunk_rows - train_take

        if train_take > 0:
            x_train = x_chunk[:train_take]
            y_train = y_chunk[:train_take]
            shard_x_parts.append(x_train)
            shard_y_parts.append(y_train)
            shard_rows_accum += train_take
            trained_rows += train_take
            if shard_rows_accum >= effective_shard_rows:
                fit_one_shard()

        if test_take > 0:
            if not model_bundles:
                fit_one_shard()
            if not model_bundles:
                raise ValueError("No CUDA shard models were trained before test segment.")

            x_test = x_chunk[train_take:]
            y_test = y_chunk[train_take:]
            ensemble = WeightedAverageMultiOutputRegressor(model_bundles=model_bundles, target_cols=target_cols, weights=model_weights)
            y_pred = ensemble.predict(pd.DataFrame(x_test, columns=feature_cols))

            err = y_pred - y_test
            test_rows += len(y_test)
            sum_abs += np.sum(np.abs(err), axis=0)
            sum_sq += np.sum(np.square(err), axis=0)
            sum_y += np.sum(y_test, axis=0)
            sum_y2 += np.sum(np.square(y_test), axis=0)

        seen_rows += chunk_rows

        if chunk_count == 1 or (progress_every_chunks > 0 and chunk_count % progress_every_chunks == 0):
            elapsed = time.monotonic() - started
            pct = (seen_rows / total_rows * 100.0) if total_rows > 0 else 0.0
            print(
                f"[progress] chunks={chunk_count:,} rows_seen={seen_rows:,}/{total_rows:,} ({pct:.1f}%) "
                f"train_rows={trained_rows:,} test_rows={test_rows:,} models={len(model_bundles):,} elapsed={elapsed:.1f}s"
            )

    fit_one_shard()
    if not model_bundles:
        raise ValueError("No CUDA shard models were trained.")
    if test_rows <= 0:
        raise ValueError("No test rows were produced during CUDA-sharded training.")

    per_target = {}
    rmse_vals = []
    mae_vals = []
    r2_vals = []
    for idx, col in enumerate(target_cols):
        n = float(test_rows)
        rmse = float(np.sqrt(sum_sq[idx] / n))
        mae = float(sum_abs[idx] / n)
        sst = float(sum_y2[idx] - (sum_y[idx] ** 2) / n)
        r2 = float(1.0 - (sum_sq[idx] / sst)) if sst > 0 else 0.0
        per_target[col] = {"rmse": rmse, "mae": mae, "r2": r2}
        rmse_vals.append(rmse)
        mae_vals.append(mae)
        r2_vals.append(r2)

    metrics = {
        "overall": {
            "rmse_mean": float(np.mean(rmse_vals)) if rmse_vals else 0.0,
            "mae_mean": float(np.mean(mae_vals)) if mae_vals else 0.0,
            "r2_mean": float(np.mean(r2_vals)) if r2_vals else 0.0,
        },
        "per_target": per_target,
        "data": {
            "rows_total": int(trained_rows + test_rows),
            "rows_train": int(trained_rows),
            "rows_test": int(test_rows),
            "train_fraction": float(train_fraction),
            "backend": "cuda-sharded",
            "models": int(len(model_bundles)),
            "shard_rows": int(effective_shard_rows),
        },
    }

    elapsed = time.monotonic() - started
    print(
        f"[done] Gridwatch CUDA-sharded training finished in {elapsed:.1f}s; "
        f"models={len(model_bundles):,}, train_rows={trained_rows:,}, test_rows={test_rows:,}"
    )

    ensemble = WeightedAverageMultiOutputRegressor(model_bundles=model_bundles, target_cols=target_cols, weights=model_weights)
    return ensemble, metrics, (feature_cols or [])


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(description="Train Gridwatch expert model from parquet")
    parser.add_argument("--parquet-dir", default=str(project_root() / "DataSources" / "GridWatch" / "Parquet"))
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "experts" / "pre-trained-experts"),
    )
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    parser.add_argument(
        "--training-mode",
        choices=["auto", "batch", "cuda-sharded"],
        default="auto",
        help="auto uses cuda-sharded for large CUDA datasets and batch otherwise.",
    )
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
    parser.add_argument(
        "--progress-every-chunks",
        type=int,
        default=25,
        help="Print sharded progress every N parquet chunks (set 0 to disable periodic progress).",
    )
    parser.add_argument(
        "--cuda-shard-rows",
        type=int,
        default=400_000,
        help="Rows per CUDA shard model in cuda-sharded mode.",
    )
    parser.add_argument(
        "--cuda-batch-max-rows",
        type=int,
        default=800_000,
        help="In auto mode, use batch CUDA only when dataset rows are <= this threshold.",
    )
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not parquet_dir.exists() or not list_parquet_files(parquet_dir):
        print(f"[info] Gridwatch parquet not ready: {parquet_dir}")
        return 0

    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    total_rows = int(dataset.count_rows())

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    requested_mode = args.training_mode
    effective_mode = requested_mode
    if requested_mode == "auto":
        if backend == "cuda" and args.max_rows == 0 and total_rows > args.cuda_batch_max_rows:
            effective_mode = "cuda-sharded"
        else:
            effective_mode = "batch"
        print(f"[info] Auto mode selected '{effective_mode}'")

    if effective_mode == "cuda-sharded" and backend != "cuda":
        print("[warn] cuda-sharded requested without CUDA backend; falling back to batch mode.")
        effective_mode = "batch"

    print(f"[load] Reading gridwatch parquet dataset from: {parquet_dir}")
    if effective_mode == "cuda-sharded":
        target_cols = pick_target_columns_from_schema(parquet_dir)
    else:
        df = load_gridwatch_parquet(parquet_dir, max_rows=args.max_rows, batch_size=args.batch_size)
        if df.empty:
            print("[error] Loaded dataframe is empty.")
            return 1

        df = ensure_datetime(df)
        target_cols = pick_target_columns(df)

    if not target_cols:
        print("[error] No numeric target columns found.")
        return 1

    if effective_mode == "cuda-sharded":
        print(
            f"[train] Training gridwatch expert on backend: cuda-sharded "
            f"(rows={total_rows:,}, max_rows={args.max_rows})"
        )
        model, metrics, feature_cols = train_cuda_sharded_gridwatch(
            parquet_dir=parquet_dir,
            batch_size=args.batch_size,
            train_fraction=args.train_fraction,
            random_state=args.random_state,
            n_estimators=args.n_estimators,
            target_cols=target_cols,
            shard_rows=args.cuda_shard_rows,
            progress_every_chunks=args.progress_every_chunks,
        )
    else:
        features = build_features(df)
        target = df[target_cols].copy()
        print(f"[train] Training gridwatch expert with {len(df):,} rows on backend: {backend}")
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
    print_training_summary("Gridwatch expert", backend, metrics, performance, weights)

    model_path = output_dir / "gridwatch_expert_model.joblib"
    metrics_path = output_dir / "gridwatch_expert_metrics.json"

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=target_cols,
        feature_columns=feature_cols,
        metadata={
            "dataset": "GridWatch",
            "parquet_dir": str(parquet_dir),
            "n_estimators": args.n_estimators,
            "train_fraction": args.train_fraction,
            "random_state": args.random_state,
            "backend": backend,
            "training_mode": effective_mode,
            "device_arg": args.device,
            "max_rows": args.max_rows,
            "batch_size": args.batch_size,
            "cuda_shard_rows": args.cuda_shard_rows,
            "cuda_batch_max_rows": args.cuda_batch_max_rows,
            "progress_every_chunks": args.progress_every_chunks,
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
