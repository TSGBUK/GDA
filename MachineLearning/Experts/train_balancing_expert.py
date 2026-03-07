#!/usr/bin/env python3
"""Train a BalancingServices expert model from parquet data.

The model is intentionally simple and robust:
- Input: time/settlement features from BalancingServices parquet records
- Target: all numeric balancing cost columns found in the dataset
- Model: multi-output RandomForestRegressor

Outputs:
- model artifact (.joblib)
- metrics/metadata JSON

If parquet is not available yet, the script exits gracefully with guidance.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.multioutput import MultiOutputRegressor

from common_trainer import build_performance_metrics, print_training_summary, summarize_feature_weights

try:
    import joblib
except Exception as exc:  # pragma: no cover
    raise RuntimeError("joblib is required to save trained models") from exc


EXCLUDE_COLUMNS = {"SETT_DATE", "SETT_PERIOD", "DatetimeUTC", "year"}


def gpu_stack_available() -> bool:
    try:
        import cudf  # noqa: F401
        from cuml.ensemble import RandomForestRegressor as _  # noqa: F401
        return True
    except Exception:
        return False


def resolve_backend(device: str) -> str:
    gpu_ok = gpu_stack_available()
    if device == "cpu":
        return "cpu"
    if device == "cuda":
        if not gpu_ok:
            raise RuntimeError("CUDA backend requested but cuDF/cuML are not available")
        return "cuda"

    # auto
    return "cuda" if gpu_ok else "cpu"


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def load_balancing_parquet(parquet_dir: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    table = dataset.to_table()
    return table.to_pandas()


def ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "DatetimeUTC" in out.columns:
        out["DatetimeUTC"] = pd.to_datetime(out["DatetimeUTC"], utc=True, errors="coerce")
    else:
        date_series = pd.to_datetime(out.get("SETT_DATE"), utc=True, errors="coerce", dayfirst=True)
        period_series = pd.to_numeric(out.get("SETT_PERIOD"), errors="coerce")
        out["DatetimeUTC"] = date_series + pd.to_timedelta((period_series - 1) * 30, unit="m")

    out = out.dropna(subset=["DatetimeUTC"]).sort_values("DatetimeUTC")
    return out


def pick_target_columns(df: pd.DataFrame) -> list[str]:
    targets: list[str] = []
    for column in df.columns:
        if column in EXCLUDE_COLUMNS:
            continue
        if pd.api.types.is_numeric_dtype(df[column]):
            targets.append(column)
    return targets


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = df["DatetimeUTC"]
    features = pd.DataFrame(index=df.index)
    features["year"] = dt.dt.year.astype(np.int16)
    features["month"] = dt.dt.month.astype(np.int8)
    features["day"] = dt.dt.day.astype(np.int8)
    features["dayofweek"] = dt.dt.dayofweek.astype(np.int8)
    features["hour"] = dt.dt.hour.astype(np.int8)
    features["minute"] = dt.dt.minute.astype(np.int8)
    features["sett_period"] = pd.to_numeric(df.get("SETT_PERIOD"), errors="coerce").fillna(0).astype(np.int16)

    # cyclical encodings for periodic components
    features["hour_sin"] = np.sin(2 * np.pi * features["hour"] / 24.0)
    features["hour_cos"] = np.cos(2 * np.pi * features["hour"] / 24.0)
    features["dow_sin"] = np.sin(2 * np.pi * features["dayofweek"] / 7.0)
    features["dow_cos"] = np.cos(2 * np.pi * features["dayofweek"] / 7.0)
    features["month_sin"] = np.sin(2 * np.pi * features["month"] / 12.0)
    features["month_cos"] = np.cos(2 * np.pi * features["month"] / 12.0)

    return features


def chronological_split_count(n_rows: int, train_fraction: float) -> tuple[int, int]:
    split_idx = max(1, int(n_rows * train_fraction))
    split_idx = min(split_idx, n_rows - 1)
    return split_idx, n_rows - split_idx


def metric_summary(y_true: pd.DataFrame, y_pred: np.ndarray, target_cols: list[str]) -> dict:
    per_target = {}
    rmse_list = []
    mae_list = []
    r2_list = []

    for idx, col in enumerate(target_cols):
        y_t = y_true.iloc[:, idx]
        y_p = y_pred[:, idx]
        rmse = float(np.sqrt(mean_squared_error(y_t, y_p)))
        mae = float(mean_absolute_error(y_t, y_p))
        r2 = float(r2_score(y_t, y_p))
        rmse_list.append(rmse)
        mae_list.append(mae)
        r2_list.append(r2)
        per_target[col] = {"rmse": rmse, "mae": mae, "r2": r2}

    return {
        "overall": {
            "rmse_mean": float(np.mean(rmse_list)),
            "mae_mean": float(np.mean(mae_list)),
            "r2_mean": float(np.mean(r2_list)),
        },
        "per_target": per_target,
    }


def train_cpu(
    x_train: pd.DataFrame,
    y_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_test: pd.DataFrame,
    target_cols: list[str],
    n_estimators: int,
    random_state: int,
) -> tuple[Any, np.ndarray, dict]:
    base_model = RandomForestRegressor(
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1,
        max_depth=None,
        min_samples_leaf=2,
    )
    model = MultiOutputRegressor(base_model, n_jobs=1)
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    metrics = metric_summary(y_test, y_pred, target_cols)
    return model, y_pred, metrics


def train_cuda(
    x_train: pd.DataFrame,
    y_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_test: pd.DataFrame,
    target_cols: list[str],
    n_estimators: int,
    random_state: int,
) -> tuple[Any, np.ndarray, dict]:
    import cudf
    from cuml.ensemble import RandomForestRegressor as CuRandomForestRegressor

    x_train_gpu = cudf.DataFrame.from_pandas(x_train.reset_index(drop=True))
    x_test_gpu = cudf.DataFrame.from_pandas(x_test.reset_index(drop=True))

    models = {}
    preds = []
    for column in target_cols:
        y_train_gpu = cudf.Series(y_train[column].reset_index(drop=True).astype(float))
        model = CuRandomForestRegressor(
            n_estimators=n_estimators,
            random_state=random_state,
            max_depth=16,
            min_samples_leaf=2,
        )
        model.fit(x_train_gpu, y_train_gpu)
        pred_gpu = model.predict(x_test_gpu)
        pred_np = pred_gpu.to_numpy() if hasattr(pred_gpu, "to_numpy") else np.asarray(pred_gpu)
        models[column] = model
        preds.append(pred_np)

    y_pred = np.column_stack(preds)
    metrics = metric_summary(y_test.reset_index(drop=True), y_pred, target_cols)
    model_bundle = {"backend": "cuda", "models": models}
    return model_bundle, y_pred, metrics


def train_model(
    df: pd.DataFrame,
    target_cols: list[str],
    n_estimators: int,
    random_state: int,
    train_fraction: float,
    backend: str,
) -> tuple[Any, dict, list[str], str]:
    features = build_features(df)
    target = df[target_cols].copy()

    mask = features.notna().all(axis=1) & target.notna().all(axis=1)
    features = features.loc[mask]
    target = target.loc[mask]

    if len(features) < 100:
        raise ValueError("Not enough clean rows after filtering to train a model (need at least 100)")

    split_idx, _ = chronological_split_count(len(features), train_fraction)
    x_train = features.iloc[:split_idx]
    y_train = target.iloc[:split_idx]
    x_test = features.iloc[split_idx:]
    y_test = target.iloc[split_idx:]

    if backend == "cuda":
        model, _, metrics = train_cuda(
            x_train=x_train,
            y_train=y_train,
            x_test=x_test,
            y_test=y_test,
            target_cols=target_cols,
            n_estimators=n_estimators,
            random_state=random_state,
        )
    else:
        model, _, metrics = train_cpu(
            x_train=x_train,
            y_train=y_train,
            x_test=x_test,
            y_test=y_test,
            target_cols=target_cols,
            n_estimators=n_estimators,
            random_state=random_state,
        )

    metrics["data"] = {
        "rows_total": int(len(features)),
        "rows_train": int(len(x_train)),
        "rows_test": int(len(x_test)),
        "train_fraction": float(train_fraction),
    }
    metrics["backend"] = backend

    return model, metrics, list(features.columns), backend


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(description="Train BalancingServices expert model from parquet")
    parser.add_argument(
        "--parquet-dir",
        default=str(project_root() / "DataSources" / "NESO" / "BalancingServices" / "Parquet"),
        help="Path to BalancingServices parquet directory",
    )
    parser.add_argument(
        "--output-dir",
        default=str(project_root() / "MachineLearning" / "experts" / "pre-trained-experts"),
        help="Directory to write model artifacts",
    )
    parser.add_argument("--n-estimators", type=int, default=300, help="Number of trees (default: 300)")
    parser.add_argument("--train-fraction", type=float, default=0.8, help="Chronological train split fraction")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--device",
        choices=["auto", "cpu", "cuda"],
        default="auto",
        help="Training backend: auto (default), cpu, or cuda",
    )
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not parquet_dir.exists():
        print(f"[info] Parquet directory not found yet: {parquet_dir}")
        print("[info] Conversion may still be running. Re-run this trainer when parquet is ready.")
        return 0

    parquet_files = list_parquet_files(parquet_dir)
    if not parquet_files:
        print(f"[info] No parquet files found under: {parquet_dir}")
        print("[info] Conversion may still be running. Re-run this trainer when parquet is ready.")
        return 0

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    print(f"[load] Reading balancing parquet dataset from: {parquet_dir}")
    df = load_balancing_parquet(parquet_dir)
    if df.empty:
        print("[error] Loaded dataframe is empty.")
        return 1

    df = ensure_datetime(df)
    target_cols = pick_target_columns(df)
    if not target_cols:
        print("[error] No numeric target columns found for training.")
        return 1

    print(f"[info] Target columns: {target_cols}")
    print(f"[train] Training with {len(df):,} rows on backend: {backend}")

    model, metrics, feature_cols, actual_backend = train_model(
        df=df,
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
    print_training_summary("Balancing expert", actual_backend, metrics, performance, weights)

    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "balancing_expert_model.joblib"
    metrics_path = output_dir / "balancing_expert_metrics.json"

    joblib.dump(
        {
            "model": model,
            "target_columns": target_cols,
            "feature_columns": feature_cols,
            "metadata": {
                "parquet_dir": str(parquet_dir),
                "n_estimators": args.n_estimators,
                "train_fraction": args.train_fraction,
                "random_state": args.random_state,
                "backend": actual_backend,
                "device_arg": args.device,
            },
        },
        model_path,
    )

    metrics_path.write_text(
        json.dumps(
            {
                "model_file": str(model_path),
                "target_columns": target_cols,
                "feature_columns": feature_cols,
                "metrics": metrics,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"[done] Model saved: {model_path}")
    print(f"[done] Metrics saved: {metrics_path}")
    print(
        "[done] Overall test metrics -> "
        f"RMSE(mean): {metrics['overall']['rmse_mean']:.2f}, "
        f"MAE(mean): {metrics['overall']['mae_mean']:.2f}, "
        f"R2(mean): {metrics['overall']['r2_mean']:.3f}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
