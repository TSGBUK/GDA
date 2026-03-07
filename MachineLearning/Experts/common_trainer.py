from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.multioutput import MultiOutputRegressor

try:
    import joblib
except Exception as exc:  # pragma: no cover
    raise RuntimeError("joblib is required to save trained models") from exc


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
    return "cuda" if gpu_ok else "cpu"


def chronological_split_index(n_rows: int, train_fraction: float) -> int:
    split_idx = max(1, int(n_rows * train_fraction))
    return min(split_idx, n_rows - 1)


def metric_summary(y_true: pd.DataFrame, y_pred: np.ndarray, target_cols: list[str]) -> dict:
    per_target = {}
    rmse_list: list[float] = []
    mae_list: list[float] = []
    r2_list: list[float] = []

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
) -> tuple[Any, dict]:
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
    return model, metrics


def train_cuda(
    x_train: pd.DataFrame,
    y_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_test: pd.DataFrame,
    target_cols: list[str],
    n_estimators: int,
    random_state: int,
) -> tuple[Any, dict]:
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
    return model_bundle, metrics


def train_multioutput(
    features: pd.DataFrame,
    target: pd.DataFrame,
    target_cols: list[str],
    n_estimators: int,
    random_state: int,
    train_fraction: float,
    backend: str,
) -> tuple[Any, dict, list[str]]:
    mask = features.notna().all(axis=1) & target.notna().all(axis=1)
    features = features.loc[mask]
    target = target.loc[mask]

    if len(features) < 100:
        raise ValueError("Not enough clean rows after filtering to train a model (need at least 100)")

    split_idx = chronological_split_index(len(features), train_fraction)
    x_train = features.iloc[:split_idx]
    y_train = target.iloc[:split_idx]
    x_test = features.iloc[split_idx:]
    y_test = target.iloc[split_idx:]

    if backend == "cuda":
        model, metrics = train_cuda(
            x_train=x_train,
            y_train=y_train,
            x_test=x_test,
            y_test=y_test,
            target_cols=target_cols,
            n_estimators=n_estimators,
            random_state=random_state,
        )
    else:
        model, metrics = train_cpu(
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
        "backend": backend,
    }

    return model, metrics, list(features.columns)


def save_artifacts(
    model: Any,
    model_path: Path,
    metrics_path: Path,
    target_columns: list[str],
    feature_columns: list[str],
    metadata: dict,
    metrics: dict,
) -> None:
    model_path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        {
            "model": model,
            "target_columns": target_columns,
            "feature_columns": feature_columns,
            "metadata": metadata,
        },
        model_path,
    )

    metrics_path.write_text(
        json.dumps(
            {
                "model_file": str(model_path),
                "target_columns": target_columns,
                "feature_columns": feature_columns,
                "metrics": metrics,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _to_numpy_1d(values: Any) -> np.ndarray:
    if values is None:
        return np.array([], dtype=float)
    if hasattr(values, "to_numpy"):
        arr = values.to_numpy()
    else:
        arr = np.asarray(values)
    return np.asarray(arr, dtype=float).reshape(-1)


def _extract_target_weight_vector(model: Any, target_name: str | None = None) -> np.ndarray:
    if hasattr(model, "feature_importances_"):
        return _to_numpy_1d(getattr(model, "feature_importances_"))

    if hasattr(model, "coef_"):
        coef = np.asarray(getattr(model, "coef_"), dtype=float)
        if coef.ndim == 1:
            return np.abs(coef)
        return np.abs(coef).mean(axis=0)

    if target_name is not None and isinstance(model, dict) and "models" in model and target_name in model["models"]:
        inner = model["models"][target_name]
        return _extract_target_weight_vector(inner, None)

    if hasattr(model, "estimators_"):
        estimators = getattr(model, "estimators_")
        if isinstance(estimators, list) and estimators:
            vectors = [_extract_target_weight_vector(est, None) for est in estimators]
            vectors = [vec for vec in vectors if vec.size > 0]
            if vectors:
                width = min(vec.size for vec in vectors)
                trimmed = np.vstack([vec[:width] for vec in vectors])
                return np.mean(trimmed, axis=0)

    return np.array([], dtype=float)


def summarize_feature_weights(
    model: Any,
    feature_columns: list[str],
    target_columns: list[str],
    top_k: int = 10,
) -> dict:
    per_target: dict[str, list[dict[str, float]]] = {}
    target_vectors: list[np.ndarray] = []

    for target in target_columns:
        vec = _extract_target_weight_vector(model, target)
        if vec.size == 0:
            continue
        width = min(len(feature_columns), vec.size)
        vec = np.abs(vec[:width])
        target_vectors.append(vec)
        ranking = sorted(
            (
                {"feature": feature_columns[idx], "weight": float(vec[idx])}
                for idx in range(width)
            ),
            key=lambda item: item["weight"],
            reverse=True,
        )
        per_target[target] = ranking[:top_k]

    overall_top: list[dict[str, float]] = []
    if target_vectors:
        width = min(vec.size for vec in target_vectors)
        mat = np.vstack([vec[:width] for vec in target_vectors])
        mean_vec = np.mean(mat, axis=0)
        overall_top = sorted(
            (
                {"feature": feature_columns[idx], "weight": float(mean_vec[idx])}
                for idx in range(width)
            ),
            key=lambda item: item["weight"],
            reverse=True,
        )[:top_k]

    return {
        "top_k": int(top_k),
        "overall_top_features": overall_top,
        "per_target_top_features": per_target,
    }


def build_performance_metrics(metrics: dict, started_at: float, finished_at: float | None = None) -> dict:
    end = time.monotonic() if finished_at is None else finished_at
    elapsed = max(0.0, float(end - started_at))

    data = metrics.get("data", {}) if isinstance(metrics, dict) else {}
    rows_total = int(data.get("rows_total", 0) or 0)
    rows_train = int(data.get("rows_train", 0) or 0)
    rows_test = int(data.get("rows_test", 0) or 0)

    return {
        "elapsed_seconds": elapsed,
        "rows_total": rows_total,
        "rows_train": rows_train,
        "rows_test": rows_test,
        "rows_per_second_total": float(rows_total / elapsed) if elapsed > 0 and rows_total > 0 else 0.0,
        "rows_per_second_train": float(rows_train / elapsed) if elapsed > 0 and rows_train > 0 else 0.0,
        "rows_per_second_test": float(rows_test / elapsed) if elapsed > 0 and rows_test > 0 else 0.0,
    }


def print_training_summary(
    label: str,
    backend: str,
    metrics: dict,
    performance: dict,
    weights: dict | None = None,
) -> None:
    overall = metrics.get("overall", {}) if isinstance(metrics, dict) else {}
    print("")
    print("┌───────────────────────────────────────────────────────────────")
    print(f"│ Summary: {label}")
    print("├───────────────────────────────────────────────────────────────")
    print(f"│ Backend      : {backend}")
    print(f"│ Elapsed      : {performance.get('elapsed_seconds', 0.0):.2f}s")
    print(
        "│ Throughput   : "
        f"{performance.get('rows_per_second_total', 0.0):,.2f} rows/s "
        f"(train={performance.get('rows_per_second_train', 0.0):,.2f}, "
        f"test={performance.get('rows_per_second_test', 0.0):,.2f})"
    )
    print(
        "│ Rows         : "
        f"total={performance.get('rows_total', 0):,} "
        f"train={performance.get('rows_train', 0):,} "
        f"test={performance.get('rows_test', 0):,}"
    )
    print(
        "│ Metrics      : "
        f"RMSE={float(overall.get('rmse_mean', 0.0)):.4f} "
        f"MAE={float(overall.get('mae_mean', 0.0)):.4f} "
        f"R2={float(overall.get('r2_mean', 0.0)):.4f}"
    )

    top = (weights or {}).get("overall_top_features", []) if isinstance(weights, dict) else []
    if top:
        rendered = ", ".join(f"{item['feature']}={item['weight']:.4f}" for item in top[:5])
        print(f"│ Top weights  : {rendered}")
    else:
        print("│ Top weights  : unavailable for this backend/model")

    print("└───────────────────────────────────────────────────────────────")


def build_calendar_time_features(timestamp_series: pd.Series) -> pd.DataFrame:
    dt = pd.to_datetime(timestamp_series, utc=True, errors="coerce")
    out = pd.DataFrame(index=timestamp_series.index)
    out["year"] = dt.dt.year.astype(float)
    out["month"] = dt.dt.month.astype(float)
    out["day"] = dt.dt.day.astype(float)
    out["dayofweek"] = dt.dt.dayofweek.astype(float)
    out["hour"] = dt.dt.hour.astype(float)
    out["minute"] = dt.dt.minute.astype(float)
    out["second"] = dt.dt.second.astype(float)
    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24.0)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24.0)
    out["dow_sin"] = np.sin(2 * np.pi * out["dayofweek"] / 7.0)
    out["dow_cos"] = np.cos(2 * np.pi * out["dayofweek"] / 7.0)
    out["month_sin"] = np.sin(2 * np.pi * out["month"] / 12.0)
    out["month_cos"] = np.cos(2 * np.pi * out["month"] / 12.0)
    return out


def select_numeric_targets(
    df: pd.DataFrame,
    exclude: set[str] | None = None,
    min_non_null_ratio: float = 0.35,
    max_targets: int = 24,
) -> list[str]:
    exclude = exclude or set()
    candidates: list[tuple[str, float]] = []

    for col in df.columns:
        if col in exclude:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        ratio = float(series.notna().mean())
        if ratio >= min_non_null_ratio and series.nunique(dropna=True) > 1:
            candidates.append((col, ratio))

    ordered = [col for col, _ in sorted(candidates, key=lambda x: x[1], reverse=True)]
    return ordered[:max_targets]
