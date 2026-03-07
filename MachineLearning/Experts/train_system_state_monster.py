#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.multioutput import MultiOutputRegressor

try:
    import joblib
except Exception as exc:  # pragma: no cover
    raise RuntimeError("joblib is required") from exc


BASE_TARGETS = [
    "frequency_hz",
    "Outturn Inertia",
    "Market Provided Inertia",
    "Temperature_C",
    "Wind_Speed_100m_kph",
    "Solar_Radiation_W_m2",
    "curtailment_proxy_mw",
]


@dataclass
class TrainRoundResult:
    round_idx: int
    n_estimators: int
    model_path: Path
    metrics_path: Path
    r2_mean: float
    rmse_mean: float
    mae_mean: float


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def find_frequency_files(freq_dir: Path) -> list[Path]:
    return sorted(path for path in freq_dir.glob("f-*.csv") if path.is_file())


def read_frequency_raw(freq_files: list[Path], max_rows: int = 0, chunk_rows: int = 500_000) -> pd.DataFrame:
    chunks: deque[pd.DataFrame] = deque()
    total_rows = 0
    scanned_rows = 0

    for file_path in freq_files:
        for raw_chunk in pd.read_csv(
            file_path,
            usecols=[0, 1],
            names=["dtm", "f"],
            header=0,
            chunksize=chunk_rows,
        ):
            chunk = pd.DataFrame()
            chunk["timestamp"] = pd.to_datetime(raw_chunk["dtm"], utc=True, errors="coerce")
            chunk["frequency_hz"] = pd.to_numeric(raw_chunk["f"], errors="coerce").astype("float32")
            chunk = chunk.dropna(subset=["timestamp", "frequency_hz"])
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
        return pd.DataFrame(columns=["timestamp", "frequency_hz"])

    out = pd.concat(list(chunks), ignore_index=True)
    out = out.sort_values("timestamp")
    print(f"[load] Frequency rows scanned: {scanned_rows:,}; using: {len(out):,}")
    return out[["timestamp", "frequency_hz"]]


def load_parquet_frame(parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
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

    out = pd.concat(list(chunks), ignore_index=True)
    print(f"[load] {parquet_dir.name}: scanned rows={scanned_rows:,}; using={len(out):,}")
    return out


def ensure_numeric_columns(df: pd.DataFrame, exclude: set[str]) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if col in exclude:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def load_weather(weather_parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    if not weather_parquet_dir.exists():
        return pd.DataFrame(columns=["timestamp"])
    df = load_parquet_frame(weather_parquet_dir, max_rows=max_rows, batch_size=batch_size)
    if "Date" not in df.columns:
        return pd.DataFrame(columns=["timestamp"])
    df["timestamp"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    df = ensure_numeric_columns(df, exclude={"Date", "timestamp", "year"})
    keep = ["timestamp"] + [c for c in ["Temperature_C", "Wind_Speed_100m_kph", "Solar_Radiation_W_m2"] if c in df.columns]
    return df[keep].dropna(subset=["timestamp"]).sort_values("timestamp")


def load_inertia(inertia_parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    if not inertia_parquet_dir.exists():
        return pd.DataFrame(columns=["timestamp"])
    df = load_parquet_frame(inertia_parquet_dir, max_rows=max_rows, batch_size=batch_size)
    if "DatetimeUTC" not in df.columns:
        return pd.DataFrame(columns=["timestamp"])
    df["timestamp"] = pd.to_datetime(df["DatetimeUTC"], utc=True, errors="coerce")
    for col in ["Outturn Inertia", "Market Provided Inertia", "Settlement Period"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    keep = ["timestamp"] + [c for c in ["Outturn Inertia", "Market Provided Inertia", "Settlement Period"] if c in df.columns]
    return df[keep].dropna(subset=["timestamp"]).sort_values("timestamp")


def load_demand(demand_parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    if not demand_parquet_dir.exists():
        return pd.DataFrame(columns=["timestamp"])
    df = load_parquet_frame(demand_parquet_dir, max_rows=max_rows, batch_size=batch_size)
    if "DatetimeUTC" not in df.columns:
        return pd.DataFrame(columns=["timestamp"])
    df["timestamp"] = pd.to_datetime(df["DatetimeUTC"], utc=True, errors="coerce")
    exclude = {"DatetimeUTC", "timestamp", "SETTLEMENT_DATE", "year"}
    for col in df.columns:
        if col in exclude:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    numeric_cols = [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    keep = ["timestamp", *numeric_cols]
    return df[keep].dropna(subset=["timestamp"]).sort_values("timestamp")


def load_balancing(balancing_parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    if not balancing_parquet_dir.exists():
        return pd.DataFrame(columns=["timestamp"])
    df = load_parquet_frame(balancing_parquet_dir, max_rows=max_rows, batch_size=batch_size)
    if "DatetimeUTC" in df.columns:
        df["timestamp"] = pd.to_datetime(df["DatetimeUTC"], utc=True, errors="coerce")
    elif {"SETT_DATE", "SETT_PERIOD"}.issubset(df.columns):
        dates = pd.to_datetime(df["SETT_DATE"], utc=True, errors="coerce")
        periods = pd.to_numeric(df["SETT_PERIOD"], errors="coerce")
        df["timestamp"] = dates + pd.to_timedelta((periods - 1) * 30, unit="m")
    else:
        return pd.DataFrame(columns=["timestamp"])

    exclude = {"SETT_DATE", "SETT_PERIOD", "DatetimeUTC", "timestamp", "year"}
    for col in df.columns:
        if col in exclude:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    numeric_cols = [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    keep = ["timestamp", *numeric_cols]
    return df[keep].dropna(subset=["timestamp"]).sort_values("timestamp")


def load_gridwatch(gridwatch_parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    if not gridwatch_parquet_dir.exists():
        return pd.DataFrame(columns=["timestamp"])
    df = load_parquet_frame(gridwatch_parquet_dir, max_rows=max_rows, batch_size=batch_size)
    if "timestamp" not in df.columns:
        return pd.DataFrame(columns=["timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    exclude = {"timestamp", "id", "year"}
    for col in df.columns:
        if col in exclude:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
    numeric_cols = [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]
    keep = ["timestamp", *numeric_cols]
    return df[keep].dropna(subset=["timestamp"]).sort_values("timestamp")


def load_generation_with_curtailment(generation_csv_path: Path, max_rows: int = 0, chunk_rows: int = 200_000) -> pd.DataFrame:
    if not generation_csv_path.exists():
        return pd.DataFrame(columns=["timestamp"])

    chunks: deque[pd.DataFrame] = deque()
    total_rows = 0
    scanned_rows = 0
    for chunk in pd.read_csv(generation_csv_path, chunksize=chunk_rows):
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
        return pd.DataFrame(columns=["timestamp"])

    df = pd.concat(list(chunks), ignore_index=True)
    print(f"[load] generation csv rows scanned={scanned_rows:,}; using={len(df):,}")
    if "DATETIME" not in df.columns:
        return pd.DataFrame(columns=["timestamp"])

    df["timestamp"] = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce")
    for col in df.columns:
        if col in {"DATETIME", "timestamp"}:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["WIND", "WIND_EMB", "SOLAR", "RENEWABLE"]:
        if col not in df.columns:
            df[col] = 0.0

    renewable_actual = df["WIND"].fillna(0.0) + df["WIND_EMB"].fillna(0.0) + df["SOLAR"].fillna(0.0)
    renewable_reference = df["RENEWABLE"].fillna(renewable_actual)
    renewable_signal = np.maximum(renewable_actual, renewable_reference)

    expected = pd.Series(renewable_signal).rolling(window=336, min_periods=48).quantile(0.95)
    df["curtailment_proxy_mw"] = np.clip(expected - renewable_signal, 0.0, None)

    numeric_cols = [c for c in df.columns if c not in {"DATETIME", "timestamp"} and pd.api.types.is_numeric_dtype(df[c])]
    keep = ["timestamp", *numeric_cols]
    return df[keep].dropna(subset=["timestamp"]).sort_values("timestamp")


def sanitize_feature_name(name: str) -> str:
    return re.sub(r"[^0-9A-Za-z_]+", "_", str(name)).strip("_")


def load_generic_parquet_timeseries(
    parquet_dir: Path,
    timestamp_candidates: list[str],
    prefix: str,
    settlement_col: str | None = None,
    drop_cols: set[str] | None = None,
    max_rows: int = 0,
    batch_size: int = 100_000,
) -> pd.DataFrame:
    if not parquet_dir.exists():
        return pd.DataFrame(columns=["timestamp"])

    df = load_parquet_frame(parquet_dir, max_rows=max_rows, batch_size=batch_size)
    if df.empty:
        return pd.DataFrame(columns=["timestamp"])

    timestamp_col = next((col for col in timestamp_candidates if col in df.columns), None)
    if timestamp_col is None:
        for col in df.columns:
            low = str(col).lower()
            if "date" in low or "time" in low:
                timestamp_col = col
                break
    if timestamp_col is None:
        return pd.DataFrame(columns=["timestamp"])

    out = df.copy()
    out["timestamp"] = pd.to_datetime(out[timestamp_col], utc=True, errors="coerce")

    if settlement_col and settlement_col in out.columns:
        periods = pd.to_numeric(out[settlement_col], errors="coerce")
        out["timestamp"] = out["timestamp"] + pd.to_timedelta((periods - 1) * 30, unit="m")

    exclude = {timestamp_col, "timestamp", "year"}
    if drop_cols:
        exclude.update(drop_cols)

    renamed: dict[str, str] = {}
    for col in out.columns:
        if col in exclude:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
        if pd.api.types.is_numeric_dtype(out[col]):
            renamed[col] = f"{prefix}_{sanitize_feature_name(col)}"

    if not renamed:
        return pd.DataFrame(columns=["timestamp"])

    keep = ["timestamp", *renamed.keys()]
    out = out[keep].dropna(subset=["timestamp"]).sort_values("timestamp")
    return out.rename(columns=renamed)


def load_bsad_aggregated(parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    return load_generic_parquet_timeseries(
        parquet_dir=parquet_dir,
        timestamp_candidates=["DatetimeUTC", "Date"],
        prefix="bsad_agg",
        settlement_col="Settlement Period",
        max_rows=max_rows,
        batch_size=batch_size,
    )


def load_bsad_forward(parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    return load_generic_parquet_timeseries(
        parquet_dir=parquet_dir,
        timestamp_candidates=["DatetimeUTC", "Date"],
        prefix="bsad_fwd",
        settlement_col="Settlement Period",
        max_rows=max_rows,
        batch_size=batch_size,
    )


def load_carbon_balancing(parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    return load_generic_parquet_timeseries(
        parquet_dir=parquet_dir,
        timestamp_candidates=["DATETIME", "DatetimeUTC", "Date"],
        prefix="carbon_bal",
        max_rows=max_rows,
        batch_size=batch_size,
    )


def load_eac_auction(parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    return load_generic_parquet_timeseries(
        parquet_dir=parquet_dir,
        timestamp_candidates=["deliveryStart", "Date", "DatetimeUTC"],
        prefix="eac",
        drop_cols={"auctionID", "orderID"},
        max_rows=max_rows,
        batch_size=batch_size,
    )


def load_ecbr_auction(parquet_dir: Path, max_rows: int = 0, batch_size: int = 100_000) -> pd.DataFrame:
    return load_generic_parquet_timeseries(
        parquet_dir=parquet_dir,
        timestamp_candidates=["deliveryStart", "Date", "DatetimeUTC"],
        prefix="ecbr",
        drop_cols={"auctionID"},
        max_rows=max_rows,
        batch_size=batch_size,
    )


def asof_join(base: pd.DataFrame, right: pd.DataFrame, tolerance: str) -> pd.DataFrame:
    if right.empty:
        return base
    return pd.merge_asof(
        base.sort_values("timestamp"),
        right.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(tolerance),
    )


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dt = out["timestamp"]
    out["year"] = dt.dt.year.astype(np.int16)
    out["month"] = dt.dt.month.astype(np.int8)
    out["day"] = dt.dt.day.astype(np.int8)
    out["dayofweek"] = dt.dt.dayofweek.astype(np.int8)
    out["hour"] = dt.dt.hour.astype(np.int8)
    out["minute"] = dt.dt.minute.astype(np.int8)
    out["second"] = dt.dt.second.astype(np.int8)

    out["hour_sin"] = np.sin(2 * np.pi * out["hour"] / 24.0)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour"] / 24.0)
    out["dow_sin"] = np.sin(2 * np.pi * out["dayofweek"] / 7.0)
    out["dow_cos"] = np.cos(2 * np.pi * out["dayofweek"] / 7.0)
    out["month_sin"] = np.sin(2 * np.pi * out["month"] / 12.0)
    out["month_cos"] = np.cos(2 * np.pi * out["month"] / 12.0)
    return out


def add_frequency_window_features(df: pd.DataFrame, windows_seconds: list[int]) -> pd.DataFrame:
    out = df.copy().sort_values("timestamp")
    freq = out["frequency_hz"]
    out["freq_delta_1"] = freq.diff(1)
    out["freq_delta_5"] = freq.diff(5)
    out["freq_delta_30"] = freq.diff(30)
    out["freq_roc_abs"] = out["freq_delta_1"].abs()
    out["under_49_8"] = (freq < 49.8).astype(np.int8)
    out["under_49_5"] = (freq < 49.5).astype(np.int8)
    out["over_50_2"] = (freq > 50.2).astype(np.int8)

    for window in windows_seconds:
        key = f"w{window}s"
        roll = freq.rolling(window=window, min_periods=max(2, window // 3))
        out[f"freq_mean_{key}"] = roll.mean()
        out[f"freq_std_{key}"] = roll.std()
        out[f"freq_min_{key}"] = roll.min()
        out[f"freq_max_{key}"] = roll.max()
    return out


def discover_expert_models(artifacts_dir: Path) -> list[Path]:
    if not artifacts_dir.exists():
        return []
    return sorted(path for path in artifacts_dir.glob("*_model.joblib") if path.is_file())


def build_time_feature_lookup(timestamp_series: pd.Series) -> pd.DataFrame:
    dt = pd.to_datetime(timestamp_series, utc=True, errors="coerce")
    lookup = pd.DataFrame(index=timestamp_series.index)
    lookup["year"] = dt.dt.year.astype(float)
    lookup["month"] = dt.dt.month.astype(float)
    lookup["day"] = dt.dt.day.astype(float)
    lookup["dayofweek"] = dt.dt.dayofweek.astype(float)
    lookup["hour"] = dt.dt.hour.astype(float)
    lookup["minute"] = dt.dt.minute.astype(float)
    lookup["second"] = dt.dt.second.astype(float)
    lookup["hour_sin"] = np.sin(2 * np.pi * lookup["hour"] / 24.0)
    lookup["hour_cos"] = np.cos(2 * np.pi * lookup["hour"] / 24.0)
    lookup["dow_sin"] = np.sin(2 * np.pi * lookup["dayofweek"] / 7.0)
    lookup["dow_cos"] = np.cos(2 * np.pi * lookup["dayofweek"] / 7.0)
    lookup["month_sin"] = np.sin(2 * np.pi * lookup["month"] / 12.0)
    lookup["month_cos"] = np.cos(2 * np.pi * lookup["month"] / 12.0)
    return lookup


def predict_with_expert_artifact(artifact_path: Path, source_df: pd.DataFrame) -> pd.DataFrame:
    payload = joblib.load(artifact_path)
    model = payload.get("model") if isinstance(payload, dict) else None
    if model is None:
        return pd.DataFrame(index=source_df.index)

    feature_columns = payload.get("feature_columns", []) if isinstance(payload, dict) else []
    target_columns = payload.get("target_columns", []) if isinstance(payload, dict) else []

    time_lookup = build_time_feature_lookup(source_df["timestamp"])
    x = pd.DataFrame(index=source_df.index)
    for col in feature_columns:
        if col in source_df.columns:
            x[col] = pd.to_numeric(source_df[col], errors="coerce")
        elif col in time_lookup.columns:
            x[col] = pd.to_numeric(time_lookup[col], errors="coerce")
        else:
            x[col] = 0.0

    x = x.replace([np.inf, -np.inf], np.nan)
    x = x.ffill().bfill().fillna(0.0)

    # CUDA bundles from common_trainer are stored as dicts with per-target models.
    if isinstance(model, dict) and model.get("backend") == "cuda":
        return pd.DataFrame(index=source_df.index)

    if not hasattr(model, "predict"):
        return pd.DataFrame(index=source_df.index)

    pred = model.predict(x)
    pred = np.asarray(pred)
    if pred.ndim == 1:
        pred = pred.reshape(-1, 1)

    if not target_columns:
        target_columns = [f"target_{idx}" for idx in range(pred.shape[1])]

    expert_name = artifact_path.stem.replace("_model", "")
    cols = [f"expert_{expert_name}_{target}" for target in target_columns]
    return pd.DataFrame(pred, columns=cols, index=source_df.index)


def add_expert_features(df: pd.DataFrame, artifacts_dir: Path) -> tuple[pd.DataFrame, list[str]]:
    out = df.copy()
    used_artifacts: list[str] = []
    for artifact_path in discover_expert_models(artifacts_dir):
        try:
            pred_df = predict_with_expert_artifact(artifact_path, out)
            if pred_df.empty:
                continue
            out = pd.concat([out, pred_df], axis=1)
            used_artifacts.append(artifact_path.name)
        except Exception:
            continue
    return out, used_artifacts


def make_feature_target_frames(
    df: pd.DataFrame,
    target_candidates: list[str],
    min_target_non_null_ratio: float,
    min_feature_non_null_ratio: float,
    min_joint_target_rows: int,
    max_feature_columns: int,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    target_cols_all = [
        col
        for col in target_candidates
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col])
    ]
    if not target_cols_all:
        raise ValueError("No available numeric target columns found for training")

    target_frame = df[target_cols_all].apply(pd.to_numeric, errors="coerce")
    target_ratios = target_frame.notna().mean()
    target_cols = [col for col in target_cols_all if float(target_ratios.get(col, 0.0)) >= min_target_non_null_ratio]
    if not target_cols:
        if "frequency_hz" in target_cols_all and float(target_ratios.get("frequency_hz", 0.0)) > 0:
            target_cols = ["frequency_hz"]
            print(
                "[warn] No joined external targets met coverage threshold; "
                "falling back to target=['frequency_hz']"
            )
        else:
            raise ValueError(
                "No target columns met non-null coverage threshold "
                f"({min_target_non_null_ratio:.3f}). Candidates={target_cols_all}"
            )

    while len(target_cols) > 1:
        joint_rows = int(target_frame[target_cols].notna().all(axis=1).sum())
        if joint_rows >= min_joint_target_rows:
            break
        drop_col = min(target_cols, key=lambda c: float(target_ratios.get(c, 0.0)))
        target_cols = [c for c in target_cols if c != drop_col]

    joint_mask = target_frame[target_cols].notna().all(axis=1)
    joint_rows = int(joint_mask.sum())
    if joint_rows <= 0:
        raise ValueError(
            "No joint target rows after target selection. "
            f"Selected targets={target_cols}; coverage={ {c: float(target_ratios[c]) for c in target_cols} }"
        )

    feature_exclude = {"timestamp", *target_cols_all}
    candidate_features = [
        col
        for col in df.columns
        if col not in feature_exclude and pd.api.types.is_numeric_dtype(df[col])
    ]
    if not candidate_features:
        raise ValueError("No numeric feature columns available")

    feature_frame = df[candidate_features].apply(pd.to_numeric, errors="coerce")
    feature_ratios = feature_frame.notna().mean()
    feature_cols = [
        col
        for col in candidate_features
        if float(feature_ratios.get(col, 0.0)) >= min_feature_non_null_ratio
    ]

    if not feature_cols:
        ranked = sorted(candidate_features, key=lambda c: float(feature_ratios.get(c, 0.0)), reverse=True)
        fallback_count = min(32, len(ranked))
        feature_cols = ranked[:fallback_count]

    if max_feature_columns > 0 and len(feature_cols) > max_feature_columns:
        feature_cols = sorted(feature_cols, key=lambda c: float(feature_ratios.get(c, 0.0)), reverse=True)[:max_feature_columns]

    features = feature_frame[feature_cols].loc[joint_mask].copy()
    target = target_frame[target_cols].loc[joint_mask].copy()

    features = features.replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0.0)
    target = target.replace([np.inf, -np.inf], np.nan)

    print(
        "[info] Monster selection | "
        f"targets={target_cols} joint_rows={joint_rows:,} "
        f"features_kept={len(feature_cols):,}/{len(candidate_features):,}"
    )
    return features, target, target_cols


def compute_metrics(y_true: pd.DataFrame, y_pred: np.ndarray, target_cols: list[str]) -> dict[str, Any]:
    per_target: dict[str, dict[str, float]] = {}
    rmse_vals: list[float] = []
    mae_vals: list[float] = []
    r2_vals: list[float] = []

    for idx, col in enumerate(target_cols):
        t = y_true.iloc[:, idx]
        p = y_pred[:, idx]
        rmse = float(np.sqrt(mean_squared_error(t, p)))
        mae = float(mean_absolute_error(t, p))
        r2 = float(r2_score(t, p))
        rmse_vals.append(rmse)
        mae_vals.append(mae)
        r2_vals.append(r2)
        per_target[col] = {"rmse": rmse, "mae": mae, "r2": r2}

    return {
        "overall": {
            "rmse_mean": float(np.mean(rmse_vals)),
            "mae_mean": float(np.mean(mae_vals)),
            "r2_mean": float(np.mean(r2_vals)),
        },
        "per_target": per_target,
    }


def chronological_split(n_rows: int, train_fraction: float) -> int:
    split_idx = max(1, int(n_rows * train_fraction))
    return min(split_idx, n_rows - 1)


def train_one_round(
    round_idx: int,
    n_estimators: int,
    x_train: pd.DataFrame,
    y_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_test: pd.DataFrame,
    target_cols: list[str],
    output_dir: Path,
    random_state: int,
) -> TrainRoundResult:
    base = RandomForestRegressor(
        n_estimators=n_estimators,
        random_state=random_state + round_idx,
        n_jobs=-1,
        max_depth=None,
        min_samples_leaf=1,
        max_features="sqrt",
    )
    model = MultiOutputRegressor(base, n_jobs=-1)
    model.fit(x_train, y_train)
    pred = model.predict(x_test)
    metrics = compute_metrics(y_test, pred, target_cols)

    model_path = output_dir / f"monster_round_{round_idx:03d}_model.joblib"
    metrics_path = output_dir / f"monster_round_{round_idx:03d}_metrics.json"

    payload = {
        "model": model,
        "target_columns": target_cols,
        "feature_columns": list(x_train.columns),
        "round": round_idx,
        "n_estimators": n_estimators,
    }
    joblib.dump(payload, model_path)

    metrics_payload = {
        "round": round_idx,
        "n_estimators": n_estimators,
        "metrics": metrics,
    }
    metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")

    return TrainRoundResult(
        round_idx=round_idx,
        n_estimators=n_estimators,
        model_path=model_path,
        metrics_path=metrics_path,
        r2_mean=metrics["overall"]["r2_mean"],
        rmse_mean=metrics["overall"]["rmse_mean"],
        mae_mean=metrics["overall"]["mae_mean"],
    )


def weighted_ensemble_manifest(results: list[TrainRoundResult], top_k: int) -> dict[str, Any]:
    top = sorted(results, key=lambda x: x.r2_mean, reverse=True)[:top_k]
    weights_raw = [max(0.0, r.r2_mean + 1.0) for r in top]
    total = float(sum(weights_raw)) if weights_raw else 1.0
    weights = [w / total for w in weights_raw]

    return {
        "strategy": "weighted_average_predictions",
        "selection_metric": "r2_mean",
        "top_k": top_k,
        "members": [
            {
                "round": r.round_idx,
                "n_estimators": r.n_estimators,
                "weight": float(weights[idx]),
                "model_file": str(r.model_path),
                "metrics_file": str(r.metrics_path),
                "r2_mean": float(r.r2_mean),
                "rmse_mean": float(r.rmse_mean),
                "mae_mean": float(r.mae_mean),
            }
            for idx, r in enumerate(top)
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Train a high-capacity system-state fusion model from raw frequency + all datasets + pretrained experts"
        )
    )

    root = project_root()
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
    parser.add_argument("--output-dir", default=str(root / "MachineLearning" / "experts" / "pre-trained-experts" / "monster"))

    parser.add_argument("--max-frequency-files", type=int, default=0, help="0 means all files")
    parser.add_argument("--max-frequency-raw-rows", type=int, default=0, help="Cap raw frequency rows loaded before resample; 0 means no cap")
    parser.add_argument("--source-max-rows", type=int, default=0, help="Cap rows loaded per auxiliary dataset source; 0 means no cap")
    parser.add_argument("--source-batch-size", type=int, default=100000, help="Batch size for parquet source scanning")
    parser.add_argument("--resolution", default="1s", help="Frequency resample resolution, e.g. 1s, 500ms")
    parser.add_argument("--join-tolerance", default="35m")
    parser.add_argument("--row-stride", type=int, default=1, help="Keep every Nth row after fusion")
    parser.add_argument("--max-rows", type=int, default=0, help="0 means no cap; otherwise keep most recent N rows")
    parser.add_argument(
        "--min-target-non-null-ratio",
        type=float,
        default=0.01,
        help="Minimum non-null ratio required for a target column to be eligible.",
    )
    parser.add_argument(
        "--min-feature-non-null-ratio",
        type=float,
        default=0.005,
        help="Minimum non-null ratio required for a feature column to be eligible.",
    )
    parser.add_argument(
        "--min-joint-target-rows",
        type=int,
        default=1000,
        help="Minimum rows where all selected targets are non-null; sparsest targets are dropped until met.",
    )
    parser.add_argument(
        "--max-feature-columns",
        type=int,
        default=512,
        help="Cap number of feature columns after coverage filtering (0 means no cap).",
    )
    parser.add_argument(
        "--min-clean-rows",
        type=int,
        default=1000,
        help="Minimum clean rows required after selection for training.",
    )

    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--base-estimators", type=int, default=300)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--top-k-merge", type=int, default=4)

    parser.add_argument("--window-seconds", default="5,30,120,600")
    parser.add_argument("--disable-expert-features", action="store_true")

    args = parser.parse_args()

    frequency_dir = Path(args.frequency_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    freq_files = find_frequency_files(frequency_dir)
    if not freq_files:
        print(f"[error] No raw frequency files found in: {frequency_dir}")
        return 1

    if args.max_frequency_files > 0:
        freq_files = freq_files[-args.max_frequency_files :]

    print(f"[load] Reading {len(freq_files)} frequency files from {frequency_dir}")
    freq_df = read_frequency_raw(freq_files, max_rows=args.max_frequency_raw_rows)

    freq_df = (
        freq_df.set_index("timestamp")
        .resample(args.resolution)
        .mean(numeric_only=True)
        .dropna(subset=["frequency_hz"])
        .reset_index()
    )

    windows_seconds = [int(x.strip()) for x in args.window_seconds.split(",") if x.strip()]
    fusion = add_time_features(freq_df)
    fusion = add_frequency_window_features(fusion, windows_seconds)

    weather = load_weather(Path(args.weather_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    inertia = load_inertia(Path(args.inertia_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    demand = load_demand(Path(args.demand_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    balancing = load_balancing(Path(args.balancing_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    gridwatch = load_gridwatch(Path(args.gridwatch_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    generation = load_generation_with_curtailment(Path(args.generation_csv_path).resolve(), max_rows=args.source_max_rows)
    bsad_aggregated = load_bsad_aggregated(Path(args.bsad_aggregated_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    bsad_forward = load_bsad_forward(Path(args.bsad_forward_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    carbon_balancing = load_carbon_balancing(Path(args.carbon_balancing_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    eac = load_eac_auction(Path(args.eac_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)
    ecbr = load_ecbr_auction(Path(args.ecbr_parquet_dir).resolve(), max_rows=args.source_max_rows, batch_size=args.source_batch_size)

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
        fusion = asof_join(fusion, frame, tolerance=args.join_tolerance)

    if not args.disable_expert_features:
        fusion, used_artifacts = add_expert_features(fusion, Path(args.experts_dir).resolve())
    else:
        used_artifacts = []

    fusion = fusion.sort_values("timestamp")
    if args.row_stride > 1:
        fusion = fusion.iloc[:: args.row_stride].copy()
    if args.max_rows > 0 and len(fusion) > args.max_rows:
        fusion = fusion.iloc[-args.max_rows :].copy()

    features, target, target_cols = make_feature_target_frames(
        fusion,
        BASE_TARGETS,
        min_target_non_null_ratio=args.min_target_non_null_ratio,
        min_feature_non_null_ratio=args.min_feature_non_null_ratio,
        min_joint_target_rows=args.min_joint_target_rows,
        max_feature_columns=args.max_feature_columns,
    )
    if len(features) < args.min_clean_rows:
        print(f"[error] Not enough clean rows for robust training: {len(features):,}")
        return 1

    split_idx = chronological_split(len(features), args.train_fraction)
    x_train = features.iloc[:split_idx]
    y_train = target.iloc[:split_idx]
    x_test = features.iloc[split_idx:]
    y_test = target.iloc[split_idx:]

    print(
        "[train] Monster training start | "
        f"rows={len(features):,} train={len(x_train):,} test={len(x_test):,} "
        f"features={len(features.columns)} targets={target_cols}"
    )

    round_results: list[TrainRoundResult] = []
    for round_idx in range(1, args.rounds + 1):
        n_estimators = args.base_estimators * round_idx
        result = train_one_round(
            round_idx=round_idx,
            n_estimators=n_estimators,
            x_train=x_train,
            y_train=y_train,
            x_test=x_test,
            y_test=y_test,
            target_cols=target_cols,
            output_dir=output_dir,
            random_state=args.random_state,
        )
        round_results.append(result)
        print(
            f"[round {round_idx:03d}] trees={n_estimators} "
            f"R2={result.r2_mean:.4f} RMSE={result.rmse_mean:.4f} MAE={result.mae_mean:.4f}"
        )

    manifest = weighted_ensemble_manifest(round_results, top_k=args.top_k_merge)
    manifest_path = output_dir / "monster_ensemble_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    run_summary = {
        "frequency_files_used": [str(p) for p in freq_files],
        "resolution": args.resolution,
        "join_tolerance": args.join_tolerance,
        "row_stride": args.row_stride,
        "max_rows": args.max_rows,
        "max_frequency_raw_rows": args.max_frequency_raw_rows,
        "source_max_rows": args.source_max_rows,
        "source_batch_size": args.source_batch_size,
        "min_target_non_null_ratio": float(args.min_target_non_null_ratio),
        "min_feature_non_null_ratio": float(args.min_feature_non_null_ratio),
        "min_joint_target_rows": int(args.min_joint_target_rows),
        "max_feature_columns": int(args.max_feature_columns),
        "min_clean_rows": int(args.min_clean_rows),
        "rows_fused": int(len(fusion)),
        "rows_clean_for_training": int(len(features)),
        "train_fraction": float(args.train_fraction),
        "targets": target_cols,
        "feature_count": int(len(features.columns)),
        "rounds": int(args.rounds),
        "base_estimators": int(args.base_estimators),
        "used_expert_artifacts": used_artifacts,
        "best_round_by_r2": {
            "round": int(max(round_results, key=lambda x: x.r2_mean).round_idx),
            "r2_mean": float(max(round_results, key=lambda x: x.r2_mean).r2_mean),
        },
        "ensemble_manifest": str(manifest_path),
    }
    summary_path = output_dir / "monster_run_summary.json"
    summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

    print(f"[done] Saved ensemble manifest: {manifest_path}")
    print(f"[done] Saved run summary: {summary_path}")
    print("[done] Use the manifest to blend top checkpoints at inference time.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
