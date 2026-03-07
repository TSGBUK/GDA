#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
from collections import deque
import json
from pathlib import Path
import sys
import time
import re
import subprocess
import tempfile

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from sklearn.linear_model import SGDRegressor

from common_trainer import (
    build_performance_metrics,
    print_training_summary,
    resolve_backend,
    save_artifacts,
    summarize_feature_weights,
    train_multioutput,
)


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


def resolve_frequency_columns(columns: list[str]) -> tuple[str, str]:
    lower_map = {col.lower(): col for col in columns}

    date_col = None
    value_col = None

    for candidate in ("date", "datetimeutc", "datetime", "dtm", "timestamp"):
        if candidate in lower_map:
            date_col = lower_map[candidate]
            break

    for candidate in ("value", "f", "frequency", "freq"):
        if candidate in lower_map:
            value_col = lower_map[candidate]
            break

    if date_col is None or value_col is None:
        raise ValueError(
            f"Unable to resolve frequency columns from schema: {columns}. "
            "Expected date/time column (Date/dtm/DatetimeUTC) and value column (Value/f)."
        )

    return date_col, value_col


def frequency_dataset_and_columns(parquet_dir: Path) -> tuple[ds.Dataset, str, str]:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    date_col, value_col = resolve_frequency_columns(list(dataset.schema.names))
    return dataset, date_col, value_col


def load_frequency_parquet(parquet_dir: Path, max_rows: int = 0, batch_size: int = 250_000) -> pd.DataFrame:
    dataset, date_col, value_col = frequency_dataset_and_columns(parquet_dir)
    scanner = dataset.scanner(columns=[date_col, value_col], batch_size=batch_size)

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
        return pd.DataFrame(columns=["Date", "Value"])

    df = pd.concat(list(chunks), ignore_index=True)
    df = df.rename(columns={date_col: "Date", value_col: "Value"})
    print(f"[load] Scanned rows: {scanned_rows:,}; using rows in memory: {len(df):,}")
    return df


def _fragment_sort_key(fragment: ds.ParquetFileFragment) -> tuple[int, int, str]:
    path = getattr(fragment, "path", "") or ""
    year_match = re.search(r"year=(\d{4})", path)
    file_match = re.search(r"f-(\d{4})-(\d{1,2})\.parquet$", path)
    year = int(file_match.group(1)) if file_match else (int(year_match.group(1)) if year_match else 0)
    month = int(file_match.group(2)) if file_match else 0
    return year, month, path


def iter_frequency_chunks(parquet_dir: Path, batch_size: int, date_col: str, value_col: str):
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    fragments = sorted(dataset.get_fragments(), key=_fragment_sort_key)
    for fragment in fragments:
        scanner = fragment.scanner(columns=[date_col, value_col], batch_size=batch_size)
        for batch in scanner.to_batches():
            chunk = batch.to_pandas()
            if chunk.empty:
                continue
            chunk = chunk.rename(columns={date_col: "Date", value_col: "Value"})
            yield chunk


def prepare_frequency_chunk(chunk: pd.DataFrame, history: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    curr = chunk[["Date", "Value"]].copy()
    curr["Date"] = pd.to_datetime(curr.get("Date"), utc=True, errors="coerce")
    curr["Value"] = pd.to_numeric(curr.get("Value"), errors="coerce")
    curr = curr.dropna(subset=["Date", "Value"])
    if curr.empty:
        return pd.DataFrame(), history

    curr["_is_new"] = True
    base = history.copy()
    if not base.empty:
        base["_is_new"] = False
        combined = pd.concat([base, curr], ignore_index=True)
    else:
        combined = curr

    combined = combined.sort_values("Date").reset_index(drop=True)
    combined["lag_1"] = combined["Value"].shift(1)
    combined["lag_5"] = combined["Value"].shift(5)
    combined["roll_mean_10"] = combined["Value"].rolling(10, min_periods=1).mean()
    combined = combined.dropna(subset=["lag_1", "lag_5"])

    prepared = combined[combined["_is_new"]].copy()
    prepared = prepared.drop(columns=["_is_new"])

    new_history = combined[["Date", "Value"]].tail(12).copy()
    return prepared, new_history


def ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Date"] = pd.to_datetime(out.get("Date"), utc=True, errors="coerce")
    out["Value"] = pd.to_numeric(out.get("Value"), errors="coerce")
    out = out.dropna(subset=["Date", "Value"]).sort_values("Date")
    out = out.set_index("Date")
    out["lag_1"] = out["Value"].shift(1)
    out["lag_5"] = out["Value"].shift(5)
    out["roll_mean_10"] = out["Value"].rolling(10, min_periods=1).mean()
    out = out.dropna(subset=["lag_1", "lag_5"])
    return out.reset_index()


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = df["Date"]
    features = pd.DataFrame(index=df.index)
    features["year"] = dt.dt.year.astype(np.int16)
    features["month"] = dt.dt.month.astype(np.int8)
    features["day"] = dt.dt.day.astype(np.int8)
    features["dayofweek"] = dt.dt.dayofweek.astype(np.int8)
    features["hour"] = dt.dt.hour.astype(np.int8)
    features["minute"] = dt.dt.minute.astype(np.int8)
    features["second"] = dt.dt.second.astype(np.int8)
    features["lag_1"] = df["lag_1"].astype(float)
    features["lag_5"] = df["lag_5"].astype(float)
    features["roll_mean_10"] = df["roll_mean_10"].astype(float)

    features["hour_sin"] = np.sin(2 * np.pi * features["hour"] / 24.0)
    features["hour_cos"] = np.cos(2 * np.pi * features["hour"] / 24.0)
    features["dow_sin"] = np.sin(2 * np.pi * features["dayofweek"] / 7.0)
    features["dow_cos"] = np.cos(2 * np.pi * features["dayofweek"] / 7.0)
    return features


def build_metrics_from_sums(count: int, sum_abs: float, sum_sq: float, sum_y: float, sum_y2: float) -> dict:
    if count <= 0:
        raise ValueError("No test rows were produced during incremental training.")

    mae = float(sum_abs / count)
    rmse = float(np.sqrt(sum_sq / count))
    sst = float(sum_y2 - (sum_y * sum_y) / count)
    r2 = float(1.0 - (sum_sq / sst)) if sst > 0 else 0.0

    return {
        "overall": {
            "rmse_mean": rmse,
            "mae_mean": mae,
            "r2_mean": r2,
        },
        "per_target": {
            "Value": {"rmse": rmse, "mae": mae, "r2": r2}
        },
    }


def detect_visible_gpu_ids() -> list[int]:
    try:
        import cupy as cp

        count = int(cp.cuda.runtime.getDeviceCount())
        if count > 0:
            return list(range(count))
    except Exception:
        pass

    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        ids = [int(line.strip()) for line in out.splitlines() if line.strip().isdigit()]
        return ids
    except Exception:
        return []


def detect_gpu_memory_gb_map() -> dict[int, float]:
    mem_map: dict[int, float] = {}
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index,memory.total", "--format=csv,noheader,nounits"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) != 2:
                continue
            try:
                idx = int(parts[0])
                mem_mib = float(parts[1])
            except ValueError:
                continue
            mem_map[idx] = mem_mib / 1024.0
    except Exception:
        return {}
    return mem_map


def parse_gpu_ids(spec: str) -> list[int]:
    visible = detect_visible_gpu_ids()
    if not visible:
        return []

    text = (spec or "auto").strip().lower()
    if text in {"", "auto", "all"}:
        return visible

    requested: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            requested.append(int(part))
        except ValueError:
            continue

    return [gpu for gpu in requested if gpu in visible]


def _gpu_context(gpu_id: int | None):
    if gpu_id is None:
        return contextlib.nullcontext()
    try:
        import cupy as cp

        return cp.cuda.Device(gpu_id)
    except Exception:
        return contextlib.nullcontext()


class WeightedAverageRegressor:
    def __init__(self, models: list, weights: list[float], gpu_ids: list[int] | None = None):
        self.models = models
        self.weights = np.asarray(weights, dtype=float) if weights else np.array([], dtype=float)
        self.gpu_ids = gpu_ids or [None] * len(models)

    @property
    def feature_importances_(self):
        vectors = []
        for model in self.models:
            if hasattr(model, "feature_importances_"):
                vec = np.asarray(model.feature_importances_, dtype=float).reshape(-1)
                if vec.size > 0:
                    vectors.append(vec)
        if not vectors:
            return np.array([], dtype=float)
        width = min(vec.size for vec in vectors)
        mat = np.vstack([vec[:width] for vec in vectors])
        return np.mean(mat, axis=0)

    def predict(self, x):
        if not self.models:
            return np.zeros(len(x), dtype=float)

        try:
            import cudf

            x_gpu = cudf.DataFrame.from_pandas(pd.DataFrame(x).reset_index(drop=True))
            preds = []
            for model, gpu_id in zip(self.models, self.gpu_ids):
                with _gpu_context(gpu_id):
                    pred_gpu = model.predict(x_gpu)
                pred_np = pred_gpu.to_numpy() if hasattr(pred_gpu, "to_numpy") else np.asarray(pred_gpu)
                preds.append(np.asarray(pred_np, dtype=float).reshape(-1))
        except Exception:
            preds = [np.asarray(model.predict(x), dtype=float).reshape(-1) for model in self.models]

        if len(preds) == 1:
            return preds[0]

        mat = np.vstack(preds)
        if self.weights.size == len(preds) and self.weights.sum() > 0:
            w = self.weights / self.weights.sum()
            return np.average(mat, axis=0, weights=w)
        return np.mean(mat, axis=0)


def train_cuda_sharded_frequency(
    parquet_dir: Path,
    batch_size: int,
    train_fraction: float,
    random_state: int,
    n_estimators: int,
    shard_rows: int,
    target_vram_gb: float,
    gpu_ids: list[int],
    allow_multi_gpu_switch: bool,
    progress_every_chunks: int,
) -> tuple[WeightedAverageRegressor, dict, list[str]]:
    try:
        import cudf
        from cuml.ensemble import RandomForestRegressor as CuRandomForestRegressor
    except Exception as exc:
        raise ValueError(f"CUDA sharded mode requires cuDF/cuML: {exc}") from exc

    dataset, date_col, value_col = frequency_dataset_and_columns(parquet_dir)
    total_rows = int(dataset.count_rows())
    if total_rows < 100:
        raise ValueError("Not enough rows in dataset to train (need at least 100).")

    train_row_cutoff = max(1, int(total_rows * train_fraction))
    effective_shard_rows = int(shard_rows)
    if effective_shard_rows <= 0:
        inferred = int(max(1.0, target_vram_gb) * 600_000)
        effective_shard_rows = max(500_000, min(8_000_000, inferred))

    requested_gpu_cycle = gpu_ids if gpu_ids else [None]
    gpu_cycle = requested_gpu_cycle

    use_isolated_multi_gpu = len([gpu for gpu in gpu_cycle if gpu is not None]) > 1 and not allow_multi_gpu_switch
    if use_isolated_multi_gpu:
        print(
            "[info] Multi-GPU mode enabled with isolated shard workers "
            "(one subprocess per shard, pinned per GPU) for reliability."
        )

    print(
        f"[info] CUDA-sharded dataset rows={total_rows:,}, train cutoff={train_row_cutoff:,}, "
        f"shard_rows={effective_shard_rows:,}, target_vram_gb={target_vram_gb:.1f}, "
        f"gpus={gpu_cycle} (requested={requested_gpu_cycle})"
    )

    shard_x_parts: list[np.ndarray] = []
    shard_y_parts: list[np.ndarray] = []
    shard_rows_accum = 0

    models = []
    model_weights: list[float] = []
    model_gpu_ids: list[int | None] = []

    history = pd.DataFrame(columns=["Date", "Value"])
    seen_rows = 0
    trained_rows = 0
    test_rows = 0
    sum_abs = 0.0
    sum_sq = 0.0
    sum_y = 0.0
    sum_y2 = 0.0
    feature_cols: list[str] | None = None
    chunk_count = 0
    started = time.monotonic()
    worker_script = project_root() / "MachineLearning" / "experts" / "frequency_cuda_shard_worker.py"

    def fit_shard_isolated(
        x_np: np.ndarray,
        y_np: np.ndarray,
        gpu_id: int,
        model_idx: int,
    ) -> object:
        with tempfile.TemporaryDirectory(prefix="freq_shard_") as tmpdir:
            tmp_path = Path(tmpdir)
            x_path = tmp_path / "x.npy"
            y_path = tmp_path / "y.npy"
            cols_path = tmp_path / "feature_cols.json"
            model_path = tmp_path / "model.joblib"

            np.save(x_path, x_np)
            np.save(y_path, y_np)
            cols_path.write_text(json.dumps(feature_cols or []), encoding="utf-8")

            cmd = [
                sys.executable,
                str(worker_script),
                "--x-npy",
                str(x_path),
                "--y-npy",
                str(y_path),
                "--feature-cols-json",
                str(cols_path),
                "--gpu-id",
                str(gpu_id),
                "--n-estimators",
                str(n_estimators),
                "--random-state",
                str(random_state + model_idx),
                "--model-out",
                str(model_path),
            ]
            run = subprocess.run(cmd, capture_output=True, text=True)

            if run.returncode != 0:
                stderr = (run.stderr or "").strip()
                stdout = (run.stdout or "").strip()
                detail = stderr if stderr else stdout
                raise RuntimeError(
                    f"Isolated shard worker failed on GPU {gpu_id} (exit={run.returncode}): {detail}"
                )

            try:
                from joblib import load as joblib_load

                return joblib_load(model_path)
            except Exception as exc:
                raise RuntimeError(f"Unable to load isolated shard model artifact: {exc}") from exc

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

        model_idx = len(models)
        gpu_id = gpu_cycle[model_idx % len(gpu_cycle)]
        if use_isolated_multi_gpu and gpu_id is not None:
            try:
                model = fit_shard_isolated(x_np=x_np, y_np=y_np, gpu_id=gpu_id, model_idx=model_idx)
            except Exception as exc:
                primary_gpu = requested_gpu_cycle[0] if requested_gpu_cycle else 0
                print(
                    f"[warn] Isolated shard training failed on GPU {gpu_id}: {exc}. "
                    f"Retrying shard on primary GPU {primary_gpu}."
                )
                model = fit_shard_isolated(x_np=x_np, y_np=y_np, gpu_id=int(primary_gpu), model_idx=model_idx)
                gpu_id = int(primary_gpu)
            model_gpu_ids.append(None)
        else:
            with _gpu_context(gpu_id):
                x_gpu = cudf.DataFrame.from_pandas(pd.DataFrame(x_np, columns=feature_cols))
                y_gpu = cudf.Series(y_np)
                model = CuRandomForestRegressor(
                    n_estimators=n_estimators,
                    random_state=random_state + model_idx,
                    max_depth=16,
                    min_samples_leaf=2,
                )
                model.fit(x_gpu, y_gpu)
            model_gpu_ids.append(gpu_id)

        models.append(model)
        model_weights.append(float(len(y_np)))
        elapsed = time.monotonic() - started
        #print(
            #f"[progress] trained CUDA shard model #{len(models)} rows={len(y_np):,} "
            #f"gpu={gpu_id} elapsed={elapsed:.1f}s"
        #)

    for raw_chunk in iter_frequency_chunks(
        parquet_dir,
        batch_size=batch_size,
        date_col=date_col,
        value_col=value_col,
    ):
        chunk_count += 1
        prepared, history = prepare_frequency_chunk(raw_chunk, history)
        if prepared.empty:
            continue

        features = build_features(prepared)
        target = prepared["Value"].astype(float)
        mask = features.notna().all(axis=1) & target.notna()
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
            if not models:
                fit_one_shard()
            if not models:
                raise ValueError("No CUDA shard models were trained before test segment.")

            x_test = x_chunk[train_take:]
            y_test = y_chunk[train_take:]
            ensemble = WeightedAverageRegressor(models=models, weights=model_weights, gpu_ids=model_gpu_ids)
            y_pred = ensemble.predict(pd.DataFrame(x_test, columns=feature_cols))

            err = y_pred - y_test
            abs_err = np.abs(err)
            sq_err = err * err

            test_rows += len(y_test)
            sum_abs += float(abs_err.sum())
            sum_sq += float(sq_err.sum())
            sum_y += float(y_test.sum())
            sum_y2 += float((y_test * y_test).sum())

        seen_rows += chunk_rows

        if chunk_count == 1 or (progress_every_chunks > 0 and chunk_count % progress_every_chunks == 0):
            elapsed = time.monotonic() - started
            pct = (seen_rows / total_rows * 100.0) if total_rows > 0 else 0.0
            #print(
                #f"[progress] chunks={chunk_count:,} rows_seen={seen_rows:,}/{total_rows:,} ({pct:.1f}%) "
                #f"train_rows={trained_rows:,} test_rows={test_rows:,} models={len(models):,} elapsed={elapsed:.1f}s"
            #)

    fit_one_shard()
    if not models:
        raise ValueError("No CUDA shard models were trained.")
    if test_rows <= 0:
        raise ValueError("No test rows were produced during CUDA-sharded training.")

    metrics = build_metrics_from_sums(test_rows, sum_abs, sum_sq, sum_y, sum_y2)
    metrics["data"] = {
        "rows_total": int(trained_rows + test_rows),
        "rows_train": int(trained_rows),
        "rows_test": int(test_rows),
        "train_fraction": float(train_fraction),
        "backend": "cuda-sharded",
        "models": int(len(models)),
        "gpus_used": [gpu for gpu in gpu_cycle if gpu is not None],
        "shard_rows": int(effective_shard_rows),
        "target_vram_gb": float(target_vram_gb),
    }

    elapsed = time.monotonic() - started
    print(
        f"[done] CUDA-sharded training finished in {elapsed:.1f}s; "
        f"models={len(models):,}, train_rows={trained_rows:,}, test_rows={test_rows:,}"
    )

    ensemble = WeightedAverageRegressor(models=models, weights=model_weights, gpu_ids=model_gpu_ids)
    return ensemble, metrics, (feature_cols or [])


def train_incremental_frequency(
    parquet_dir: Path,
    batch_size: int,
    train_fraction: float,
    random_state: int,
    progress_every_chunks: int,
) -> tuple[SGDRegressor, dict, list[str]]:
    dataset, date_col, value_col = frequency_dataset_and_columns(parquet_dir)
    total_rows = int(dataset.count_rows())
    if total_rows < 100:
        raise ValueError("Not enough rows in dataset to train (need at least 100).")

    train_row_cutoff = max(1, int(total_rows * train_fraction))
    print(
        f"[info] Incremental dataset rows={total_rows:,}, "
        f"train cutoff={train_row_cutoff:,}, expected test rows~{max(0, total_rows - train_row_cutoff):,}"
    )

    model = SGDRegressor(
        loss="huber",
        penalty="l2",
        alpha=1e-5,
        random_state=random_state,
        learning_rate="invscaling",
    )

    history = pd.DataFrame(columns=["Date", "Value"])
    seen_rows = 0
    trained_rows = 0
    test_rows = 0
    sum_abs = 0.0
    sum_sq = 0.0
    sum_y = 0.0
    sum_y2 = 0.0
    first_fit_done = False
    feature_cols: list[str] | None = None
    chunk_count = 0
    started = time.monotonic()

    for raw_chunk in iter_frequency_chunks(
        parquet_dir,
        batch_size=batch_size,
        date_col=date_col,
        value_col=value_col,
    ):
        chunk_count += 1
        prepared, history = prepare_frequency_chunk(raw_chunk, history)
        if prepared.empty:
            continue

        features = build_features(prepared)
        target = prepared["Value"].astype(float)
        mask = features.notna().all(axis=1) & target.notna()
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
            model.partial_fit(x_train, y_train)
            first_fit_done = True
            trained_rows += train_take

        if test_take > 0 and first_fit_done:
            x_test = x_chunk[train_take:]
            y_test = y_chunk[train_take:]
            y_pred = model.predict(x_test)
            err = y_pred - y_test
            abs_err = np.abs(err)
            sq_err = err * err

            test_rows += len(y_test)
            sum_abs += float(abs_err.sum())
            sum_sq += float(sq_err.sum())
            sum_y += float(y_test.sum())
            sum_y2 += float((y_test * y_test).sum())

        seen_rows += chunk_rows

        if chunk_count == 1 or (progress_every_chunks > 0 and chunk_count % progress_every_chunks == 0):
            elapsed = time.monotonic() - started
            pct = (seen_rows / total_rows * 100.0) if total_rows > 0 else 0.0
            #print(
                #f"[progress] chunks={chunk_count:,} rows_seen={seen_rows:,}/{total_rows:,} ({pct:.1f}%) "
                #f"train_rows={trained_rows:,} test_rows={test_rows:,} elapsed={elapsed:.1f}s"
            #)

    if not first_fit_done or trained_rows < 50:
        raise ValueError("Not enough training rows after preprocessing for incremental training.")

    metrics = build_metrics_from_sums(test_rows, sum_abs, sum_sq, sum_y, sum_y2)
    metrics["data"] = {
        "rows_total": int(trained_rows + test_rows),
        "rows_train": int(trained_rows),
        "rows_test": int(test_rows),
        "train_fraction": float(train_fraction),
        "backend": "cpu-incremental",
    }

    elapsed = time.monotonic() - started
    print(
        f"[done] Incremental training finished in {elapsed:.1f}s; "
        f"chunks={chunk_count:,}, train_rows={trained_rows:,}, test_rows={test_rows:,}"
    )

    return model, metrics, (feature_cols or [])


def main() -> int:
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(description="Train Frequency expert model from parquet")
    parser.add_argument("--parquet-dir", default=str(project_root() / "DataSources" / "NESO" / "Frequency" / "Parquet"))
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
        choices=["auto", "incremental", "batch", "cuda-sharded"],
        default="auto",
        help="auto chooses best path (cuda-sharded for very large CUDA datasets).",
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
        help="Print incremental progress every N parquet chunks (set 0 to disable periodic progress).",
    )
    parser.add_argument(
        "--cuda-shard-rows",
        type=int,
        default=0,
        help="Rows per CUDA shard model in cuda-sharded mode (0 = auto from --cuda-target-vram-gb).",
    )
    parser.add_argument(
        "--cuda-target-vram-gb",
        type=float,
        default=0.0,
        help="VRAM target per shard model in GB; <=0 enables auto target from GPU memory.",
    )
    parser.add_argument(
        "--cuda-target-vram-fraction",
        type=float,
        default=0.7,
        help="When auto-targeting VRAM, use this fraction of smallest selected GPU memory.",
    )
    parser.add_argument(
        "--cuda-target-vram-cap-gb",
        type=float,
        default=8.0,
        help="When auto-targeting VRAM, cap target at this GB amount.",
    )
    parser.add_argument(
        "--cuda-gpus",
        type=str,
        default="auto",
        help="Comma-separated GPU IDs, 'all', or 'auto' for CUDA-sharded mode.",
    )
    parser.add_argument(
        "--cuda-allow-multi-gpu-switch",
        action="store_true",
        help=(
            "Allow switching active CUDA device per shard model in one Python process. "
            "Disabled by default because some RAPIDS/cuML stacks crash with raft::cuda_error."
        ),
    )
    parser.add_argument(
        "--cuda-batch-max-rows",
        type=int,
        default=5_000_000,
        help="In auto mode, use batch CUDA only when dataset rows are <= this threshold.",
    )
    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not parquet_dir.exists() or not list_parquet_files(parquet_dir):
        print(f"[info] Frequency parquet not ready: {parquet_dir}")
        return 0

    dataset, date_col, value_col = frequency_dataset_and_columns(parquet_dir)
    print(
        f"[info] Frequency parquet schema resolved: date_col='{date_col}', value_col='{value_col}', "
        f"rows={int(dataset.count_rows()):,}"
    )

    target_cols = ["Value"]
    requested_mode = args.training_mode
    effective_mode = requested_mode

    gpu_ids = parse_gpu_ids(args.cuda_gpus)
    has_cuda_stack = False
    try:
        has_cuda_stack = resolve_backend("auto") == "cuda"
    except Exception:
        has_cuda_stack = False

    total_rows = int(dataset.count_rows())
    if requested_mode == "auto":
        if args.device == "cpu":
            effective_mode = "incremental"
        elif has_cuda_stack and total_rows > args.cuda_batch_max_rows:
            effective_mode = "cuda-sharded"
        elif has_cuda_stack:
            effective_mode = "batch"
        else:
            effective_mode = "incremental"
        print(f"[info] Auto mode selected '{effective_mode}'")

    if effective_mode == "incremental":
        print(
            "[info] Incremental mode uses CPU by design (streamed shard training via SGDRegressor.partial_fit). "
            "Current RAPIDS/cuML random-forest flow in this project is batch/in-memory, not streamed partial-fit. "
            "Use --training-mode batch for GPU random-forest training."
        )
        if args.device == "cuda":
            print("[warn] --device cuda ignored in incremental mode.")
        backend = "cpu-incremental"
        print(f"[load] Streaming frequency parquet dataset from: {parquet_dir}")
        #print(
            #f"[train] Incremental shard training with batch-size={args.batch_size:,}, "
            #f"progress-every-chunks={args.progress_every_chunks}"
        #)
        try:
            model, metrics, feature_cols = train_incremental_frequency(
                parquet_dir=parquet_dir,
                batch_size=args.batch_size,
                train_fraction=args.train_fraction,
                random_state=args.random_state,
                progress_every_chunks=args.progress_every_chunks,
            )
        except ValueError as exc:
            print(f"[error] {exc}")
            return 1
    elif effective_mode == "cuda-sharded":
        try:
            backend = resolve_backend(args.device)
        except RuntimeError as exc:
            print(f"[error] {exc}")
            print("[hint] Install RAPIDS (cuDF/cuML) or use --training-mode incremental")
            return 1

        if backend != "cuda":
            print("[warn] CUDA-sharded requested but CUDA backend unavailable. Falling back to incremental CPU.")
            backend = "cpu-incremental"
            model, metrics, feature_cols = train_incremental_frequency(
                parquet_dir=parquet_dir,
                batch_size=args.batch_size,
                train_fraction=args.train_fraction,
                random_state=args.random_state,
                progress_every_chunks=args.progress_every_chunks,
            )
        else:
            if not gpu_ids:
                gpu_ids = parse_gpu_ids("auto")

            effective_target_vram_gb = args.cuda_target_vram_gb
            if effective_target_vram_gb <= 0:
                mem_map = detect_gpu_memory_gb_map()
                selected_mems = [mem_map[gpu] for gpu in gpu_ids if gpu in mem_map]
                if selected_mems:
                    smallest = min(selected_mems)
                    frac = max(0.1, min(0.95, args.cuda_target_vram_fraction))
                    cap = max(1.0, args.cuda_target_vram_cap_gb)
                    effective_target_vram_gb = min(cap, smallest * frac)
                else:
                    effective_target_vram_gb = max(1.0, args.cuda_target_vram_cap_gb)

            print(
                f"[load] Streaming frequency parquet dataset from: {parquet_dir}\n"
                f"[train] CUDA-sharded training with shard-rows={args.cuda_shard_rows:,}, "
                f"batch-size={args.batch_size:,}, target-vram-gb={effective_target_vram_gb:.2f}, "
                f"gpus={gpu_ids if gpu_ids else '[default]'}"
            )
            try:
                model, metrics, feature_cols = train_cuda_sharded_frequency(
                    parquet_dir=parquet_dir,
                    batch_size=args.batch_size,
                    train_fraction=args.train_fraction,
                    random_state=args.random_state,
                    n_estimators=args.n_estimators,
                    shard_rows=args.cuda_shard_rows,
                    target_vram_gb=effective_target_vram_gb,
                    gpu_ids=gpu_ids,
                    allow_multi_gpu_switch=args.cuda_allow_multi_gpu_switch,
                    progress_every_chunks=args.progress_every_chunks,
                )
                backend = "cuda-sharded"
            except ValueError as exc:
                print(f"[error] {exc}")
                return 1
    else:
        try:
            backend = resolve_backend(args.device)
        except RuntimeError as exc:
            print(f"[error] {exc}")
            print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
            return 1

        print(f"[load] Reading frequency parquet dataset from: {parquet_dir}")
        effective_max_rows = args.max_rows
        if requested_mode == "auto" and effective_mode == "batch" and backend == "cuda" and args.max_rows == 0:
            effective_max_rows = min(total_rows, args.cuda_batch_max_rows)
            print(
                f"[info] Auto-batch CUDA cap applied: max_rows={effective_max_rows:,} "
                f"(override with --max-rows)"
            )

        df = load_frequency_parquet(parquet_dir, max_rows=effective_max_rows, batch_size=args.batch_size)
        if df.empty:
            print("[error] Loaded dataframe is empty.")
            return 1

        df = ensure_datetime(df)
        features = build_features(df)
        target = df[target_cols].copy()

        print(f"[train] Training frequency expert with {len(df):,} rows on backend: {backend}")
        model, metrics, feature_cols = train_multioutput(
            features=features,
            target=target,
            target_cols=target_cols,
            n_estimators=args.n_estimators,
            random_state=args.random_state,
            train_fraction=args.train_fraction,
            backend=backend,
        )

    model_path = output_dir / "frequency_expert_model.joblib"
    metrics_path = output_dir / "frequency_expert_metrics.json"

    performance = build_performance_metrics(metrics, started_at=started_at)
    weights = summarize_feature_weights(model, feature_cols, target_cols)
    metrics["performance"] = performance
    metrics["weights"] = weights
    print_training_summary("Frequency expert", backend, metrics, performance, weights)

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=target_cols,
        feature_columns=feature_cols,
        metadata={
            "dataset": "Frequency",
            "parquet_dir": str(parquet_dir),
            "n_estimators": args.n_estimators,
            "train_fraction": args.train_fraction,
            "random_state": args.random_state,
            "backend": backend,
            "device_arg": args.device,
            "max_rows": args.max_rows,
            "batch_size": args.batch_size,
            "training_mode": requested_mode,
            "effective_training_mode": effective_mode,
            "progress_every_chunks": args.progress_every_chunks,
            "cuda_shard_rows": args.cuda_shard_rows,
            "cuda_target_vram_gb": args.cuda_target_vram_gb,
            "cuda_gpus": args.cuda_gpus,
            "cuda_allow_multi_gpu_switch": bool(args.cuda_allow_multi_gpu_switch),
            "cuda_isolated_multi_gpu_default": True,
            "cuda_batch_max_rows": args.cuda_batch_max_rows,
        },
        metrics=metrics,
    )

    print(f"[done] Model saved: {model_path}")
    print(f"[done] Metrics saved: {metrics_path}")
    print(
        "[done] Overall test metrics -> "
        f"RMSE(mean): {metrics['overall']['rmse_mean']:.4f}, "
        f"MAE(mean): {metrics['overall']['mae_mean']:.4f}, "
        f"R2(mean): {metrics['overall']['r2_mean']:.4f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
