#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.types as pat
from sklearn.linear_model import SGDRegressor

from common_trainer import (
    build_performance_metrics,
    build_calendar_time_features,
    print_training_summary,
    resolve_backend,
    save_artifacts,
    select_numeric_targets,
    summarize_feature_weights,
    train_multioutput,
)


@dataclass(frozen=True)
class DatasetSpec:
    slug: str
    label: str
    parquet_dir: Path
    timestamp_candidates: tuple[str, ...]
    settlement_col: str | None = None
    drop_cols: tuple[str, ...] = tuple()


def project_root() -> Path:
    return next(path for path in Path(__file__).resolve().parents if path.name == "GDA")


def list_parquet_files(parquet_dir: Path) -> list[Path]:
    return [path for path in parquet_dir.rglob("*.parquet") if path.is_file()]


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

    df = pd.concat(list(chunks), ignore_index=True)
    print(f"[load] Scanned rows: {scanned_rows:,}; using rows in memory: {len(df):,}")
    return df


def iter_parquet_batches(parquet_dir: Path, batch_size: int, max_rows: int = 0):
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    scanner = dataset.scanner(batch_size=batch_size)
    emitted = 0

    for batch in scanner.to_batches():
        chunk = batch.to_pandas()
        if chunk.empty:
            continue

        if max_rows > 0 and emitted >= max_rows:
            break

        if max_rows > 0 and emitted + len(chunk) > max_rows:
            keep = max_rows - emitted
            if keep <= 0:
                break
            chunk = chunk.iloc[:keep].copy()

        emitted += len(chunk)
        yield chunk


def select_numeric_targets_from_schema(
    parquet_dir: Path,
    exclude: set[str],
    max_targets: int,
) -> list[str]:
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    candidates: list[str] = []

    for field in dataset.schema:
        name = field.name
        if name in exclude:
            continue
        if pat.is_integer(field.type) or pat.is_floating(field.type) or pat.is_decimal(field.type):
            candidates.append(name)

    return candidates[:max_targets]


def prepare_chunk_frames(
    chunk: pd.DataFrame,
    timestamp_col: str,
    settlement_col: str | None,
    target_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = chunk.copy()
    out["timestamp"] = pd.to_datetime(out[timestamp_col], utc=True, errors="coerce")

    if settlement_col and settlement_col in out.columns:
        period = pd.to_numeric(out[settlement_col], errors="coerce")
        out["timestamp"] = out["timestamp"] + pd.to_timedelta((period - 1) * 30, unit="m")

    out = out.dropna(subset=["timestamp"]).sort_values("timestamp")
    if out.empty:
        return pd.DataFrame(), pd.DataFrame()

    features = build_calendar_time_features(out["timestamp"])
    if settlement_col and settlement_col in out.columns:
        features["settlement_period"] = pd.to_numeric(out[settlement_col], errors="coerce")

    target_df = pd.DataFrame(index=out.index)
    for col in target_cols:
        if col in out.columns:
            target_df[col] = pd.to_numeric(out[col], errors="coerce")
        else:
            target_df[col] = np.nan

    features = features.replace([float("inf"), float("-inf")], pd.NA).ffill().bfill().fillna(0.0)
    target_df = target_df.replace([float("inf"), float("-inf")], pd.NA)

    feature_mask = features.notna().all(axis=1)
    return features.loc[feature_mask], target_df.loc[feature_mask]


def pick_incremental_targets(
    sample_df: pd.DataFrame,
    exclude: set[str],
    min_target_non_null: float,
    max_targets: int,
    minimum_joint_rows: int = 100,
) -> list[str]:
    targets = select_numeric_targets(
        sample_df,
        exclude=exclude,
        min_non_null_ratio=min_target_non_null,
        max_targets=max_targets,
    )
    if not targets:
        return []

    while len(targets) > 1:
        y = pd.DataFrame({col: pd.to_numeric(sample_df[col], errors="coerce") for col in targets})
        joint_rows = int(y.notna().all(axis=1).sum())
        if joint_rows >= minimum_joint_rows:
            return targets

        ratios = {col: float(pd.to_numeric(sample_df[col], errors="coerce").notna().mean()) for col in targets}
        drop_col = min(ratios, key=ratios.get)
        targets = [col for col in targets if col != drop_col]

    return targets


def init_metric_state(n_targets: int) -> dict:
    return {
        "count": np.zeros(n_targets, dtype=np.float64),
        "sum_y": np.zeros(n_targets, dtype=np.float64),
        "sum_y2": np.zeros(n_targets, dtype=np.float64),
        "sum_abs_err": np.zeros(n_targets, dtype=np.float64),
        "sum_sq_err": np.zeros(n_targets, dtype=np.float64),
    }


def update_metric_state(state: dict, y_true: np.ndarray, y_pred: np.ndarray) -> None:
    err = y_true - y_pred
    state["count"] += y_true.shape[0]
    state["sum_y"] += np.sum(y_true, axis=0)
    state["sum_y2"] += np.sum(np.square(y_true), axis=0)
    state["sum_abs_err"] += np.sum(np.abs(err), axis=0)
    state["sum_sq_err"] += np.sum(np.square(err), axis=0)


def finalize_metrics(state: dict, target_cols: list[str]) -> dict:
    per_target = {}
    rmse_list: list[float] = []
    mae_list: list[float] = []
    r2_list: list[float] = []

    for idx, col in enumerate(target_cols):
        n = max(1.0, float(state["count"][idx]))
        rmse = float(np.sqrt(state["sum_sq_err"][idx] / n))
        mae = float(state["sum_abs_err"][idx] / n)
        sst = float(state["sum_y2"][idx] - (state["sum_y"][idx] ** 2) / n)
        if sst > 0:
            r2 = float(1.0 - (state["sum_sq_err"][idx] / sst))
        else:
            r2 = 0.0

        rmse_list.append(rmse)
        mae_list.append(mae)
        r2_list.append(r2)
        per_target[col] = {"rmse": rmse, "mae": mae, "r2": r2}

    return {
        "overall": {
            "rmse_mean": float(np.mean(rmse_list)) if rmse_list else 0.0,
            "mae_mean": float(np.mean(mae_list)) if mae_list else 0.0,
            "r2_mean": float(np.mean(r2_list)) if r2_list else 0.0,
        },
        "per_target": per_target,
    }


def train_one_dataset_incremental(
    spec: DatasetSpec,
    n_estimators: int,
    train_fraction: float,
    random_state: int,
    min_target_non_null: float,
    max_targets: int,
    max_rows_per_dataset: int,
    batch_size: int,
    progress_every_chunks: int,
) -> tuple[object, dict, list[str], list[str]]:
    del n_estimators

    dataset = ds.dataset(str(spec.parquet_dir), format="parquet", partitioning="hive")
    total_rows_raw = int(dataset.count_rows())
    total_rows_limit = min(total_rows_raw, max_rows_per_dataset) if max_rows_per_dataset > 0 else total_rows_raw
    if total_rows_limit < 100:
        raise ValueError("Not enough rows available for incremental training")

    first_chunk = None
    batch_iter = iter_parquet_batches(spec.parquet_dir, batch_size=batch_size, max_rows=max_rows_per_dataset)
    for chunk in batch_iter:
        if not chunk.empty:
            first_chunk = chunk
            break
    if first_chunk is None:
        raise ValueError("No readable parquet batches")

    timestamp_col = find_timestamp_column(first_chunk, spec.timestamp_candidates)
    if timestamp_col is None:
        raise ValueError("No timestamp column found")

    exclude = {
        "timestamp",
        timestamp_col,
        "year",
        *(spec.drop_cols or tuple()),
    }
    if spec.settlement_col:
        exclude.add(spec.settlement_col)

    profile_chunks = [first_chunk]
    for chunk in batch_iter:
        if chunk.empty:
            continue
        profile_chunks.append(chunk)
        if len(profile_chunks) >= 3:
            break

    sample_df = pd.concat(profile_chunks, ignore_index=True)
    target_cols = pick_incremental_targets(
        sample_df=sample_df,
        exclude=exclude,
        min_target_non_null=min_target_non_null,
        max_targets=max_targets,
    )
    if not target_cols:
        target_cols = select_numeric_targets_from_schema(
            spec.parquet_dir,
            exclude=exclude,
            max_targets=max_targets,
        )
    if not target_cols:
        raise ValueError("No numeric targets found for incremental training")

    print(f"[info] {spec.label}: incremental targets ({len(target_cols)}): {target_cols}")

    clean_rows_total = 0
    for chunk in iter_parquet_batches(spec.parquet_dir, batch_size=batch_size, max_rows=max_rows_per_dataset):
        features, target = prepare_chunk_frames(
            chunk=chunk,
            timestamp_col=timestamp_col,
            settlement_col=spec.settlement_col,
            target_cols=target_cols,
        )
        if not features.empty:
            clean_rows_total += len(features)

    if clean_rows_total < 100:
        raise ValueError("No trainable rows found in incremental path")

    split_idx = max(1, int(clean_rows_total * train_fraction))
    split_idx = min(split_idx, clean_rows_total - 1)

    models = {
        col: SGDRegressor(
            loss="squared_error",
            penalty="l2",
            alpha=1e-4,
            random_state=random_state,
            max_iter=1,
            tol=None,
        )
        for col in target_cols
    }

    trained_targets: set[str] = set()
    rows_seen = 0
    rows_train = 0
    rows_test = 0
    chunks_seen = 0
    feature_cols: list[str] = []
    metric_state = init_metric_state(len(target_cols))

    for chunk in iter_parquet_batches(spec.parquet_dir, batch_size=batch_size, max_rows=max_rows_per_dataset):
        features, target = prepare_chunk_frames(
            chunk=chunk,
            timestamp_col=timestamp_col,
            settlement_col=spec.settlement_col,
            target_cols=target_cols,
        )
        if features.empty:
            continue

        if not feature_cols:
            feature_cols = list(features.columns)

        x = features.to_numpy(dtype=np.float32)

        n_rows = len(features)
        global_idx = rows_seen + np.arange(n_rows)
        train_global_mask = global_idx < split_idx
        test_global_mask = ~train_global_mask

        rows_train += int(np.sum(train_global_mask))
        rows_test += int(np.sum(test_global_mask))

        for idx, col in enumerate(target_cols):
            y_col = target[col].to_numpy(dtype=np.float64)
            valid_mask = ~np.isnan(y_col)
            if not np.any(valid_mask):
                continue

            fit_mask = train_global_mask & valid_mask
            if np.any(fit_mask):
                models[col].partial_fit(x[fit_mask], y_col[fit_mask].astype(np.float32))
                trained_targets.add(col)

            pred_mask = test_global_mask & valid_mask
            if col in trained_targets and np.any(pred_mask):
                y_true = y_col[pred_mask]
                y_pred = models[col].predict(x[pred_mask]).astype(np.float64)
                err = y_true - y_pred
                metric_state["count"][idx] += len(y_true)
                metric_state["sum_y"][idx] += float(np.sum(y_true))
                metric_state["sum_y2"][idx] += float(np.sum(np.square(y_true)))
                metric_state["sum_abs_err"][idx] += float(np.sum(np.abs(err)))
                metric_state["sum_sq_err"][idx] += float(np.sum(np.square(err)))

        rows_seen += n_rows
        chunks_seen += 1
        if progress_every_chunks > 0 and chunks_seen % progress_every_chunks == 0:
            pct = (rows_seen / clean_rows_total * 100.0) if clean_rows_total > 0 else 0.0
            print(
                f"[progress] {spec.label}: chunks={chunks_seen} rows={rows_seen:,}/{clean_rows_total:,} "
                f"({pct:.1f}%) train={rows_train:,} test={rows_test:,}"
            )

        if rows_seen >= clean_rows_total:
            break

    if not trained_targets:
        raise ValueError("No trainable rows found in incremental path")

    metrics = finalize_metrics(metric_state, target_cols)
    metrics["data"] = {
        "rows_total": int(rows_train + rows_test),
        "rows_train": int(rows_train),
        "rows_test": int(rows_test),
        "train_fraction": float(train_fraction),
        "backend": "cpu-incremental",
    }

    model_bundle = {"backend": "cpu-incremental", "models": models}
    return model_bundle, metrics, feature_cols, target_cols


def find_timestamp_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    for col in df.columns:
        low = str(col).lower()
        if "date" in low or "time" in low:
            return col
    return None


def prepare_frames(
    df: pd.DataFrame,
    timestamp_col: str,
    settlement_col: str | None,
    drop_cols: tuple[str, ...],
    min_target_non_null: float,
    max_targets: int,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out[timestamp_col], utc=True, errors="coerce")

    if settlement_col and settlement_col in out.columns:
        period = pd.to_numeric(out[settlement_col], errors="coerce")
        out["timestamp"] = out["timestamp"] + pd.to_timedelta((period - 1) * 30, unit="m")

    out = out.dropna(subset=["timestamp"]).sort_values("timestamp")
    features = build_calendar_time_features(out["timestamp"]) 

    if settlement_col and settlement_col in out.columns:
        features["settlement_period"] = pd.to_numeric(out[settlement_col], errors="coerce")

    exclude = {
        "timestamp",
        timestamp_col,
        "year",
        *(drop_cols or tuple()),
    }
    if settlement_col:
        exclude.add(settlement_col)

    targets = select_numeric_targets(
        out,
        exclude=exclude,
        min_non_null_ratio=min_target_non_null,
        max_targets=max_targets,
    )
    if not targets:
        raise ValueError("No numeric targets met selection criteria")

    target_df = pd.DataFrame(index=out.index)
    for col in targets:
        target_df[col] = pd.to_numeric(out[col], errors="coerce")

    features = features.replace([float("inf"), float("-inf")], pd.NA).ffill().bfill().fillna(0.0)
    target_df = target_df.replace([float("inf"), float("-inf")], pd.NA)

    return features, target_df, targets


def train_one_dataset(
    spec: DatasetSpec,
    output_dir: Path,
    n_estimators: int,
    train_fraction: float,
    random_state: int,
    backend: str,
    training_mode: str,
    incremental_threshold_rows: int,
    progress_every_chunks: int,
    min_target_non_null: float,
    max_targets: int,
    max_rows_per_dataset: int,
    batch_size: int,
) -> int:
    started_at = time.monotonic()
    if not spec.parquet_dir.exists() or not list_parquet_files(spec.parquet_dir):
        print(f"[skip] {spec.label}: parquet not ready at {spec.parquet_dir}")
        return 0

    dataset = ds.dataset(str(spec.parquet_dir), format="parquet", partitioning="hive")
    dataset_rows = int(dataset.count_rows())
    use_incremental = (
        training_mode == "incremental"
        or (
            training_mode == "auto"
            and max_rows_per_dataset == 0
            and dataset_rows >= incremental_threshold_rows
        )
    )

    if use_incremental:
        if backend == "cuda":
            print(
                f"[info] {spec.label}: using CPU incremental streaming for {dataset_rows:,} rows "
                "(current generic multi-target GPU path is batch/in-memory)."
            )
        print(f"[train] {spec.label}: mode=incremental rows~{dataset_rows:,}")
        try:
            model, metrics, feature_cols, target_cols = train_one_dataset_incremental(
                spec=spec,
                n_estimators=n_estimators,
                train_fraction=train_fraction,
                random_state=random_state,
                min_target_non_null=min_target_non_null,
                max_targets=max_targets,
                max_rows_per_dataset=max_rows_per_dataset,
                batch_size=batch_size,
                progress_every_chunks=progress_every_chunks,
            )
        except ValueError as exc:
            print(f"[skip] {spec.label}: {exc}")
            return 0
        summary_backend = "cpu-incremental"
    else:
        print(f"[load] {spec.label}: {spec.parquet_dir}")
        df = load_parquet_frame(
            spec.parquet_dir,
            max_rows=max_rows_per_dataset,
            batch_size=batch_size,
        )
        if df.empty:
            print(f"[skip] {spec.label}: dataset is empty")
            return 0

        timestamp_col = find_timestamp_column(df, spec.timestamp_candidates)
        if timestamp_col is None:
            print(f"[skip] {spec.label}: no timestamp column found")
            return 0

        try:
            features, target, target_cols = prepare_frames(
                df=df,
                timestamp_col=timestamp_col,
                settlement_col=spec.settlement_col,
                drop_cols=spec.drop_cols,
                min_target_non_null=min_target_non_null,
                max_targets=max_targets,
            )
        except ValueError as exc:
            print(f"[skip] {spec.label}: {exc}")
            return 0

        print(
            f"[train] {spec.label}: rows={len(features):,}, targets={len(target_cols)}, backend={backend}"
        )
        model, metrics, feature_cols = train_multioutput(
            features=features,
            target=target,
            target_cols=target_cols,
            n_estimators=n_estimators,
            random_state=random_state,
            train_fraction=train_fraction,
            backend=backend,
        )
        summary_backend = backend

    performance = build_performance_metrics(metrics, started_at=started_at)
    weights = summarize_feature_weights(model, feature_cols, target_cols)
    metrics["performance"] = performance
    metrics["weights"] = weights
    print_training_summary(spec.label, summary_backend, metrics, performance, weights)

    model_path = output_dir / f"{spec.slug}_expert_model.joblib"
    metrics_path = output_dir / f"{spec.slug}_expert_metrics.json"

    save_artifacts(
        model=model,
        model_path=model_path,
        metrics_path=metrics_path,
        target_columns=target_cols,
        feature_columns=feature_cols,
        metadata={
            "dataset": spec.label,
            "parquet_dir": str(spec.parquet_dir),
            "n_estimators": n_estimators,
            "train_fraction": train_fraction,
            "random_state": random_state,
            "backend": summary_backend,
            "training_mode": training_mode,
        },
        metrics=metrics,
    )

    print(
        f"[done] {spec.label}: model={model_path.name}, "
        f"R2(mean)={metrics['overall']['r2_mean']:.3f}, "
        f"RMSE(mean)={metrics['overall']['rmse_mean']:.3f}"
    )
    return 1


def main() -> int:
    root = project_root()

    parser = argparse.ArgumentParser(
        description="Train expert models for parquet datasets not covered by the base expert set"
    )
    parser.add_argument(
        "--output-dir",
        default=str(root / "MachineLearning" / "experts" / "pre-trained-experts"),
    )
    parser.add_argument("--n-estimators", type=int, default=350)
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    parser.add_argument("--min-target-non-null", type=float, default=0.35)
    parser.add_argument("--max-targets", type=int, default=20)
    parser.add_argument("--training-mode", choices=["auto", "batch", "incremental"], default="auto")
    parser.add_argument("--incremental-threshold-rows", type=int, default=1_000_000)
    parser.add_argument("--max-rows-per-dataset", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--progress-every-chunks", type=int, default=25)
    args = parser.parse_args()

    try:
        backend = resolve_backend(args.device)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        print("[hint] Install RAPIDS (cuDF/cuML) or run with --device cpu")
        return 1

    specs = [
        DatasetSpec(
            slug="bsad_aggregated",
            label="BSAD_AggregatedData",
            parquet_dir=(root / "DataSources" / "NESO" / "BSAD_AggregatedData" / "Parquet").resolve(),
            timestamp_candidates=("DatetimeUTC", "Date"),
            settlement_col="Settlement Period",
        ),
        DatasetSpec(
            slug="bsad_forwardcontracts",
            label="BSAD_ForwardContracts",
            parquet_dir=(root / "DataSources" / "NESO" / "BSAD_ForwardContracts" / "Parquet").resolve(),
            timestamp_candidates=("DatetimeUTC", "Date"),
            settlement_col="Settlement Period",
        ),
        DatasetSpec(
            slug="carbonintensity_balancingactions",
            label="CarbonIntensityOfBalancingActions",
            parquet_dir=(root / "DataSources" / "NESO" / "CarbonIntensityOfBalancingActions" / "Parquet").resolve(),
            timestamp_candidates=("DATETIME", "DatetimeUTC", "Date"),
        ),
        DatasetSpec(
            slug="eac_enduringauctioncapability",
            label="EACEnduringAuctionCapability",
            parquet_dir=(root / "DataSources" / "NESO" / "EACEnduringAuctionCapability" / "Parquet").resolve(),
            timestamp_candidates=("deliveryStart", "Date", "DatetimeUTC"),
            drop_cols=("auctionID", "orderID", "deliveryStart", "deliveryEnd", "orderEntryTime"),
        ),
        DatasetSpec(
            slug="ecbr_auctionresults",
            label="EC-BR_AuctionResults",
            parquet_dir=(root / "DataSources" / "NESO" / "EC-BR_AuctionResults" / "Parquet").resolve(),
            timestamp_candidates=("deliveryStart", "Date", "DatetimeUTC"),
            drop_cols=("auctionID", "deliveryStart", "deliveryEnd"),
        ),
    ]

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    trained = 0
    for spec in specs:
        try:
            trained += train_one_dataset(
                spec=spec,
                output_dir=output_dir,
                n_estimators=args.n_estimators,
                train_fraction=args.train_fraction,
                random_state=args.random_state,
                backend=backend,
                training_mode=args.training_mode,
                incremental_threshold_rows=args.incremental_threshold_rows,
                progress_every_chunks=args.progress_every_chunks,
                min_target_non_null=args.min_target_non_null,
                max_targets=args.max_targets,
                max_rows_per_dataset=args.max_rows_per_dataset,
                batch_size=args.batch_size,
            )
        except Exception as exc:
            print(f"[warn] {spec.label}: training failed ({exc})")

    print(f"[done] Additional parquet experts trained: {trained}/{len(specs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
