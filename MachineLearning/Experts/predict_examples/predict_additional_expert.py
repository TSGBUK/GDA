#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd


def load_input(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input format: {path}")


def build_features_from_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        timestamp = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    elif "Date" in df.columns:
        timestamp = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    else:
        timestamp = pd.to_datetime(df.iloc[:, 0], utc=True, errors="coerce")

    features = pd.DataFrame(index=df.index)
    features["year"] = timestamp.dt.year
    features["month"] = timestamp.dt.month
    features["day"] = timestamp.dt.day
    features["hour"] = timestamp.dt.hour
    features["dayofweek"] = timestamp.dt.dayofweek
    features["is_weekend"] = (timestamp.dt.dayofweek >= 5).astype(float)

    if "settlement_period" in df.columns:
        features["settlement_period"] = pd.to_numeric(df["settlement_period"], errors="coerce")
    elif "SETTLEMENT_PERIOD" in df.columns:
        features["settlement_period"] = pd.to_numeric(df["SETTLEMENT_PERIOD"], errors="coerce")

    return features.replace([float("inf"), float("-inf")], pd.NA).ffill().bfill().fillna(0.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict with additional parquet expert model")
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    payload = joblib.load(args.model_path)
    model = payload["model"]
    feature_columns = payload["feature_columns"]
    target_columns = payload["target_columns"]

    raw = load_input(Path(args.input_path))
    features = build_features_from_timestamp(raw)

    X = features.reindex(columns=feature_columns).fillna(0.0)
    pred = model.predict(X)

    pred_df = pd.DataFrame(pred, columns=target_columns)
    pred_df.to_csv(args.output_path, index=False)


if __name__ == "__main__":
    main()
