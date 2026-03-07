#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import joblib
import pandas as pd


def _add_parent_to_path() -> None:
    here = Path(__file__).resolve().parent
    parent = here.parent
    if str(parent) not in sys.path:
        sys.path.insert(0, str(parent))


def load_input(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input format: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict with weather-generation or weather-inertia expert")
    parser.add_argument("--expert", required=True, choices=["weather_generation", "weather_inertia"])
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--weather-input-path", required=True)
    parser.add_argument("--target-input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--max-gap-minutes", type=int, default=60)
    args = parser.parse_args()

    _add_parent_to_path()

    if args.expert == "weather_generation":
        import train_weather_generation_expert as composite

        merge_fn = composite.merge_weather_generation
    else:
        import train_weather_inertia_expert as composite

        merge_fn = composite.merge_weather_inertia

    payload = joblib.load(args.model_path)
    model = payload["model"]
    feature_columns = payload["feature_columns"]
    target_columns = payload["target_columns"]

    weather_df = load_input(Path(args.weather_input_path))
    target_df = load_input(Path(args.target_input_path))

    merged = merge_fn(weather_df, target_df, max_gap_minutes=args.max_gap_minutes)
    features = composite.build_features(merged)
    X = features.reindex(columns=feature_columns).replace([float("inf"), float("-inf")], pd.NA).ffill().bfill().fillna(0.0)

    pred = model.predict(X)
    pred_df = pd.DataFrame(pred, columns=target_columns)
    pred_df.to_csv(args.output_path, index=False)


if __name__ == "__main__":
    main()
