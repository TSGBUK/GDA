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
    parser = argparse.ArgumentParser(description="Predict with a standard expert model")
    parser.add_argument("--expert", required=True, choices=["weather", "demand", "inertia", "gridwatch", "balancing", "generation"])
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    _add_parent_to_path()

    module_map = {
        "weather": "train_weather_expert",
        "demand": "train_demand_expert",
        "inertia": "train_inertia_expert",
        "gridwatch": "train_gridwatch_expert",
        "balancing": "train_balancing_expert",
        "generation": "train_generation_expert",
    }

    module = __import__(module_map[args.expert], fromlist=["*"])

    payload = joblib.load(args.model_path)
    model = payload["model"]
    feature_columns = payload["feature_columns"]
    target_columns = payload["target_columns"]

    raw = load_input(Path(args.input_path))
    if hasattr(module, "ensure_datetime"):
        raw = module.ensure_datetime(raw)

    features = module.build_features(raw)
    X = features.reindex(columns=feature_columns).replace([float("inf"), float("-inf")], pd.NA).ffill().bfill().fillna(0.0)

    pred = model.predict(X)
    pred_df = pd.DataFrame(pred, columns=target_columns)
    pred_df.to_csv(args.output_path, index=False)


if __name__ == "__main__":
    main()
