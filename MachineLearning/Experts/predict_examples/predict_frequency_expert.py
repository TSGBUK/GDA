#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import joblib
import numpy as np
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


def _predict_from_payload(model_obj: object, X: pd.DataFrame) -> np.ndarray:
    if isinstance(model_obj, dict) and model_obj.get("kind") == "cuda_sharded_ensemble":
        members = model_obj.get("members", [])
        if not members:
            raise ValueError("cuda_sharded_ensemble has no members")
        preds = []
        for member in members:
            estimator = member["estimator"]
            member_pred = estimator.predict(X)
            preds.append(np.asarray(member_pred).reshape(-1, 1))
        stacked = np.hstack(preds)
        return stacked.mean(axis=1)

    pred = model_obj.predict(X)
    return np.asarray(pred).reshape(-1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict with frequency expert model")
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    _add_parent_to_path()
    import train_frequency_expert as freq

    payload = joblib.load(args.model_path)
    model = payload["model"]
    feature_columns = payload["feature_columns"]

    raw = load_input(Path(args.input_path))
    raw = freq.ensure_datetime(raw)
    features = freq.build_features(raw)

    X = features.reindex(columns=feature_columns).replace([float("inf"), float("-inf")], pd.NA).ffill().bfill().fillna(0.0)
    pred = _predict_from_payload(model, X)

    out = pd.DataFrame({"prediction": pred})
    out.to_csv(args.output_path, index=False)


if __name__ == "__main__":
    main()
