#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os

import joblib
import numpy as np
import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit one CUDA RF shard model for frequency expert")
    parser.add_argument("--x-npy", required=True)
    parser.add_argument("--y-npy", required=True)
    parser.add_argument("--feature-cols-json", required=True)
    parser.add_argument("--gpu-id", type=int, default=0)
    parser.add_argument("--n-estimators", type=int, required=True)
    parser.add_argument("--random-state", type=int, required=True)
    parser.add_argument("--model-out", required=True)
    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu_id)

    import cudf
    from cuml.ensemble import RandomForestRegressor as CuRandomForestRegressor

    with open(args.feature_cols_json, "r", encoding="utf-8") as f:
        feature_cols = json.load(f)

    x_np = np.load(args.x_npy)
    y_np = np.load(args.y_npy)

    x_gpu = cudf.DataFrame.from_pandas(pd.DataFrame(x_np, columns=feature_cols))
    y_gpu = cudf.Series(y_np)

    model = CuRandomForestRegressor(
        n_estimators=args.n_estimators,
        random_state=args.random_state,
        max_depth=16,
        min_samples_leaf=2,
    )
    model.fit(x_gpu, y_gpu)

    joblib.dump(model, args.model_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
