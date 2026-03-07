# Expert Trainers Guide

This document explains every trainer in `MachineLearning/experts`, what data it uses, what it predicts, and how to train and use each model.

## 1) Folder purpose

The `MachineLearning/experts` folder contains:

- Dataset-specific expert trainers (`train_*_expert.py`)
- Composite experts combining datasets (for example weather + generation)
- A generalized trainer for smaller parquet datasets (`train_additional_parquet_experts.py`)
- A system-level fusion model (`train_system_state_monster.py`)
- A full orchestrator script (`run_all_experts.sh`)

All trainers write artifacts to:

- `MachineLearning/experts/pre-trained-experts/*.joblib`
- `MachineLearning/experts/pre-trained-experts/*_metrics.json`

---

## 2) Quick start

### Run everything in dependency-safe order

```bash
cd /home/ubuntu/GDA/MachineLearning/experts
./run_all_experts.sh --device cuda
```

### Run one expert directly

```bash
cd /home/ubuntu/GDA/MachineLearning/experts
python train_frequency_expert.py --device cuda --training-mode auto
```

### If CUDA reports missing cuDF/cuML

Use the RAPIDS environment explicitly:

```bash
cd /home/ubuntu/GDA/MachineLearning/experts
~/miniconda3/bin/conda run -n tsgb_rapids python train_frequency_expert.py --device cuda --training-mode auto
```

### Validated training environments

The systems below are environments where training has been run successfully (some slowly), plus known pending high-scale targets.

All listed systems use Ubuntu 24 and CUDA 12.2 where CUDA is available.

| System | Runtime | Status | Notes |
|---|---:|---|---|
| Laptop Intel N305, 8GB RAM, CPU-only | 2 weeks 3 days | Successful | Full training completes, but very slow on CPU-only hardware. |
| VPS 8 cores, 16GB RAM, CPU-only | 6 days 20 hours | Successful | Faster than laptop CPU-only baseline. |
| VPS 1x RTX A4000, 24GB RAM, 4 cores, CUDA/CPU | < 1 hour | Successful | Fast, practical baseline for regular retraining. |
| VPS 4x RTX A4000, 90GB RAM, 20 cores, CUDA/CPU | Pending | Pending | Planned multi-GPU validation target. |
| VPS 12x RTX A4000, 384GB RAM, 60 cores, CUDA/CPU | Pending | Pending | Planned large-scale production-style validation target. |

---

## 3) Common model artifact contract

Most experts save the same payload structure:

- `model`: fitted estimator (CPU sklearn, CUDA bundle, or incremental bundle)
- `target_columns`: list of predicted columns
- `feature_columns`: list of model input features
- `metadata`: run metadata (backend, parquet paths, knobs)

Minimal generic load/predict pattern:

```python
import joblib
import pandas as pd

payload = joblib.load("MachineLearning/experts/pre-trained-experts/weather_expert_model.joblib")
model = payload["model"]
feature_columns = payload["feature_columns"]

# X must contain exactly the same engineered features used by the trainer
X = pd.DataFrame(...)
X = X.reindex(columns=feature_columns).fillna(0)

y_pred = model.predict(X)
```

Important: each trainer has its own preprocessing/feature logic. Reuse the corresponding script logic for inference inputs.

---

## 4) Expert catalog

## Generation expert

- Script: `train_generation_expert.py`
- Dataset: `HistoricalGenerationData/Parquet`
- Purpose: predict generation and carbon-related operational series from timestamp features.
- Targets: numeric generation columns excluding `DATETIME`, `year`, and `_perc` derived percentage fields.
- Features: calendar/cyclical features from `DATETIME`.

Train:

```bash
python train_generation_expert.py --device cuda --n-estimators 300 --max-rows 0 --batch-size 250000
```

Use model:

1. Build a DataFrame with `DATETIME`.
2. Apply the same feature engineering as `build_features` in `train_generation_expert.py`.
3. Predict all `target_columns` from the saved payload.

Output files:

- `pre-trained-experts/generation_expert_model.joblib`
- `pre-trained-experts/generation_expert_metrics.json`

---

## Weather expert

- Script: `train_weather_expert.py`
- Dataset: `Weather/Parquet`
- Purpose: weather baseline expert.
- Targets: `Temperature_C`, `Wind_Speed_100m_kph`, `Solar_Radiation_W_m2`.
- Features: calendar/cyclical features from `Date`.

Train:

```bash
python train_weather_expert.py --device cuda --n-estimators 300
```

Use model:

1. Build a DataFrame with `Date` (UTC).
2. Apply `ensure_datetime` + `build_features` logic from `train_weather_expert.py`.
3. Predict the 3 weather targets.

Output files:

- `pre-trained-experts/weather_expert_model.joblib`
- `pre-trained-experts/weather_expert_metrics.json`

---

## Demand expert

- Script: `train_demand_expert.py`
- Dataset: `DemandData/Parquet`
- Purpose: predict demand-side operational targets.
- Targets: all numeric columns excluding `SETTLEMENT_DATE`, `SETTLEMENT_PERIOD`, `DatetimeUTC`, `year`.
- Features: calendar/cyclical features + `settlement_period`.

Train:

```bash
python train_demand_expert.py --device cuda --n-estimators 300
```

Use model:

1. Build frame with either `DatetimeUTC` or `SETTLEMENT_DATE` + `SETTLEMENT_PERIOD`.
2. Apply `ensure_datetime` + `build_features` from `train_demand_expert.py`.
3. Predict all demand target columns from payload.

Output files:

- `pre-trained-experts/demand_expert_model.joblib`
- `pre-trained-experts/demand_expert_metrics.json`

---

## Inertia expert

- Script: `train_inertia_expert.py`
- Dataset: `Inertia/Parquet`
- Purpose: inertia baseline expert.
- Targets: `Outturn Inertia`, `Market Provided Inertia`.
- Features: calendar/cyclical features + `Settlement Period`.

Train:

```bash
python train_inertia_expert.py --device cuda --n-estimators 300
```

Use model:

1. Input frame should provide `DatetimeUTC` and `Settlement Period`.
2. Apply `ensure_datetime` + `build_features` from `train_inertia_expert.py`.
3. Predict the two inertia targets.

Output files:

- `pre-trained-experts/inertia_expert_model.joblib`
- `pre-trained-experts/inertia_expert_metrics.json`

---

## Gridwatch expert

- Script: `train_gridwatch_expert.py`
- Dataset: `GridwatchData/Parquet`
- Purpose: predict gridwatch numeric series from timestamp dynamics.
- Targets: all numeric columns except `timestamp`, `id`, `year`.
- Features: calendar/cyclical features from `timestamp`.
- Memory controls: `--max-rows`, `--batch-size` for safe training.

Train:

```bash
python train_gridwatch_expert.py --device cuda --n-estimators 300 --max-rows 400000 --batch-size 250000
```

Use model:

1. Input frame must include `timestamp`.
2. Apply `ensure_datetime` + `build_features` from `train_gridwatch_expert.py`.
3. Predict all gridwatch target columns listed in payload.

Output files:

- `pre-trained-experts/gridwatch_expert_model.joblib`
- `pre-trained-experts/gridwatch_expert_metrics.json`

---

## Frequency expert

- Script: `train_frequency_expert.py`
- Dataset: `Frequency/Parquet`
- Purpose: high-volume frequency forecasting expert with large-scale modes.
- Target: `Value`.
- Schema resolution: supports `Date/Value` and `dtm/f`-style schemas.
- Features: calendar/cyclical + lags (`lag_1`, `lag_5`, `roll_mean_10`).

Training modes:

- `batch`: in-memory model training path (CPU or CUDA)
- `incremental`: streamed CPU `SGDRegressor.partial_fit` path
- `cuda-sharded`: streamed shard training on CUDA with model ensembling
- `auto`: selects best path based on backend and dataset size

Train (auto mode):

```bash
python train_frequency_expert.py \
  --device cuda \
  --training-mode auto \
  --batch-size 250000 \
  --cuda-batch-max-rows 5000000 \
  --progress-every-chunks 25
```

Train (explicit sharded CUDA):

```bash
python train_frequency_expert.py \
  --device cuda \
  --training-mode cuda-sharded \
  --cuda-gpus auto \
  --cuda-shard-rows 0 \
  --cuda-target-vram-gb 0 \
  --cuda-target-vram-fraction 0.7 \
  --cuda-target-vram-cap-gb 8
```

Use model:

1. Build frame with canonical columns `Date`, `Value`.
2. Apply `ensure_datetime` + `build_features` from `train_frequency_expert.py`.
3. Predict next-step `Value`.

Output files:

- `pre-trained-experts/frequency_expert_model.joblib`
- `pre-trained-experts/frequency_expert_metrics.json`

---

## Balancing expert

- Script: `train_balancing_expert.py`
- Dataset: `BalancingServices/Parquet`
- Purpose: balancing cost/imbalance expert.
- Targets: numeric columns excluding `SETT_DATE`, `SETT_PERIOD`, `DatetimeUTC`, `year`.
- Features: calendar/cyclical features + settlement period.

Train:

```bash
python train_balancing_expert.py --device cuda --n-estimators 300
```

Use model:

1. Provide `DatetimeUTC` or `SETT_DATE` + `SETT_PERIOD`.
2. Apply `ensure_datetime` + `build_features` in `train_balancing_expert.py`.
3. Predict all balancing target columns in payload.

Output files:

- `pre-trained-experts/balancing_expert_model.joblib`
- `pre-trained-experts/balancing_expert_metrics.json`

---

## Weather + Generation expert

- Script: `train_weather_generation_expert.py`
- Datasets:
  - `Weather/Parquet`
  - `HistoricalGenerationData/Parquet`
- Purpose: predict generation targets conditioned on weather.
- Targets: generation numeric columns excluding `DATETIME`, `year`, `_perc`.
- Features: calendar/cyclical + weather signals.
- Merge logic: nearest as-of join on timestamp with configurable gap.

Train:

```bash
python train_weather_generation_expert.py --device cuda --n-estimators 400 --max-rows 0 --batch-size 250000
```

Use model:

1. Build merged weather-generation frame using `merge_weather_generation` logic.
2. Apply `build_features` from this script.
3. Predict generation target columns from payload.

Output files:

- `pre-trained-experts/weather_generation_expert_model.joblib`
- `pre-trained-experts/weather_generation_expert_metrics.json`

---

## Weather + Inertia expert

- Script: `train_weather_inertia_expert.py`
- Datasets:
  - `Weather/Parquet`
  - `Inertia/Parquet`
- Purpose: predict inertia targets conditioned on weather.
- Targets: `Outturn Inertia`, `Market Provided Inertia`.
- Features: calendar/cyclical + weather signals + settlement period.
- Merge logic: nearest as-of join on timestamp with configurable gap.

Train:

```bash
python train_weather_inertia_expert.py --device cuda --n-estimators 400
```

Use model:

1. Build merged weather-inertia frame using `merge_weather_inertia` logic.
2. Apply `build_features` in this script.
3. Predict inertia targets.

Output files:

- `pre-trained-experts/weather_inertia_expert_model.joblib`
- `pre-trained-experts/weather_inertia_expert_metrics.json`

---

## Additional parquet experts

- Script: `train_additional_parquet_experts.py`
- Purpose: train expert models for datasets not covered by base experts.
- Supported datasets and outputs:
  - `BSAD_AggregatedData` -> `bsad_aggregated_expert_*`
  - `BSAD_ForwardContracts` -> `bsad_forwardcontracts_expert_*`
  - `CarbonIntensityOfBalancingActions` -> `carbonintensity_balancingactions_expert_*`
  - `EACEnduringAuctionCapability` -> `eac_enduringauctioncapability_expert_*`
  - `EC-BR_AuctionResults` -> `ecbr_auctionresults_expert_*`

Modes:

- `batch`: in-memory training
- `incremental`: streamed CPU incremental training
- `auto`: batch for smaller sets, incremental for very large sets (based on `--incremental-threshold-rows`)

Recommended full-data command:

```bash
python train_additional_parquet_experts.py \
  --device cuda \
  --training-mode auto \
  --incremental-threshold-rows 1000000 \
  --max-rows-per-dataset 0 \
  --batch-size 100000 \
  --progress-every-chunks 25
```

Use model (per dataset):

1. Load the corresponding `*_expert_model.joblib`.
2. Read `feature_columns` and build matching feature frame from timestamps/settlement as done in this trainer.
3. Predict target columns listed in that payload.

---

## System-state monster model

- Training script: `train_system_state_monster.py`
- Inference script: `run_monster_inference.py`
- Purpose: fuse frequency + all dataset joins + optional expert features into a high-capacity ensemble.

Train monster (example):

```bash
python train_system_state_monster.py \
  --rounds 12 \
  --base-estimators 300 \
  --max-frequency-files 0 \
  --max-frequency-raw-rows 2000000 \
  --row-stride 1 \
  --max-rows 0 \
  --source-max-rows 300000 \
  --source-batch-size 100000
```

Low-memory controls:

- `--max-frequency-raw-rows`: caps raw frequency rows loaded before resampling.
- `--source-max-rows`: caps rows loaded per joined dataset source.
- `--source-batch-size`: parquet scan batch size for source loading.

Run monster inference:

```bash
python run_monster_inference.py \
  --manifest-path MachineLearning/experts/pre-trained-experts/monster/monster_ensemble_manifest.json
```

Monster outputs:

- `pre-trained-experts/monster/monster_ensemble_manifest.json`
- `pre-trained-experts/monster/monster_run_summary.json`
- Inference CSV/JSON paths from `run_monster_inference.py` args

---

## 5) Orchestrator usage (`run_all_experts.sh`)

This script runs experts in sequence and applies OOM fallback when needed.

### Typical CUDA run

```bash
cd /home/ubuntu/GDA/MachineLearning/experts
./run_all_experts.sh --device cuda
```

### Useful stability knobs

- Gridwatch memory:
  - `--gridwatch-max-rows`
  - `--gridwatch-batch-size`
- Frequency scale behavior:
  - `--frequency-training-mode auto|batch|incremental|cuda-sharded`
  - `--frequency-cuda-batch-max-rows`
  - `--frequency-cuda-target-vram-*`
- Additional experts full-data behavior:
  - `--additional-training-mode auto|batch|incremental`
  - `--additional-incremental-threshold-rows`
  - `--additional-max-rows-per-dataset 0`

### OOM retry knobs

- `--oom-retry-estimators`
- `--oom-retry-generation-max-rows`
- `--oom-retry-additional-max-rows-per-dataset`
- Monster-specific retry knobs (`--oom-retry-monster-*`)

---

## 6) Practical inference guidance per expert

For each expert model:

1. Load payload from `pre-trained-experts/<expert>_model.joblib`.
2. Build the same engineered features as in that expert script.
3. Reindex to payload `feature_columns`.
4. Fill nulls/inf safely (`ffill`, `bfill`, `fillna(0)` as needed).
5. Run `model.predict(X)`.
6. Map output array columns to payload `target_columns`.

If you need repeatable production inference, create a dedicated `predict_<expert>.py` per trainer that imports and reuses each script’s preprocessing functions.

---

## 7) Notes and caveats

- CUDA availability is environment-dependent. Prefer running in `tsgb_rapids`.
- Some very large datasets are intentionally routed to CPU incremental mode in `auto` for memory safety.
- `_metrics.json` and `.joblib` files are generated artifacts; they should remain ignored in git.
- Quality metrics can differ significantly between datasets depending on target scale and sparsity.

---

## 8) Inference templates (`predict_examples/`)

A ready-to-run template set is available in:

- `MachineLearning/experts/predict_examples`

Files:

- `predict_standard_expert.py`
  - Covers `weather`, `demand`, `inertia`, `gridwatch`, `balancing`, `generation`.
  - Imports the corresponding trainer module and reuses its preprocessing.
- `predict_frequency_expert.py`
  - Handles standard frequency model payloads and CUDA sharded ensemble payloads.
- `predict_composite_expert.py`
  - Supports `weather_generation` and `weather_inertia` by reusing merge + feature logic.
- `predict_additional_expert.py`
  - Template for models produced by `train_additional_parquet_experts.py`.

Run examples (from `MachineLearning/experts`):

```bash
python predict_examples/predict_standard_expert.py \
  --expert weather \
  --model-path pre-trained-experts/weather_expert_model.joblib \
  --input-path /path/to/input.csv \
  --output-path /path/to/predictions.csv
```

```bash
python predict_examples/predict_frequency_expert.py \
  --model-path pre-trained-experts/frequency_expert_model.joblib \
  --input-path /path/to/frequency_input.csv \
  --output-path /path/to/frequency_predictions.csv
```

```bash
python predict_examples/predict_composite_expert.py \
  --expert weather_generation \
  --model-path pre-trained-experts/weather_generation_expert_model.joblib \
  --weather-input-path /path/to/weather.csv \
  --target-input-path /path/to/generation.csv \
  --output-path /path/to/composite_predictions.csv
```

```bash
python predict_examples/predict_additional_expert.py \
  --model-path pre-trained-experts/eac_enduringauctioncapability_expert_model.joblib \
  --input-path /path/to/eac_input.csv \
  --output-path /path/to/eac_predictions.csv
```
