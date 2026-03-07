# MachineLearning Scripts

This folder contains machine-learning data loading, feature-engineering utilities, and expert-model trainers.

## Scripts

### `ml_pipeline.py`
**What it does**
- Provides reusable loaders for generation, weather, frequency, inertia, demand, balancing, and gridwatch datasets.
- Supports optional year filtering for partitioned parquet sources.
- Supports optional GPU-backed loading when RAPIDS (`cudf` / `dask_cudf`) is installed.
- Includes helper utilities to:
  - Merge multiple datasets on time index
  - Add datetime features (`hour`, `dayofweek`, `month`, `season`)
  - Label frequency out-of-band events

**How to use**
This is primarily a module (import from notebooks/scripts) rather than an interactive CLI.

```python
from MachineLearning import ml_pipeline

gen = ml_pipeline.load_generation()
weather = ml_pipeline.load_weather(years=[2024, 2025])
freq = ml_pipeline.load_frequency(years=2025)

merged = ml_pipeline.merge_all(gen, weather, freq)
features = ml_pipeline.add_datetime_features(merged)
labelled = ml_pipeline.label_frequency_events(features)
```

### `__init__.py`
**What it does**
- Package marker for the `MachineLearning` module.

### `Patternator/patternator.py`
**What it does**
- Runs a frequency-first pattern surfacing pipeline over raw CSV datasets.
- Computes coarse instability metrics from frequency windows and joins weather, inertia, demand, and generation features.
- Exports ranked correlation and feature-importance reports plus generation-mix bucket summaries.

**How to run**
```bash
python MachineLearning/Patternator/patternator.py --max-frequency-files 12 --row-stride 5 --resample 1H
```

## Experts

### `experts/train_balancing_expert.py`
**What it does**
- Trains a BalancingServices expert model from parquet data.
- Uses chronological train/test split (time-aware) and predicts all numeric balancing cost columns.
- Saves:
  - model artifact: `MachineLearning/experts/pre-trained-experts/balancing_expert_model.joblib`
  - metrics JSON: `MachineLearning/experts/pre-trained-experts/balancing_expert_metrics.json`
- If parquet is not ready yet, exits gracefully with an info message.

**How to run**
```bash
python MachineLearning/experts/train_balancing_expert.py
```

**Useful options**
```bash
python MachineLearning/experts/train_balancing_expert.py --n-estimators 500 --train-fraction 0.85
python MachineLearning/experts/train_balancing_expert.py --parquet-dir BalancingServices/Parquet --output-dir MachineLearning/experts/pre-trained-experts
python MachineLearning/experts/train_balancing_expert.py --device auto
python MachineLearning/experts/train_balancing_expert.py --device cuda
python MachineLearning/experts/train_balancing_expert.py --device cpu
```

**CUDA support**
- The trainer now supports `--device {auto,cpu,cuda}`.
- `auto` uses CUDA when RAPIDS (`cudf` + `cuml`) is available, otherwise falls back to CPU.
- `cuda` enforces GPU mode and returns an error if RAPIDS is not installed.

### `experts/train_demand_expert.py`
**What it does**
- Trains a DemandData expert model from parquet data.
- Uses chronological train/test split (time-aware) and predicts all numeric demand columns.
- Saves:
  - model artifact: `MachineLearning/experts/pre-trained-experts/demand_expert_model.joblib`
  - metrics JSON: `MachineLearning/experts/pre-trained-experts/demand_expert_metrics.json`
- If parquet is not ready yet, exits gracefully with an info message.

**How to run**
```bash
python MachineLearning/experts/train_demand_expert.py
```

**Useful options**
```bash
python MachineLearning/experts/train_demand_expert.py --n-estimators 500 --train-fraction 0.85
python MachineLearning/experts/train_demand_expert.py --parquet-dir DemandData/Parquet --output-dir MachineLearning/experts/pre-trained-experts
python MachineLearning/experts/train_demand_expert.py --device auto
python MachineLearning/experts/train_demand_expert.py --device cuda
python MachineLearning/experts/train_demand_expert.py --device cpu
```

**CUDA support**
- Supports `--device {auto,cpu,cuda}` with the same behavior as balancing trainer.

### `experts/common_trainer.py`
**What it does**
- Shared training utility used by dataset-specific expert trainers.
- Provides:
  - backend resolution (`auto/cpu/cuda`)
  - CPU training path (scikit-learn multi-output random forest)
  - CUDA training path (RAPIDS cuML random forest with per-target models)
  - common metrics and artifact persistence helpers

### `experts/train_frequency_expert.py`
**What it does**
- Trains a Frequency expert model from parquet data.
- Builds time features plus lag features (`f_lag_1`, `f_lag_2`, `f_lag_3`) and predicts `f`.
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_frequency_expert.py
```

### `experts/train_inertia_expert.py`
**What it does**
- Trains an Inertia expert model from parquet data.
- Predicts numeric inertia outputs (including reported outturn/market columns where available).
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_inertia_expert.py
```

### `experts/train_weather_expert.py`
**What it does**
- Trains a Weather expert model from parquet data.
- Predicts key numeric weather signals (temperature, wind speed, solar irradiance, etc. as present).
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_weather_expert.py
```

### `experts/train_generation_expert.py`
**What it does**
- Trains a HistoricalGenerationData expert model from canonical CSV data.
- Builds datetime features and predicts all numeric generation columns.
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_generation_expert.py
```

### `experts/train_gridwatch_expert.py`
**What it does**
- Trains a GridwatchData expert model from parquet data.
- Uses datetime-derived features and predicts all numeric gridwatch columns.
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_gridwatch_expert.py
```

### `experts/train_weather_inertia_expert.py`
**What it does**
- Trains a custom Weather+Inertia fusion expert.
- Uses weather signals (`Temperature_C`, `Wind_Speed_100m_kph`, `Solar_Radiation_W_m2`) as exogenous features.
- Merges Weather and Inertia datasets by nearest timestamp (`merge_asof`) with configurable tolerance.
- Predicts inertia targets: `Outturn Inertia` and `Market Provided Inertia`.
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_weather_inertia_expert.py
```

**Useful options**
```bash
python MachineLearning/experts/train_weather_inertia_expert.py --max-merge-gap-minutes 30 --device auto
python MachineLearning/experts/train_weather_inertia_expert.py --weather-parquet-dir Weather/Parquet --inertia-parquet-dir Inertia/Parquet
```

### `experts/train_weather_generation_expert.py`
**What it does**
- Trains a custom Weather+HistoricalGeneration fusion expert.
- Uses weather signals (`Temperature_C`, `Wind_Speed_100m_kph`, `Solar_Radiation_W_m2`) as exogenous features.
- Merges Weather parquet and generation CSV by nearest timestamp (`merge_asof`) with configurable tolerance.
- Predicts all numeric generation columns from `HistoricalGenerationData/df_fuel_ckan.csv`.
- Saves model and metrics under `MachineLearning/experts/pre-trained-experts/`.

**How to run**
```bash
python MachineLearning/experts/train_weather_generation_expert.py
```

**Useful options**
```bash
python MachineLearning/experts/train_weather_generation_expert.py --max-merge-gap-minutes 60 --device auto
python MachineLearning/experts/train_weather_generation_expert.py --generation-csv-path HistoricalGenerationData/df_fuel_ckan.csv
```

### `experts/train_system_state_monster.py`
**What it does**
- Trains a high-capacity fusion model from raw Frequency CSV files plus all available datasets:
  - Weather, Inertia, DemandData, BalancingServices, GridwatchData, HistoricalGenerationData
- Uses raw frequency as the anchor timeline, then nearest-time merges all other data sources.
- Creates high-resolution frequency window features (rolling stats, ramps, threshold flags) and datetime/cyclical features.
- Optionally loads all `*_model.joblib` files from `pre-trained-experts` and injects their predictions as additional meta-features.
- Trains iterative rounds (increasing tree count each round), saves every checkpoint, then writes a weighted merge manifest of top rounds.

**Saved outputs**
- Round checkpoints: `monster_round_XXX_model.joblib`
- Round metrics: `monster_round_XXX_metrics.json`
- Merge manifest: `monster_ensemble_manifest.json`
- Run summary: `monster_run_summary.json`

**How to run (full build)**
```bash
python MachineLearning/experts/train_system_state_monster.py --rounds 20 --base-estimators 500 --resolution 1S --join-tolerance 35m
```

**How to run (very heavy / high-iteration)**
```bash
python MachineLearning/experts/train_system_state_monster.py --rounds 40 --base-estimators 800 --resolution 1S --top-k-merge 6
```

**Scale-control options**
```bash
python MachineLearning/experts/train_system_state_monster.py --max-frequency-files 12 --row-stride 2 --max-rows 5000000
python MachineLearning/experts/train_system_state_monster.py --disable-expert-features
```

### `experts/run_monster_inference.py`
**What it does**
- Loads `monster_ensemble_manifest.json` and runs ensemble inference over fused system-state features.
- Rebuilds the same frequency-anchored fusion frame (raw frequency + joined Weather/Inertia/Demand/Balancing/Gridwatch/Generation).
- Applies each selected checkpoint model, computes weighted blended predictions, and writes per-target uncertainty (`*_std`) across member outputs.

**Saved outputs**
- Predictions CSV: `MachineLearning/experts/pre-trained-experts/monster/monster_inference.csv`
- Summary JSON: `MachineLearning/experts/pre-trained-experts/monster/monster_inference_summary.json`

**How to run**
```bash
python MachineLearning/experts/run_monster_inference.py
```

**Useful options**
```bash
python MachineLearning/experts/run_monster_inference.py --manifest-path MachineLearning/experts/pre-trained-experts/monster/monster_ensemble_manifest.json
python MachineLearning/experts/run_monster_inference.py --max-frequency-files 8 --resolution 1S --join-tolerance 35m
python MachineLearning/experts/run_monster_inference.py --row-stride 2 --max-rows 5000000
```

**Common device option for all expert trainers**
```bash
python MachineLearning/experts/train_frequency_expert.py --device auto
python MachineLearning/experts/train_inertia_expert.py --device cuda
python MachineLearning/experts/train_weather_expert.py --device cpu
python MachineLearning/experts/train_generation_expert.py --device auto
python MachineLearning/experts/train_gridwatch_expert.py --device auto
python MachineLearning/experts/train_weather_inertia_expert.py --device auto
python MachineLearning/experts/train_weather_generation_expert.py --device auto
```
