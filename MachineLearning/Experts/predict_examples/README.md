# Predict Examples

This folder contains minimal inference templates for expert model families.

## Files

- `predict_standard_expert.py`
  - For `weather`, `demand`, `inertia`, `gridwatch`, `balancing`, `generation`
  - Imports each trainer’s preprocessing (`ensure_datetime` when present, and `build_features`)
- `predict_frequency_expert.py`
  - For `frequency_expert_model.joblib`
  - Handles standard models and CUDA sharded ensemble payloads
- `predict_composite_expert.py`
  - For `weather_generation` and `weather_inertia`
  - Runs the same merge + feature logic as trainer scripts
- `predict_additional_expert.py`
  - For outputs from `train_additional_parquet_experts.py`
  - Builds calendar features and maps to saved `feature_columns`

## Quick usage

From `MachineLearning/experts`:

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
