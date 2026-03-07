# Patternator

Patternator is a coarse, frequency-first pattern surfacing pipeline.

It treats system frequency as the anchor signal, computes instability metrics, joins weather/inertia/demand/generation features on a common resampled timeline, and produces ranked pattern outputs for refinement.

## What it does

- Loads raw data directly from:
  - `Frequency/f-*.csv`
  - `Weather/uk_weather_data_2010-01-01_2025-12-31.csv`
  - `Inertia/inertia*.csv`
  - `DemandData/demanddata_*.csv`
  - `HistoricalGenerationData/df_fuel_ckan.csv`
- Builds frequency instability target metrics per resample window.
- Produces:
  - linear relationship ranking (`top_correlations.csv`)
  - non-linear ranking (`feature_importance.csv`)
  - generation-mix bucket pattern tables (`pattern_gen_*_perc.csv` where available)

## Quick run

```bash
python MachineLearning/Patternator/patternator.py --max-frequency-files 12 --row-stride 5 --resample 1H
```

## More complete run

```bash
python MachineLearning/Patternator/patternator.py --max-frequency-files 60 --row-stride 2 --resample 30T --top-n 40
```

## Main outputs

Default output folder:

`MachineLearning/Patternator/output`

Files:

- `patternator_timeseries.csv` (joined analysis table)
- `top_correlations.csv` (top absolute correlations with `instability_score`)
- `feature_importance.csv` (random-forest feature importance)
- `pattern_gen_*.csv` (generation mix instability buckets)
- `summary.json` (run metadata + top findings)

## Notes

- Frequency is downsampled by `--row-stride` before window aggregation to keep runs practical.
- Use smaller `--max-frequency-files` for rapid iteration, then scale up.
- This is intentionally coarse pattern discovery for later refinement.
