# Frequency Processors

This folder contains 4 scripts for filename normalization, parquet conversion, query analysis, and charting of grid frequency data.

## Scripts

### `fix_file_names.py`
**What it does**
- Normalizes raw frequency CSV filenames into `f-YYYY-M.csv` format.
- Helps ensure conversion scripts can infer year/month reliably.

**How to run**
```bash
python Frequency/Processors/fix_file_names.py
```

### `parquet_data_conversion.py`
**What it does**
- Reads frequency CSV files from `Frequency/`.
- Parses mixed datetime formats into UTC.
- Cleans numeric frequency values and writes partitioned parquet files to `Frequency/Parquet/year=YYYY/`.

**How to run**
```bash
python Frequency/Processors/parquet_data_conversion.py
```

### `query_freq_data.py`
**What it does**
- Menu-driven analysis over parquet data.
- Includes average/min/max frequency, under/over-generation excursions, RoCoF counts and clustered RoCoF analysis.
- Includes volatility, recovery, yearly nadir/zenith, rough inertia trend estimates, and time-in-band statistics.

**How to run**
```bash
python Frequency/Processors/query_freq_data.py
```

### `chart_freq_data.py`
**What it does**
- Interactive Plotly charts for:
  - Frequency and RoCoF over time
  - Time-in-band duration per year
- Supports year/date filters and configurable resample frequency.

**How to run**
```bash
python Frequency/Processors/chart_freq_data.py
```

## Typical workflow
1. Run `fix_file_names.py` (optional, when raw naming is inconsistent).
2. Run `parquet_data_conversion.py` to build/update parquet.
3. Use `query_freq_data.py` for menu-based diagnostics.
4. Use `chart_freq_data.py` for interactive visuals.
