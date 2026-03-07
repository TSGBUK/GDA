# BalancingServices Processors

This folder contains 3 scripts for converting, querying, and charting balancing services cost data.

## Scripts

### `parquet_data_conversion.py`
**What it does**
- Reads yearly balancing services CSV files from `BalancingServices/`.
- Builds `DatetimeUTC` from settlement date + period.
- Converts numeric cost columns and writes partitioned parquet files to `BalancingServices/Parquet/year=YYYY/`.

**How to run**
```bash
python BalancingServices/Processors/parquet_data_conversion.py
```

### `query_balancing_services.py`
**What it does**
- Loads parquet data and provides a menu-driven CLI.
- Calculates yearly totals/averages by cost category.
- Provides monthly totals for a selected year.
- Computes burden-per-house metrics when a meter/house count is provided.

**How to run**
```bash
python BalancingServices/Processors/query_balancing_services.py
```

### `chart_balancing_services.py`
**What it does**
- Creates interactive Plotly charts from parquet data.
- Supports yearly stacked totals by category.
- Supports cost time-series charting with configurable resample frequency.

**How to run**
```bash
python BalancingServices/Processors/chart_balancing_services.py
```

## Typical workflow
1. Run `parquet_data_conversion.py` to build/update parquet.
2. Use `query_balancing_services.py` for tabular analysis.
3. Use `chart_balancing_services.py` for visual analysis.
