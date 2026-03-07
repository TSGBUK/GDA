# InertiaCosts Processors

This folder contains scripts for converting, querying, and charting the inertia cost dataset.

## Scripts

### `parquet_data_conversion.py`
**What it does**
- Reads inertia cost CSV files from `InertiaCosts/`.
- Parses mixed settlement date formats (`YYYY-MM-DD` and `DD/MM/YYYY`).
- Converts `Cost_per_GVAs` to numeric and drops invalid rows.
- Writes partitioned parquet files to `InertiaCosts/Parquet/year=YYYY/`.

**How to run**
```bash
python InertiaCosts/Processors/parquet_data_conversion.py
```

### `query_inertia_costs.py`
**What it does**
- Menu-driven analysis over the parquet dataset.
- Reports yearly average/min/max/total costs and zero/non-zero day counts.
- Reports monthly aggregates for a selected year.
- Reports top-N highest-cost days.

**How to run**
```bash
python InertiaCosts/Processors/query_inertia_costs.py
```

### `chart_inertia_costs.py`
**What it does**
- Creates interactive Plotly charts for inertia costs.
- Plots yearly summary bars (average, maximum, total).
- Plots cost time series with configurable resampling.

**How to run**
```bash
python InertiaCosts/Processors/chart_inertia_costs.py
```

## Typical workflow
1. Run `parquet_data_conversion.py` to build/update parquet files.
2. Run `query_inertia_costs.py` for table-based checks.
3. Run `chart_inertia_costs.py` for interactive trend visualisation.
