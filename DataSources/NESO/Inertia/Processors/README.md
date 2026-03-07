# Inertia Processors

This folder contains 3 scripts for inertia data conversion, query analysis, and charting.

## Scripts

### `parquet_data_conversion.py`
**What it does**
- Reads inertia CSV files from `Inertia/`.
- Builds `DatetimeUTC` from settlement date + settlement period.
- Converts outturn/market inertia columns to numeric.
- Writes partitioned parquet files to `Inertia/Parquet/year=YYYY/`.

**How to run**
```bash
python Inertia/Processors/parquet_data_conversion.py
```

### `query_inertia_data.py`
**What it does**
- Menu-driven inertia analysis over parquet data.
- Reports yearly average/low/high outturn inertia.
- Reports market-vs-outturn average gap.
- Reports min/median/max tables and unit conversions (`GVAs`, `MWs`, `MW_equiv`, `% capacity`).

**How to run**
```bash
python Inertia/Processors/query_inertia_data.py
```

### `chart_inertia_data.py`
**What it does**
- Creates interactive Plotly inertia charts over time.
- Plots outturn inertia, market provided inertia, and their difference.
- Supports year/date filtering, resampling, and display unit selection.

**How to run**
```bash
python Inertia/Processors/chart_inertia_data.py
```

**Prompts you will get**
- Year or all years
- Start/end date filters
- Resample frequency
- Display unit (`GVAs`, `MWs`, `MW_equiv`, `PctCapacity`)

## Typical workflow
1. Run `parquet_data_conversion.py` to build/update parquet.
2. Use `query_inertia_data.py` for tabular checks.
3. Use `chart_inertia_data.py` for visual trends.
