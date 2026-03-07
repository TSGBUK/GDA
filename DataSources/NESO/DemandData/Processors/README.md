# DemandData Processors

This folder contains 3 scripts for demand data conversion, query analysis, and charting.

## Scripts

### `parquet_data_conversion.py`
**What it does**
- Reads `DemandData/demanddata_*.csv` files.
- Builds `DatetimeUTC` from settlement date + settlement period.
- Applies schema-aware numeric typing for demand/interconnector fields.
- Writes partitioned parquet files to `DemandData/Parquet/year=YYYY/`.

**How to run**
```bash
python DemandData/Processors/parquet_data_conversion.py
```

### `query_demand_data.py`
**What it does**
- Loads demand parquet data through a menu-driven CLI.
- Shows schema descriptions and units.
- Produces yearly ND/TSD statistics.
- Produces yearly totals across numeric columns and monthly averages for a selected year.

**How to run**
```bash
python DemandData/Processors/query_demand_data.py
```

### `chart_demand_data.py`
**What it does**
- Creates interactive Plotly charts for demand data.
- Plots ND/TSD time series (resampled).
- Plots monthly average demand bars for a selected year.

**How to run**
```bash
python DemandData/Processors/chart_demand_data.py
```

## Typical workflow
1. Run `parquet_data_conversion.py` to build/update parquet.
2. Use `query_demand_data.py` for numeric summaries.
3. Use `chart_demand_data.py` for visual trends.
