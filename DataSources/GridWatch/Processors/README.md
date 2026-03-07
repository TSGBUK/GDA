# GridwatchData Processors

This folder contains 2 scripts for converting Gridwatch CSV data to parquet and querying yearly summaries.

## Scripts

### `parquet_data_conversion.py`
**What it does**
- Reads `GridwatchData/gridwatch.csv`.
- Parses timestamps and partitions output by year.
- Writes parquet files to `GridwatchData/Parquet/year=YYYY/gridwatch.parquet`.

**How to run**
```bash
python GridwatchData/Processors/parquet_data_conversion.py
```

### `query_gridwatch_data.py`
**What it does**
- Menu-driven analysis over Gridwatch parquet data.
- Includes yearly demand and frequency statistics.
- Includes generation/interconnector totals and yearly summary metrics.
- Supports inspecting raw sample rows for a selected year.

**How to run**
```bash
python GridwatchData/Processors/query_gridwatch_data.py
```

## Typical workflow
1. Run `parquet_data_conversion.py` to build/update parquet.
2. Use `query_gridwatch_data.py` for yearly analytics.
