# HistoricalGenerationData Processors

This folder currently contains 1 script for charting historical generation by fuel type.

## Script

### `chart_fuel_data.py`
**What it does**
- Reads `HistoricalGenerationData/df_fuel_ckan.csv`.
- Builds interactive Plotly charts for:
  - Fuel generation stack (GW)
  - Fossil generation percentage
  - Total generation (GW)
- Supports optional fuel selection, date-range filters, and resampling.

**How to run**
```bash
python HistoricalGenerationData/Processors/chart_fuel_data.py
```

**Prompts you will get**
- Fuel list (comma-separated) or Enter for defaults
- Start date (`YYYY-MM-DD`) or Enter
- End date (`YYYY-MM-DD`) or Enter
- Resample frequency (for example `1H`, `1D`) or Enter for native resolution
