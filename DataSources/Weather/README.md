# Weather Scripts

This folder contains 3 scripts for weather data download, conversion to parquet, and query analysis.

## Scripts

### `weather_data.py`
**What it does**
- Downloads hourly UK weather data from Open-Meteo archive API.
- Retrieves temperature, 100m wind speed, and direct radiation.
- Saves output to CSV named `uk_weather_data_<start>_<end>.csv`.

**How to run**
```bash
python Weather/weather_data.py
```

**Prompts you will get**
- Start date (`YYYY-MM-DD`)
- End date (`YYYY-MM-DD`)

### `parquet_data_conversion.py`
**What it does**
- Reads weather CSV (`uk_weather_data_2010-01-01_2025-12-31.csv`).
- Parses and validates datetime values.
- Writes partitioned parquet files to `Weather/Parquet/year=YYYY/`.

**How to run**
```bash
python Weather/parquet_data_conversion.py
```

### `query_weather_data.py`
**What it does**
- Menu-driven yearly weather statistics on parquet data.
- Provides temperature, wind-speed, and solar-radiation stats.
- Includes monthly averages for a selected year.

**How to run**
```bash
python Weather/query_weather_data.py
```

## Typical workflow
1. Run `weather_data.py` to collect/update raw CSV.
2. Run `parquet_data_conversion.py` to build parquet partitions.
3. Run `query_weather_data.py` for summary analysis.
