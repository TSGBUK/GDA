# UKPN Processors

This folder contains CSV-to-Parquet processors for UK Power Networks exports.

- `ukpn_parquet_common.py`: shared conversion helpers
- `parse_<dataset>.py`: one parser per dataset slug (`*__export.csv`)
- `parquet_data_conversion.py`: master runner that walks history and triggers parsers

## Run

```sh
python DataSources/UkPowerNetworks/Processors/parquet_data_conversion.py
```

## Force reconvert

```sh
python DataSources/UkPowerNetworks/Processors/parquet_data_conversion.py --force
```

Parquet output root:
- `DataSources/UkPowerNetworks/Parquet/...`
