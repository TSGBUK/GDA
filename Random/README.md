# Random Folder Scripts

This folder contains 8 analysis scripts focused on weather, generation, inertia estimation, frequency compliance checks, and RoCoF derivation.

## Common Data Used

- Weather parquet data: `GDA/Weather/Parquet`
- Generation CSV: `GDA/HistoricalGenerationData/df_fuel_ckan.csv`
- Inertia CSV data: `GDA/Inertia/*.csv`

## How To Use Each Script

### 1) `solar_analysis_simple.py`

**What it does**
- Loads solar radiation weather data and SOLAR generation data.
- Merges both datasets by timestamp (with up to 1-hour match tolerance).
- Prints correlation, solar-radiation bin stats, and summary statistics.
- Runs with a default date window of `2015-01-01` to `2025-01-01`.

**How to run**
```bash
python solar_analysis_simple.py
```

**Output**
- Console-only statistical output (no charts).

### 2) `solar_generation_analysis.py`

**What it does**
- Performs the same core solar correlation/statistical analysis as the simple script.
- Adds interactive Plotly charts:
	- Scatter plot (solar radiation vs solar generation)
	- 2D density plot
	- Multi-panel time series (solar radiation, SOLAR generation, GAS generation, total generation)

**How to run**
```bash
python solar_generation_analysis.py
```

**Prompts you will get**
- Year(s) (comma-separated) or Enter for all
- Start date (`YYYY-MM-DD`) or Enter
- End date (`YYYY-MM-DD`) or Enter
- Resample frequency (for example `1H` or `1D`, default `1D`)

**Output**
- Console stats + interactive browser plots.

### 3) `wind_analysis_simple.py`

**What it does**
- Loads wind speed weather data and WIND generation data.
- Merges both datasets by timestamp (with up to 1-hour match tolerance).
- Prints correlation, wind-speed bin stats, and summary statistics.
- Uses all available years by default.

**How to run**
```bash
python wind_analysis_simple.py
```

**Output**
- Console-only statistical output (no charts).

### 4) `wind_speed_generation_analysis.py`

**What it does**
- Performs core wind correlation/statistical analysis.
- Adds interactive Plotly charts:
	- Scatter plot (wind speed vs wind generation)
	- 2D density plot
	- Multi-panel time series (wind speed, WIND generation, GAS generation, total generation)

**How to run**
```bash
python wind_speed_generation_analysis.py
```

**Prompts you will get**
- Year(s) (comma-separated) or Enter for all
- Start date (`YYYY-MM-DD`) or Enter
- End date (`YYYY-MM-DD`) or Enter
- Resample frequency (for example `1H` or `1D`, default `1D`)

**Output**
- Console stats + interactive browser plots.

### 5) `CalculateInertia.py`

**What it does**
- Finds the nearest timestamp in historical generation data.
- Models inertia from all major available generation inputs (`COAL`, `GAS`, `NUCLEAR`, `HYDRO`, `BIOMASS`, `STORAGE`, `WIND`, `WIND_EMB`, `SOLAR`, `IMPORTS`, `OTHER`).
- Computes low/best/high inertia in GVA·s from defensible default H ranges.
- Optionally compares against reported outturn/market inertia from `Inertia/*.csv`.
- Optionally calibrates the estimate to reported outturn inertia over a historical window.

**How to run (human-readable output)**
```bash
python CalculateInertia.py --timestamp "2025-01-01 00:00:00"
```

**How to run (JSON stdout)**
```bash
python CalculateInertia.py --timestamp "2025-01-01 00:00:00" --json
```

**How to run (with calibration)**
```bash
python CalculateInertia.py --timestamp "2025-01-01 00:00:00" --calibrate --calibration-days 120 --json
```

**How to write JSON to file**
```bash
python CalculateInertia.py --timestamp "2025-01-01 00:00:00" --calibrate --output-json inertia_result.json
```

**Optional H overrides**
- You can provide per-fuel low/best/high overrides using:
```bash
python CalculateInertia.py --timestamp "2025-01-01 00:00:00" --h-overrides h_overrides.json --json
```
- `h_overrides.json` format:
```json
{
	"GAS": [3.2, 4.3, 5.4],
	"WIND": [0.0, 0.35, 0.7]
}
```

### 6) `scan_statutory_frequency_breaches.py`

**What it does**
- Scans `GDA/Frequency/*.csv` for contiguous windows where frequency is outside statutory limits.
- Uses UK statutory defaults from ESQCR Regulation 27: `50 Hz ±1%` (default band `49.5` to `50.5` Hz).
- Detects both under-frequency and over-frequency windows.
- Returns breach windows with start/end/duration, min/max/worst Hz, and point counts.

**How to run (console summary)**
```bash
python scan_statutory_frequency_breaches.py
```

**Quick test on a subset of files**
```bash
python scan_statutory_frequency_breaches.py --max-files 12
```

**How to run (JSON output to stdout)**
```bash
python scan_statutory_frequency_breaches.py --json
```

**Save JSON and CSV outputs**
```bash
python scan_statutory_frequency_breaches.py --output-json statutory_breaches.json --output-csv statutory_breaches.csv
```

**Use custom thresholds (if needed)**
```bash
python scan_statutory_frequency_breaches.py --low 49.5 --high 50.5 --min-duration-seconds 1
```

**Statutory reference used**
- ESQCR 2002 Regulation 27 (declared frequency 50 Hz, permitted variation ±1%):
	- https://www.legislation.gov.uk/uksi/2002/2665/regulation/27

### 7) `find_lfdd_events.py`

**What it does**
- Scans `GDA/Frequency/*.csv` for contiguous windows where frequency is strictly below `49.00 Hz`.
- Treats each contiguous under-threshold period as an LFDD candidate event window.
- Returns event start/end, duration, point count, and severity metrics.

**How to run (console summary)**
```bash
python find_lfdd_events.py
```

**Quick test on subset of files**
```bash
python find_lfdd_events.py --max-files 12
```

**How to run (JSON stdout)**
```bash
python find_lfdd_events.py --json
```

**Save JSON and CSV outputs**
```bash
python find_lfdd_events.py --output-json lfdd_events.json --output-csv lfdd_events.csv
```

**Tune event filtering**
```bash
python find_lfdd_events.py --threshold 49.0 --min-duration-seconds 1
```

**Output includes**
- `summary`: total events, total duration, worst observed frequency, longest event
- `events`: list of windows with `start`, `end`, `duration_seconds`, `points`, `min_hz`, `max_hz`, `worst_hz`

### 8) `DeriveRoCoF.py`

**What it does**
- Computes sample-to-sample RoCoF from frequency CSV files (`Frequency/f-*.csv`).
- Keeps both sample endpoints (`sample_start_ts`, `sample_end_ts`) and writes an aligned output timestamp (`rocof_timestamp`) using `start`, `midpoint`, or `end` mode.
- Joins nearest historical generation row from `HistoricalGenerationData/df_fuel_ckan.csv` so each RoCoF point includes online generation context.

**How to run (quick subset)**
```bash
python DeriveRoCoF.py --max-files 6 --row-stride 4 --timestamp-mode midpoint --output-csv derived_rocof.csv
```

**How to run (single frequency file)**
```bash
python DeriveRoCoF.py --frequency-file ../Frequency/f-2025-1.csv --output-csv derived_rocof_2025_01.csv
```

**Useful options**
- `--timestamp-mode {start,midpoint,end}` controls alignment of RoCoF timestamps.
- `--generation-tolerance-minutes` controls nearest-match tolerance to generation timestamps.
- `--json` prints run summary as JSON.
- `--output-json` writes summary metadata to file.

**JSON output shape (example keys)**
```json
{
	"definition": {
		"event": "contiguous period where frequency is strictly below threshold",
		"threshold_hz": 49.0,
		"default_threshold_hz": 49.0
	},
	"scan_parameters": {
		"root": "C:/.../GDA",
		"files_scanned": 144,
		"min_duration_seconds": 1.0
	},
	"summary": {
		"total_events": 12,
		"total_duration_seconds": 33.0,
		"worst_hz": 48.74,
		"longest_duration_seconds": 9.0
	},
	"events": [
		{
			"start": "2025-01-08 17:46:11+00:00",
			"end": "2025-01-08 17:46:16+00:00",
			"duration_seconds": 5.0,
			"points": 6,
			"min_hz": 48.88,
			"max_hz": 48.99,
			"worst_hz": 48.88
		}
	]
}
```

## Notes

- Run commands from inside the `Random` folder.
- Scripts expect to be inside the `GDA` project structure so relative root discovery works.