# Grid Data Analysis (GDA)

Comprehensive UK electricity grid data engineering, analytics, machine learning, and replay workspace.

This repository brings together public UK grid datasets, converts them into analysis-ready forms, trains domain-specific ML experts, and supports replay dashboards (web and Android) for event inspection.

For day-to-day operations and command-first procedures, see RUNBOOK.md.

## What This Project Is

GDA is a full-stack research workspace for understanding system behavior in the UK electricity network by combining many independent data streams into a time-aligned operational view.

Primary goals:
- Ingest and normalize heterogeneous public datasets.
- Convert large CSV datasets into partitioned parquet for fast query and training.
- Build domain experts for frequency, demand, generation, inertia, balancing, weather, and fused system state.
- Replay historical operating periods with synchronized telemetry panels and event indicators.

## What You Can Do With It

- Build a local data lake from NESO, National Grid, GridWatch, weather, and related sources.
- Validate schema drift against a declared contract in DataSchema.json.
- Run utility pipelines for dedupe, CSV chunking, conversion orchestration, and setup verification.
- Train multiple expert models and fused models (including a large system-state trainer).
- Serve live replay snapshots over WebSocket from parquet-backed data.
- Open web dashboards or Android UI for visual analysis.

## Current Repository Layout (Canonical)

- Data root: DataSources/
- Application root: Applications/
- ML root: MachineLearning/
- Utilities root: Scripts/
- Focused analysis scripts: Random/

Legacy docs may still mention older roots such as Frequency/, DemandData/, or DataVisualizations/. Use the structure above plus DataSchema.json as source-of-truth.

## High-Level Architecture

1. Source ingestion and storage
- Raw CSV and source snapshots are kept under DataSources/ by provider and dataset.

2. Dataset normalization and parquet conversion
- Dataset-specific parquet_data_conversion.py scripts convert source files into parquet, usually hive partitioned by year.

3. Data validation and quality checks
- Schema and freshness checks are performed with Scripts/validate_data_schema.py and Scripts/validate_parquet_vs_csv.py.

4. Feature loading and model training
- MachineLearning/ml_pipeline.py provides multi-source loaders and merge helpers.
- MachineLearning/Experts/ contains expert trainers and fusion trainers.

5. Replay and visualization
- Applications/RoCoF-App/server.py streams synchronized snapshots via FastAPI + WebSocket.
- Applications/RoCoF-Reply is a browser replay dashboard.
- Applications/RoCoFAndroid is a native Android replay client.

## Top-Level Directory Guide

### Applications/

- MasterProjectSite/
  - Static site pages and assets.
- RoCoF-App/
  - FastAPI replay server reading parquet sources and streaming frames.
- RoCoF-Reply/
  - Browser replay dashboard loading replay JSON and rendering charts/cards.
- RoCoFAndroid/
  - Jetpack Compose Android dashboard for replay JSON.
- RoCoF-App-2/
  - Placeholder/experimental area.

### DataSources/

Data lake by provider and dataset. Includes raw files, Processors/, and Parquet outputs where available.

Key provider trees:
- DataSources/NESO/
- DataSources/NationalGrid/
- DataSources/GridWatch/
- DataSources/Weather/
- DataSources/UkPowerNetworks/

### MachineLearning/

- ml_pipeline.py: reusable loaders, merge helpers, datetime features, frequency event labels.
- Patternator/: exploratory pattern surfacing against frequency-centered windows.
- Experts/: training scripts for dataset experts and fusion models.

### Scripts/

Workspace orchestration and hygiene:
- run_parquet_conversions.py
- validate_data_schema.py
- validate_parquet_vs_csv.py
- verify_setup.py
- dedupe.py
- split_csv.py
- csv_totals.py
- normalize_data_schema.py
- check_parquet.py
- Installer.ps1
- Installer.py

### Random/

Focused analysis tools:
- DeriveRoCoF.py
- CalculateInertia.py
- find_lfdd_events.py
- scan_statutory_frequency_breaches.py
- solar/wind analysis scripts

## Data Sources and Datasets

Reference files:
- DATASOURCES.md: human summary.
- DataSchema.json: machine-readable source/schema/storage contract.

DataSchema.json includes:
- provider source metadata and URLs,
- local roots and dataset IDs,
- expected raw/parquet columns,
- partitioning conventions.

Core declared dataset IDs include:
- BalancingServices
- BSAD_AggregatedData
- BSAD_DissAggregatedData
- BSAD_ForwardContracts
- CarbonIntensityOfBalancingActions
- DemandData
- EACEnduringAuctionCapability
- EC-BR_AuctionResults
- Frequency
- HistoricalGenerationData
- Inertia
- InertiaCosts
- NonBM_AncillaryServiceDispatchPlatformInstructions
- NonBM_AncillaryServiceDispatchPlatformWindowPrices
- OBP_NonBMPhysicalNotifications
- OBP_ReserveAvailability
- ORPS_ReactivePowerService
- TransmissionLosses
- NationalGrid_LivePrimary_All
- NationalGrid_LiveGSP_All
- NationalGrid_BSP_All
- GridwatchData
- Weather

## Parquet Conversion Coverage

Discovered conversion scripts include:
- DataSources/GridWatch/Processors/parquet_data_conversion.py
- DataSources/NationalGrid/Processors/parquet_data_conversion.py
- DataSources/NESO/*/Processors/parquet_data_conversion.py for major NESO families
- DataSources/UkPowerNetworks/Processors/parquet_data_conversion.py
- DataSources/Weather/parquet_data_conversion.py
- DataSources/Weather/Processors/parquet_data_conversion.py

Use the orchestration script to discover and run conversion scripts rather than invoking each manually.

## Environment Setup

You can run this project in multiple ways depending on platform and GPU requirements.

### Option A: Minimal Python setup (recommended for most users)

From repository root:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# Linux/macOS
# source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Option B: Ubuntu + conda + RAPIDS path

Use setup.sh for broader environment provisioning, optional GPU stack, and helper activation script:

```bash
bash setup.sh
source activate_env.sh
python Scripts/verify_setup.py
```

Notes:
- setup.sh attempts RAPIDS install first and falls back to CPU-only conda env when needed.
- setup.sh also installs optional notebook and ML tooling.

### Option C: Installer-driven pipeline execution

- Windows PowerShell flow: Scripts/Installer.ps1
- Python equivalent flow: Scripts/Installer.py

These automate dependency checks and conversion stages with resume/validate modes.

## Quick Start Workflow

From repository root:

1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Verify environment
```bash
python Scripts/verify_setup.py
```

3. Run parquet conversions
```bash
python Scripts/run_parquet_conversions.py . --run
```

4. Validate schema contract
```bash
python Scripts/validate_data_schema.py
```

5. Validate parquet freshness/completeness
```bash
python Scripts/validate_parquet_vs_csv.py --root . --report parquet_validation.txt
```

6. Start ML analysis or expert training
```bash
python MachineLearning/Experts/train_frequency_expert.py --device auto
```

7. Start replay server (if parquet data is available)
```bash
python Applications/RoCoF-App/server.py
```

Open:
- http://127.0.0.1:8765/
- ws://127.0.0.1:8765/ws/replay

## Scripts Directory Deep Reference

### run_parquet_conversions.py

Purpose:
- Recursively discover parquet_data_conversion.py scripts and run them with selected Python interpreter.

Useful flags:
- --run: execute discovered scripts
- --raw: raw output mode
- --all: include scripts outside Processors folders
- --python: explicit Python executable
- --allow-missing-backend: skip run if parquet backend unavailable

Examples:
```bash
python Scripts/run_parquet_conversions.py
python Scripts/run_parquet_conversions.py . --run
python Scripts/run_parquet_conversions.py . --run --raw
python Scripts/run_parquet_conversions.py . --run --report parq_run.txt
```

### validate_data_schema.py

Purpose:
- Compare real dataset headers/columns with DataSchema.json declarations.

Examples:
```bash
python Scripts/validate_data_schema.py
python Scripts/validate_data_schema.py --require-parquet
```

### validate_parquet_vs_csv.py

Purpose:
- Ensure each CSV has corresponding parquet output and parquet is not older than CSV.

Examples:
```bash
python Scripts/validate_parquet_vs_csv.py --root .
python Scripts/validate_parquet_vs_csv.py --root . --report parquet_validation.txt
```

### verify_setup.py

Purpose:
- Check core packages, optional packages, GPU/CUDA visibility, and run a basic functionality probe.

Example:
```bash
python Scripts/verify_setup.py
```

### Other utility scripts

- check_parquet.py: locate and optionally clean Parquet directories.
- csv_totals.py: recursively count rows/datapoints and write JSON summary.
- dedupe.py: data deduplication utility.
- split_csv.py: split large CSV files into chunks.
- normalize_data_schema.py: normalize DataSchema.json structure from discovered headers.

## Machine Learning Pipeline

### ml_pipeline.py

Provides:
- load_generation
- load_weather
- load_frequency
- load_inertia
- load_demand
- load_balancing
- load_gridwatch
- merge_all
- add_datetime_features
- label_frequency_events

Environment override:
- Set TSGB_DATA_PATH to override base root used by loaders.

### Patternator

- MachineLearning/Patternator/patternator.py runs frequency-centered pattern surfacing and feature/correlation outputs.

Example:
```bash
python MachineLearning/Patternator/patternator.py --max-frequency-files 12 --row-stride 5 --resample 1H
```

## Expert Trainers

All trainer scripts are in MachineLearning/Experts/.
Most output model + metrics files under MachineLearning/Experts/pre-trained-experts/.

Common trainer options (varies by script):
- --device {auto,cpu,cuda}
- --n-estimators
- --train-fraction
- dataset-specific path overrides

Primary trainers:
- train_frequency_expert.py
- train_inertia_expert.py
- train_demand_expert.py
- train_balancing_expert.py
- train_generation_expert.py
- train_gridwatch_expert.py
- train_weather_expert.py
- train_weather_generation_expert.py
- train_weather_inertia_expert.py

System fusion trainer:
- train_system_state_monster.py

Ensemble inference:
- run_monster_inference.py

Batch orchestrator:
- run_all_experts.sh (Linux/conda oriented)

Example commands:
```bash
python MachineLearning/Experts/train_weather_expert.py --device auto
python MachineLearning/Experts/train_generation_expert.py --device auto
python MachineLearning/Experts/train_system_state_monster.py --rounds 20 --base-estimators 500 --resolution 1S --join-tolerance 35m
python MachineLearning/Experts/run_monster_inference.py
```

## Replay and Visualization

### WebSocket replay server: Applications/RoCoF-App/server.py

- Uses FastAPI.
- Loads parquet tables for multiple data families.
- Streams synchronized replay snapshots.
- Serves static app assets and supports WebSocket replay endpoint.

Run:
```bash
python Applications/RoCoF-App/server.py
```

### Browser replay dashboard: Applications/RoCoF-Reply/

- Loads replay JSON (default: derived_rocof_replay.json).
- Renders cards, gauges, and timeline charts for RoCoF/frequency/flow/inertia context.

Typical flow:
1. Generate replay JSON using Random/DeriveRoCoF.py.
2. Open Applications/RoCoF-Reply/index.html in browser.
3. Load generated JSON if not auto-loaded.

### Android replay app: Applications/RoCoFAndroid/

- Native Jetpack Compose dashboard.
- Reads replay JSON from storage picker or assets.

Run in Android Studio:
1. Open Applications/RoCoFAndroid.
2. Allow Gradle sync.
3. Run on emulator or device.

## Random Analysis Scripts

### DeriveRoCoF.py

Purpose:
- Compute sample-to-sample RoCoF from frequency CSVs.
- Align to nearest generation and demand context.
- Export CSV and optional replay JSON payload.

Example:
```bash
python Random/DeriveRoCoF.py --max-files 6 --row-stride 4 --timestamp-mode midpoint --output-csv Random/derived_rocof_sample.csv --output-replay-json Applications/RoCoF-Reply/derived_rocof_replay.json
```

### CalculateInertia.py

Purpose:
- Estimate inertia at timestamp using fuel mix and configurable H constants.
- Optional calibration against reported inertia data.

### scan_statutory_frequency_breaches.py

Purpose:
- Find contiguous frequency windows outside statutory limits.

### find_lfdd_events.py

Purpose:
- Identify contiguous frequency windows below LFDD threshold.

### solar and wind scripts

- solar_analysis_simple.py
- solar_generation_analysis.py
- wind_analysis_simple.py
- wind_speed_generation_analysis.py

These scripts quantify weather-to-generation relationships with console stats and optional interactive charts.

## Windows-Specific Notes

- PowerShell terminal is supported for pipeline operations.
- If script execution is blocked, set policy for current user/session as needed.
- Installer.ps1 includes help, validate-only mode, cleanup mode, and resume checkpoint support.

Example:
```powershell
cd Scripts
.\Installer.ps1 -Help
.\Installer.ps1 -Validate
.\Installer.ps1 -Resume
```

## Known Caveats

- Some legacy docs reference pre-refactor paths. Prefer DataSources/, Applications/, and MachineLearning/.
- Random/DeriveRoCoF.py currently searches parents for folder name GDA exactly. If your root folder name differs by case (for example gda), run from expected path naming or patch that function for case-insensitive root detection.
- Large conversion and training runs can take substantial time and disk IO.
- GPU acceleration requires matching CUDA, drivers, and RAPIDS package compatibility.

## Suggested End-to-End Development Flow

1. Install environment and dependencies.
2. Verify setup with Scripts/verify_setup.py.
3. Convert datasets to parquet via Scripts/run_parquet_conversions.py.
4. Run schema and freshness checks.
5. Explore data in MachineLearning/ml_analysis.ipynb or scripts.
6. Train experts in MachineLearning/Experts/.
7. Generate replay payloads from Random/DeriveRoCoF.py.
8. Inspect in web or Android replay app.

## Related Files

- README.md.old: previous root README version.
- DATASOURCES.md: quick source list.
- DataSchema.json: schema contract.
- Scripts/INSTALLER_README.md: Installer.ps1 usage and warnings.
- requirements.txt: Python dependencies.
- setup.sh: Ubuntu/conda/RAPIDS bootstrap.
- setup.py: Python package metadata and entrypoints.

## License

MIT (see package metadata). If introducing external datasets, comply with upstream source terms and acceptable usage.
