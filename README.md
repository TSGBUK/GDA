# GDA (Grid Data Analysis)

GDA is a UK grid intelligence workspace built on **real public electricity-system data**.

It combines:
- multi-source grid telemetry and market datasets,
- conversion + validation pipelines,
- ML expert training for pattern/event discovery,
- replay tooling (web and Android) for high-resolution system-state playback.

The core intent is to detect patterns, emerging events, and system-state shifts across frequency, demand, generation mix, inertia, balancing activity, and weather drivers.

---

## Repository Focus

This repository is organized around four operational layers:

1. **Data ingestion + normalization** from public providers (NESO, National Grid, GridWatch, Open-Meteo).
2. **Storage harmonization** into schema-aware CSV/Parquet structures.
3. **ML experts** trained per-domain plus cross-domain fusion models.
4. **Replay systems** that merge signals into frame-based timelines and update at 1-second cadence for detailed playback.

---

## Refactored Path Model (Important)

Canonical data location is now:

- `DataSources/...`

Application/tooling location is now:

- `Applications/...`

Machine learning location:

- `MachineLearning/...`

Some legacy docs and scripts still reference older root-level paths such as `Frequency/...`, `DemandData/...`, `DataVisualizations/...`, etc.  
When in doubt, use this README + `DataSchema.json` as the canonical map.

---

## Top-Level Structure

- `Applications/`  
  Web dashboards, replay apps, Android app, and site assets.

- `DataSources/`  
  Primary data lake for public-source raw files, processors, and parquet outputs.

- `MachineLearning/`  
  Pipeline utilities, notebooks, pattern surfacing, and expert trainers.

- `Random/`  
  Focused analysis and derivation tools (including RoCoF derivation/replay payload generation).

- `Scripts/`  
  Workspace-wide utilities (conversion orchestration, schema validation, setup verification, CSV splitting).

- `DataSchema.json`  
  Structured schema and source contract for core datasets.

- `DATASOURCES.md`  
  Human-readable source reference summary.

---

## Public Data Sources and What They Contain

### 1) NESO (`DataSources/NESO/`)

Primary operational datasets used for analytics and ML training:

- `BalancingServices/`  
  Balancing service cost categories (energy imbalance, frequency control, reserve, constraints, etc.).

- `DemandData/`  
  Settlement-period demand and embedded generation estimates, plus interconnector flows.

- `Frequency/`  
  High-frequency system frequency measurements (`Hz`) used for RoCoF/event analysis.

- `HistoricalGenerationData/`  
  Fuel mix and system generation timeline (gas, coal, nuclear, wind, solar, imports, carbon intensity, etc.).

- `Inertia/` and `InertiaCosts/`  
  Market-provided and outturn inertia fields (and associated related costs where available).

- `BSAD_AggregatedData/`, `BSAD_DissAggregatedData/`, `BSAD_ForwardContracts/`  
  Balancing services adjustment and forward contract datasets.

- `CarbonIntensityOfBalancingActions/`  
  Carbon impact context around balancing actions.

- `NonBM_AncillaryServiceDispatchPlatformInstructions/` and `NonBM_AncillaryServiceDispatchPlatformWindowPrices/`  
  Non-BM ancillary dispatch records and pricing windows.

- `OBP_NonBMPhysicalNotifications/`, `OBP_ReserveAvailability/`, `ORPS_ReactivePowerService/`, `TransmissionLosses/`, `EACEnduringAuctionCapability/`, `EC-BR_AuctionResults/`, `TransmissionEntryCpacity_TECRegister/`  
  Additional operational/market layers used in merged replay context and advanced modeling.

### 2) National Grid (`DataSources/NationalGrid/`)

Connected Data Portal synchronization with:

- `history/` raw historical pulls,
- `Processors/` transformation scripts,
- `Parquet/` merged parquet outputs.

Schema tracks merged families such as:
- Live Primary feeds,
- Live GSP feeds,
- BSP power flow feeds.

### 3) GridWatch (`DataSources/GridWatch/`)

Snapshot-style grid state files (chunked CSVs in this workspace), with processors for parquet conversion and downstream learning.

### 4) Weather (`DataSources/Weather/`)

Open-Meteo archive-derived weather timeline (UTC), including:
- temperature,
- 100m wind speed,
- direct solar radiation.

Includes raw CSV, parquet conversion, and query tooling.

---

## Data Contracts and Schema

`DataSchema.json` defines:

- source metadata and URLs,
- dataset identifiers,
- expected raw columns,
- normalized parquet conventions,
- partitioning expectations (typically hive partitioning by `year`).

Core schema dataset IDs include:

- `BalancingServices`
- `DemandData`
- `Frequency`
- `GridwatchData`
- `HistoricalGenerationData`
- `Inertia`
- `Weather`
- `NationalGrid_LivePrimary_All`
- `NationalGrid_LiveGSP_All`
- `NationalGrid_BSP_All`

Use `Scripts/validate_data_schema.py` to detect schema drift between actual files and declared expectations.

---

## Processing and Utility Tooling

### Conversion orchestration

- `Scripts/run_parquet_conversions.py`  
  Discovers all `parquet_data_conversion.py` scripts and optionally executes them in batch.

### Schema checks

- `Scripts/validate_data_schema.py`  
  Validates raw/parquet shapes against `DataSchema.json`.

### Environment verification

- `Scripts/verify_setup.py`  
  Verifies Python/data/optional GPU dependencies.

### Data management helpers

- `Scripts/split_csv.py` for large CSV chunking.
- `Scripts/check_parquet.py` for parquet folder scans/cleanup.
- `Scripts/dedupe.py`, `Scripts/validate_parquet_vs_csv.py` for hygiene and consistency checks.

---

## Machine Learning: Experts + Pattern Discovery

`MachineLearning/` contains three major capabilities:

1. **Pipeline loading + feature engineering** (`ml_pipeline.py`) for multi-dataset alignment.
2. **Pattern surfacing** (`Patternator/`) for exploratory instability/pattern signals.
3. **Expert trainers** (`MachineLearning/Experts/`) for dataset-specific and fused models.

### Expert trainers

`MachineLearning/Experts/` includes trainers such as:

- `train_frequency_expert.py`
- `train_inertia_expert.py`
- `train_demand_expert.py`
- `train_balancing_expert.py`
- `train_generation_expert.py`
- `train_gridwatch_expert.py`
- `train_weather_expert.py`
- fusion experts (`train_weather_generation_expert.py`, `train_weather_inertia_expert.py`)
- system-level fusion model (`train_system_state_monster.py`)

Artifacts are stored under:

- `MachineLearning/Experts/pre-trained-experts/`

The ML focus is not generic benchmark modeling; it is **domain-aware “expert” behavior modeling** for event context, anomaly surfacing, and emergent state identification.

---

## Replay Tooling (Web + Android)

Replay stack lives under `Applications/`.

### `Applications/RoCoF-App/` (web streaming)

- FastAPI + WebSocket replay server (`server.py`).
- Merges multiple dataset layers into runtime snapshots.
- Streams frame updates with 1-second cadence for detailed playback.
- Intended for high-fidelity replay and interactive dashboard inspection.

### `Applications/RoCoF-Reply/` (web replay UI)

- HTML/JS replay dashboard for frame-based RoCoF payloads.
- Consumes derived replay JSON and renders timeline/charts/cards.

### `Applications/RoCoFAndroid/` (native Android)

- Jetpack Compose dashboard for replay JSON playback on mobile.
- Loads replay JSON from device storage or bundled assets.
- Provides native real-time cards/charts for field/portable analysis.

### Replay payload generation

- `Random/DeriveRoCoF.py` computes sample-to-sample RoCoF, aligns generation/demand context, and builds replay payload frames.

---

## Quick Start

## 1) Python environment

You can use either:

- local virtual environment (`.venv`) on Windows, or
- Ubuntu setup script (`setup.sh`) for full stack provisioning (including optional RAPIDS path).

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## 2) Build parquet datasets

From repository root:

```bash
python Scripts/run_parquet_conversions.py . --run
```

## 3) Validate schema consistency

```bash
python Scripts/validate_data_schema.py
```

## 4) Start ML workflow

- Open `MachineLearning/ml_analysis.ipynb`, or
- run expert trainers in `MachineLearning/Experts/`.

## 5) Use replay tooling

- Generate replay frames with `Random/DeriveRoCoF.py`.
- Launch web replay from `Applications/RoCoF-App/` or `Applications/RoCoF-Reply/`.
- Open `Applications/RoCoFAndroid/` in Android Studio for native mobile replay.

---

## Current State Notes

- This repository has undergone path refactoring; canonical data paths are under `DataSources/`.
- Some internal scripts/docs still use legacy path assumptions and are being normalized.
- For authoritative structure and column expectations, treat `DataSchema.json` as source-of-truth.

---

## Why this repository exists

This project is designed to turn public UK grid data into:

- analyzable, mergeable time-series context,
- event-centric replay capability,
- and ML expert systems that can detect subtle, emerging grid behaviors.

If your goal is to study stability, operational transitions, and cross-signal causality in detail, this repo is built for exactly that.
