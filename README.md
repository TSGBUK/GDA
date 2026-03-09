
# Grid Data Analysis (GDA)

## A Workspace for Understanding the UK Electricity Grid

**GDA (Grid Data Analysis)** is an experimental research and analysis environment built around **publicly available UK electricity system data**.

The goal of this project is simple in concept but ambitious in execution:

> Given a wide collection of independent grid signals, estimate and reconstruct the **most likely operational state of the UK grid at any moment in time**.

To achieve this, GDA aggregates multiple public data sources and builds a pipeline that allows those signals to be:

* ingested
* cleaned and normalized
* merged into aligned time-series datasets
* analyzed with machine learning models
* replayed visually to explore system behaviour over time

The repository acts as a **complete workspace for grid-state analysis**, combining data engineering, machine learning, and visualization tooling.

---

# What This Project Tries To Do

The UK electricity grid is an incredibly complex system. Many different signals exist that describe its behaviour:

* system frequency
* electricity demand
* generation mix
* grid inertia
* balancing actions
* weather conditions
* interconnector flows
* reserve services
* transmission losses

Individually these signals provide **partial insight** into grid behaviour.
But when they are **combined and aligned**, they begin to reveal deeper patterns.

GDA attempts to answer questions like:

* What events precede **frequency disturbances**?
* How does **generation mix affect system inertia**?
* Do weather patterns correlate with **balancing activity**?
* What signals appear **before system stress or instability**?

In essence, the system behaves like a **very crude state estimator**:

> Given a collection of inputs, what was the *likely operational state of the grid* at time **T**?

This repository provides the tooling required to:

* process the raw data
* discover patterns
* train domain-specific machine learning models
* replay events and inspect system behaviour visually

---

# High-Level Architecture

The repository is organized around **four primary operational layers**.

## 1. Data Ingestion

Raw datasets are collected from multiple public providers, including:

* NESO
* National Grid
* GridWatch
* Open-Meteo weather archive

Each provider publishes data in different formats and time resolutions.
The ingestion layer standardizes these sources and prepares them for analysis.

---

## 2. Data Normalization and Storage

After ingestion, datasets are:

* cleaned
* validated
* normalized into consistent schemas
* converted to **Parquet** or structured CSV formats

The normalized datasets become the **data lake** used by downstream machine learning and replay systems.

This stage ensures that:

* timestamps align
* column formats are consistent
* partitions are organized for fast querying

---

## 3. Machine Learning Experts

Rather than building one large model, the project trains **domain-specific expert models**.

Each expert focuses on a particular signal family:

* frequency behaviour
* inertia patterns
* demand shifts
* generation mix changes
* balancing activity
* weather interactions

These experts can then be combined into **fusion models** that attempt to describe the **overall system state**.

---

## 4. Replay and Visualization

Once datasets and models are prepared, the system can generate **frame-based replay timelines**.

These replay frames combine multiple grid signals into a synchronized snapshot that updates **once per second**.

The replay tools allow users to:

* explore historical events
* visualize grid behaviour in real time
* inspect cross-signal relationships
* analyze disturbances and emerging patterns

Replay is supported through:

* web dashboards
* a WebSocket streaming server
* a native Android dashboard

---

# Repository Structure

The repository is organized into several major directories.

## Applications/

This directory contains **user-facing tools and dashboards**.

Examples include:

* web-based replay dashboards
* Android replay applications
* visualization utilities
* system-state viewers

These applications consume processed data and replay frames to present an interactive view of grid behaviour.

---

## DataSources/

This directory forms the **primary data lake** of the repository.

It contains:

* raw source data
* processing scripts
* normalized datasets
* Parquet storage outputs

All datasets from public providers are organized here and transformed into analysis-ready formats.

---

## MachineLearning/

This directory contains the **machine learning and analysis pipeline**.

It includes:

* feature engineering utilities
* pattern discovery tools
* domain-specific expert trainers
* system-level fusion models

The models produced here attempt to learn how different grid signals interact and how events emerge.

---

## Random/

Despite the name, this directory contains **focused analytical utilities**.

Most notably it contains tools for:

* deriving **Rate of Change of Frequency (RoCoF)**
* generating replay payloads
* creating frame-based timeline data used by the visualization systems

---

## Scripts/

This directory contains **workspace-wide utilities** used for maintenance and automation.

Examples include:

* data conversion orchestration
* schema validation tools
* dataset hygiene scripts
* environment verification utilities

---

## DataSchema.json

This file defines the **canonical schema for all core datasets**.

It specifies:

* dataset identifiers
* expected columns
* normalized data structure
* partitioning rules
* source metadata

When there is uncertainty about dataset structure, this file should be treated as the **source of truth**.

---

## DATASOURCES.md

A human-readable reference that describes the origin and purpose of each dataset included in the repository.

---

# Refactored Path Model

The repository recently underwent a **path structure refactor**.

The current canonical structure is:

```
DataSources/
Applications/
MachineLearning/
```

However, some legacy scripts or documentation may still reference older directories such as:

```
Frequency/
DemandData/
DataVisualizations/
```

If inconsistencies appear, the authoritative references are:

* this README
* `DataSchema.json`

---

# Public Data Sources

The system currently integrates several major public datasets.

---

# NESO Data

Location:

```
DataSources/NESO/
```

NESO provides the **most detailed operational datasets** used within this project.

Important datasets include:

### Balancing Services

Contains cost and activity information related to grid balancing actions such as:

* energy imbalance corrections
* frequency control services
* reserve activation
* constraint management

---

### Demand Data

Provides demand measurements and estimates including:

* system demand
* embedded generation estimates
* interconnector flows

---

### Frequency

High-resolution measurements of **system frequency in Hz**.

These datasets are critical for:

* disturbance detection
* RoCoF analysis
* stability event identification

---

### Historical Generation Data

Describes the generation mix of the grid, including:

* gas
* coal
* nuclear
* wind
* solar
* imports

Additional contextual fields include **carbon intensity** and system totals.

---

### Inertia

Datasets related to the **inertia of the power system**, which affects how the grid responds to disturbances.

These may include both measured values and market-based inertia services.

---

### Additional Operational Layers

Several other datasets provide deeper operational context, including:

* reserve availability
* reactive power services
* ancillary service dispatch instructions
* transmission losses
* auction results and capacity registers

These layers become particularly useful when **reconstructing historical grid states**.

---

# National Grid Data

Location:

```
DataSources/NationalGrid/
```

This directory synchronizes datasets from the **National Grid Connected Data Portal**.

The structure typically includes:

* raw historical pulls
* processing scripts
* merged Parquet outputs

Datasets include:

* primary grid feeds
* GSP (Grid Supply Point) feeds
* BSP power flow feeds

---

# GridWatch Data

Location:

```
DataSources/GridWatch/
```

GridWatch provides **snapshot-style grid state files** that describe generation mix and system behaviour at particular intervals.

Within this repository they are stored as **chunked CSV datasets** and converted into Parquet for efficient analysis.

---

# Weather Data

Location:

```
DataSources/Weather/
```

Weather data is derived from the **Open-Meteo archive**.

Variables include:

* temperature
* 100m wind speed
* direct solar radiation

Weather signals are essential when studying:

* renewable generation behaviour
* demand fluctuations
* grid stability under environmental conditions

---

# Data Schema and Validation

All datasets conform to definitions stored in:

```
DataSchema.json
```

The schema defines:

* dataset identifiers
* expected column sets
* normalized naming conventions
* Parquet partition structure

Typical partitioning follows a **year-based hive structure**.

Example dataset identifiers include:

* BalancingServices
* DemandData
* Frequency
* GridwatchData
* HistoricalGenerationData
* Inertia
* Weather

To verify datasets against the schema, run:

```bash
python Scripts/validate_data_schema.py
```

This tool detects:

* missing columns
* schema drift
* incorrect dataset structure

---

# Data Processing Utilities

The repository includes several scripts that help manage data pipelines.

## Parquet Conversion

To convert raw datasets into Parquet format:

```bash
python Scripts/run_parquet_conversions.py . --run
```

This script automatically discovers all conversion scripts and runs them in batch.

---

## Schema Validation

```bash
python Scripts/validate_data_schema.py
```

Ensures datasets match expected schema definitions.

---

## Environment Verification

```bash
python Scripts/verify_setup.py
```

Checks:

* Python dependencies
* optional GPU support
* required packages

---

## Dataset Management Tools

Additional utilities include:

* CSV splitting for large files
* Parquet dataset checks
* deduplication tools
* CSV vs Parquet validation checks

These scripts help maintain dataset consistency across the workspace.

---

# Machine Learning Architecture

The machine learning system is located in:

```
MachineLearning/
```

It consists of three major components.

---

## ML Pipeline

`ml_pipeline.py` handles:

* loading multiple datasets
* aligning timestamps
* building feature sets
* preparing data for model training

---

## Pattern Discovery

The `Patternator/` module explores datasets to surface potential:

* instability signals
* emergent patterns
* unusual behaviour

This stage is primarily exploratory and used to generate hypotheses.

---

## Expert Models

The `Experts/` directory contains **domain-specific trainers**.

Examples include:

* frequency behaviour models
* inertia behaviour models
* demand pattern models
* balancing activity models
* generation mix models
* weather interaction models

There are also **fusion experts** that combine signals across domains.

The most comprehensive trainer is the **system state fusion model**, which attempts to integrate multiple experts into a unified representation.

Pre-trained models are stored in:

```
MachineLearning/Experts/pre-trained-experts/
```

---

# Replay System

One of the most powerful parts of this repository is the **grid replay system**.

Replay allows historical grid behaviour to be reconstructed as **frame-based timelines**.

Each frame represents a snapshot of:

* frequency
* generation
* demand
* weather
* inertia
* balancing activity

These frames can then be streamed and visualized in multiple applications.

---

# Web Replay Server

Location:

```
Applications/RoCoF-App/
```

This component runs a **FastAPI + WebSocket server**.

It:

* merges multiple datasets
* builds runtime grid snapshots
* streams frames at **1 second resolution**

This allows high-fidelity playback of historical events.

---

# Web Replay Dashboard

Location:

```
Applications/RoCoF-Reply/
```

A browser-based dashboard that:

* loads replay JSON payloads
* renders timeline views
* displays charts and system-state cards

---

# Android Replay App

Location:

```
Applications/RoCoFAndroid/
```

A native Android dashboard built using **Jetpack Compose**.

The app can:

* load replay JSON files
* display system metrics
* provide portable field analysis tools

---

# Replay Frame Generation

Replay payloads are created using:

```
Random/DeriveRoCoF.py
```

This script:

* calculates **Rate of Change of Frequency**
* aligns generation and demand context
* produces replay frames used by visualization tools

---

# Quick Start

## 1. Install Dependencies

Create a Python environment and install dependencies:

```bash
pip install -r requirements.txt
```

---

## 2. Convert Raw Data

Run all dataset conversion scripts:

```bash
python Scripts/run_parquet_conversions.py . --run
```

---

## 3. Validate Dataset Schema

```bash
python Scripts/validate_data_schema.py
```

---

## 4. Start Machine Learning Analysis

Open the analysis notebook:

```
MachineLearning/ml_analysis.ipynb
```

Or run expert trainers directly.

---

## 5. Generate Replay Data

Run:

```
Random/DeriveRoCoF.py
```

Then start the replay server or dashboards.

---

# Current State of the Repository

The repository is currently undergoing structural improvements.

Recent changes include:

* refactoring data paths
* centralizing schema definitions
* improving dataset normalization

Some scripts may still reference older directory structures.
The authoritative definitions remain:

* `DataSchema.json`
* the directory layout described in this README.

---

# Why This Project Exists

This repository exists to transform public grid data into something much more useful:

* structured time-series datasets
* system-state reconstruction tools
* visual replay environments
* machine learning models capable of identifying emerging grid behaviour

For researchers, engineers, or analysts interested in **grid stability, operational dynamics, or energy system behaviour**, this workspace provides the tooling needed to explore those questions in depth.

---
