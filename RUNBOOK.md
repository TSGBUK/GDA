# GDA Operator Runbook

This runbook is for daily use.

It is intentionally command-first and task-oriented, so you can run the platform without digging through implementation details.

For architecture and repository deep context, see README.md.

## 0) Quick Navigation

- Standard daily flow: section 1
- First-time setup: section 2
- Data conversion and validation: section 3
- ML expert training: section 4
- Replay systems: section 5
- Troubleshooting and recovery: section 6
- Operational checklists: section 7

## 1) Standard Daily Flow (15-30 minutes)

From repository root:

1. Activate environment
2. Verify dependencies
3. Run incremental conversion pass
4. Validate schema/freshness
5. Train or infer models as needed
6. Launch replay tools

### 1.1 Windows PowerShell quick path

```powershell
# from repo root
.\.venv\Scripts\Activate.ps1
python Scripts\verify_setup.py
python Scripts\run_parquet_conversions.py . --run
python Scripts\validate_data_schema.py
python Scripts\validate_parquet_vs_csv.py --root . --report parquet_validation.txt
```

### 1.2 Linux quick path

```bash
source .venv/bin/activate
python Scripts/verify_setup.py
python Scripts/run_parquet_conversions.py . --run
python Scripts/validate_data_schema.py
python Scripts/validate_parquet_vs_csv.py --root . --report parquet_validation.txt
```

## 2) First-Time Setup

## 2.1 Minimal local venv (recommended default)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python Scripts\verify_setup.py
```

## 2.2 Installer-driven setup (Windows)

From Scripts folder:

```powershell
cd Scripts
.\Installer.ps1 -Help
.\Installer.ps1
```

Useful installer modes:

```powershell
# validate-only (read-only)
.\Installer.ps1 -Validate

# resume from prior failure
.\Installer.ps1 -Resume

# reset resume state and run
.\Installer.ps1 -Resume -ResetResume

# full rebuild of parquet trees (can be slow)
.\Installer.ps1 -CleanupParquet
```

## 2.3 Ubuntu + conda/RAPIDS path

```bash
bash setup.sh
source activate_env.sh
python Scripts/verify_setup.py
```

Notes:
- setup.sh attempts GPU/RAPIDS install, then falls back to CPU-only if incompatible.
- This path is best for large model training runs.

## 3) Data Operations

## 3.1 Discover conversion scripts

```powershell
python Scripts\run_parquet_conversions.py .
```

## 3.2 Run all conversion scripts

```powershell
python Scripts\run_parquet_conversions.py . --run
```

If you need raw child output:

```powershell
python Scripts\run_parquet_conversions.py . --run --raw
```

## 3.3 Validate schema contract

```powershell
python Scripts\validate_data_schema.py
```

Strict mode where parquet must exist:

```powershell
python Scripts\validate_data_schema.py --require-parquet
```

## 3.4 Validate CSV to parquet freshness

```powershell
python Scripts\validate_parquet_vs_csv.py --root . --report parquet_validation.txt
```

Interpretation:
- missing means no parquet output found for a CSV
- stale means parquet exists but is older than source CSV

## 3.5 Data hygiene utilities

```powershell
# find/optionally clean parquet directories
python Scripts\check_parquet.py

# split oversized CSV files
python Scripts\split_csv.py DataSources\GridWatch\gridwatch_chunk_001.csv --size 50

# estimate CSV volume
python Scripts\csv_totals.py . --top 25

# normalize DataSchema from discovered headers
python Scripts\normalize_data_schema.py
```

## 4) ML Operations

All commands below assume repo root as current directory.

## 4.1 Start with one baseline expert

```powershell
python MachineLearning\Experts\train_frequency_expert.py --device auto
```

## 4.2 Train core experts

```powershell
python MachineLearning\Experts\train_generation_expert.py --device auto
python MachineLearning\Experts\train_weather_expert.py --device auto
python MachineLearning\Experts\train_demand_expert.py --device auto
python MachineLearning\Experts\train_inertia_expert.py --device auto
python MachineLearning\Experts\train_balancing_expert.py --device auto
python MachineLearning\Experts\train_gridwatch_expert.py --device auto
```

## 4.3 Train fusion experts

```powershell
python MachineLearning\Experts\train_weather_generation_expert.py --device auto
python MachineLearning\Experts\train_weather_inertia_expert.py --device auto
```

## 4.4 Train large system-state model

```powershell
python MachineLearning\Experts\train_system_state_monster.py --rounds 20 --base-estimators 500 --resolution 1S --join-tolerance 35m
```

## 4.5 Run monster inference

```powershell
python MachineLearning\Experts\run_monster_inference.py
```

Outputs are written under MachineLearning/Experts/pre-trained-experts/.

## 5) Replay Operations

## 5.1 Generate replay JSON from frequency data

```powershell
python Random\DeriveRoCoF.py --max-files 6 --row-stride 4 --timestamp-mode midpoint --output-csv Random\derived_rocof_sample.csv --output-replay-json Applications\RoCoF-Reply\derived_rocof_replay.json
```

## 5.2 Browser dashboard replay

Open Applications/RoCoF-Reply/index.html and load Applications/RoCoF-Reply/derived_rocof_replay.json.

## 5.3 WebSocket streaming replay server

```powershell
python Applications\RoCoF-App\server.py
```

Endpoints:
- http://127.0.0.1:8765/
- ws://127.0.0.1:8765/ws/replay

## 5.4 Android replay app

Open Applications/RoCoFAndroid in Android Studio and run on emulator or device.

## 6) Troubleshooting

## 6.1 Command not found: rg

This repo can be operated without rg.
Use PowerShell alternatives:

```powershell
Get-ChildItem -Path DataSources -Recurse -Filter parquet_data_conversion.py | ForEach-Object { $_.FullName }
```

## 6.2 Missing parquet backend

Symptoms:
- conversion script warns no pyarrow.parquet and fastparquet

Fix:

```powershell
pip install pyarrow
# or
pip install fastparquet
```

Re-run:

```powershell
python Scripts\run_parquet_conversions.py . --run
```

## 6.3 Schema validation failures

Checklist:
1. Confirm conversion scripts completed for affected dataset
2. Re-run validate_data_schema.py
3. Inspect DataSchema.json columns vs real headers
4. If upstream header changed, run normalize_data_schema.py and review diff

## 6.4 Parquet stale after CSV update

Run targeted conversion for dataset or full conversion pass again.
Then re-run validate_parquet_vs_csv.py.

## 6.5 GPU requested but unavailable

Use CPU fallback:

```powershell
python MachineLearning\Experts\train_frequency_expert.py --device cpu
```

If using conda RAPIDS path, verify with:

```powershell
python Scripts\verify_setup.py
```

## 6.6 DeriveRoCoF root detection issue

DeriveRoCoF expects a parent folder named GDA.
If root folder name differs in case or name, either:
- run from the expected repository layout, or
- patch root detection in Random/DeriveRoCoF.py for case-insensitive matching.

## 6.7 Installer resume state confusion

Resume state file:
- Scripts/.installer_resume.json

To reset:

```powershell
cd Scripts
.\Installer.ps1 -Resume -ResetResume
```

## 7) Operational Checklists

## 7.1 Pre-training checklist

- Environment activated
- verify_setup.py passes core dependencies
- parquet conversion completed for required datasets
- schema/freshness checks clean or understood
- enough disk space for artifacts and logs

## 7.2 Pre-replay checklist

- Frequency + generation parquet/csv sources present
- DeriveRoCoF output generated recently
- dashboard JSON path points to latest replay file
- RoCoF-App server starts with no import/path errors

## 7.3 Pre-commit checklist (docs/data scripts)

- README and RUNBOOK commands still valid
- scripts referenced actually exist
- no accidental edits in generated outputs
- include report artifacts only when intentionally tracked

## 8) Useful Paths

- Root docs: README.md, RUNBOOK.md, DATASOURCES.md, DataSchema.json
- Utilities: Scripts/
- Data lake: DataSources/
- ML: MachineLearning/
- Replay apps: Applications/
- Focused analytics: Random/

## 9) Safe Defaults

When uncertain, use these defaults:

```powershell
python Scripts\verify_setup.py
python Scripts\run_parquet_conversions.py . --run
python Scripts\validate_data_schema.py
python Scripts\validate_parquet_vs_csv.py --root . --report parquet_validation.txt
python MachineLearning\Experts\train_frequency_expert.py --device auto
python Random\DeriveRoCoF.py --max-files 4 --row-stride 4 --timestamp-mode midpoint --output-csv Random\derived_rocof_sample.csv
```

This sequence gives a stable baseline for most day-to-day development and analysis.
