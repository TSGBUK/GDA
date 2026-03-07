# GDA Installer (`Scripts/Installer.ps1`)

This README is specific to the pipeline installer in `Scripts/Installer.ps1`.

## Purpose

`Installer.ps1` orchestrates setup and CSV→Parquet pipeline operations for the GDA workspace.

It supports:
- full pipeline runs,
- resumable runs,
- validate-only runs (CSV vs Parquet freshness/completeness),
- optional parquet cleanup,
- live output filtering for noisy logs.

---

## ⚠ Runtime Warnings

- Full conversion runs can take **several hours** on large/chunked datasets.
- `-CleanupParquet` can trigger very long rebuilds because parquet outputs are removed.
- Validate-only mode is read-only but can still take significant time on large trees.

---

## Usage

From `Scripts` directory:

```powershell
.\Installer.ps1 [options]
```

### Options

- `-Help` or `--help`  
  Show help and exit.

- `-Validate` or `--validate`  
  Run **only** parquet validation (`validate_parquet_vs_csv.py`) and skip all other steps.

- `-Resume` or `--resume`  
  Resume from a saved checkpoint of completed steps.

- `-ResetResume` or `--reset-resume`  
  Clear previous resume checkpoint before starting.

- `-CleanupParquet` or `--cleanup-parquet`  
  Remove existing parquet trees in the parquet-check step.

- `-LiveOutputSuppressKeywords <string[]>`  
  Suppress matching live lines in step 7 output (e.g. `[skip]`).

---

## Full Pipeline Steps

Default run executes:

1. `pip install -r ../requirements.txt`
2. Verify/repair parquet backends (`pyarrow`, `fastparquet`)
3. `verify_setup.py`
4. `check_parquet.py` (optional cleanup)
5. `dedupe.py`
6. `split_csv.py`
7. `run_parquet_conversions.py --run --raw`

---

## Validate-only Mode

`-Validate` runs this command only:

```powershell
python validate_parquet_vs_csv.py --root .. --report parquet_validation.txt
```

Validation checks:
- every CSV has at least one matching parquet file,
- matching parquet is not older than CSV.

Exit code:
- `0` = all valid
- `1` = missing/stale parquet found

---

## Resume Behavior

Checkpoint file: `Scripts/.installer_resume.json`

- Created/updated after each successful step.
- On `-Resume`, completed steps are skipped.
- If step definitions changed, old checkpoint is ignored.
- On full success, checkpoint file is removed automatically.

---

## Examples

```powershell
# Full pipeline
.\Installer.ps1

# Resume from previous failure
.\Installer.ps1 -Resume

# Reset checkpoint then run resumable
.\Installer.ps1 -Resume -ResetResume

# Validate only
.\Installer.ps1 -Validate

# Validate only (double-dash)
.\Installer.ps1 --validate

# Full run with custom suppression keywords
.\Installer.ps1 -LiveOutputSuppressKeywords @('[skip]', 'already converted')

# Rebuild parquet from scratch (slow)
.\Installer.ps1 -CleanupParquet
```
