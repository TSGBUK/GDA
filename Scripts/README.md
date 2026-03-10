# Scripts Directory

This folder contains utility scripts used by the TSGB project for data
management, processing, and environment verification.

## `check_parquet.py`

A small command-line tool for locating and optionally removing directories
called `Parquet` under a given root.  Its purpose is to clean up leftover
parquet data folders.

### Usage

```sh
# scan the default root (a folder named DOCS under the current working directory)
python Scripts/check_parquet.py

# specify a different root path explicitly
python Scripts/check_parquet.py /path/to/DOCS

# write the list of matching directories to a report file
python Scripts/check_parquet.py --report empty_parquets.txt

# delete every 'Parquet' directory tree under the root
python Scripts/check_parquet.py --cleanup

# combine reporting and cleanup if desired
python Scripts/check_parquet.py --report removed.txt --cleanup
```

### Notes

- The search is case-insensitive; folders named `Parquet` or `parquet`
  (or any casing variation) are matched.
- The script walks the entire tree starting from the root, so use it with
  care if running `--cleanup` in a large directory.
- Additional utilities may be added to this directory in the future.

## `run_parquet_conversions.py`

This helper searches for files named `parquet_data_conversion.py` beneath a
specified root and can optionally execute them using the same interpreter.
It simplifies discovering and running conversion scripts that might be
scattered throughout the data tree.

### Usage

```sh
# list all conversion helpers under the default DOCS root
python Scripts/run_parquet_conversions.py

# specify a different directory to scan
python Scripts/run_parquet_conversions.py /path/to/DOCS

# save the list to a report file
python Scripts/run_parquet_conversions.py --report found.txt

# execute each discovered script
python Scripts/run_parquet_conversions.py --run

# combine reporting and running
python Scripts/run_parquet_conversions.py --report log.txt --run
```

### Notes

- The script only matches files named exactly `parquet_data_conversion.py`.
- Exercise caution when using `--run` since the target scripts may perform
  irreversible actions.

## `split_csv.py`

A utility for splitting large CSV files into smaller chunks of a specified
size.  Each chunk preserves the original header row, making them independently
usable.  This is particularly useful for the large GridWatch dataset or any
other CSV files that are difficult to handle due to size constraints.

### Usage

```sh
# split gridwatch.csv into 50MB chunks (default)
python Scripts/split_csv.py GridwatchData/gridwatch.csv

# split with custom chunk size (100MB)
python Scripts/split_csv.py GridwatchData/gridwatch.csv --size 100

# specify output directory and filename prefix
python Scripts/split_csv.py data.csv --output chunks/ --prefix mydata

# full example with all options
python Scripts/split_csv.py GridwatchData/gridwatch.csv \
    --size 50 \
    --output GridwatchData/chunks \
    --prefix gridwatch
```

### Notes

- Default chunk size is 50MB, adjustable via `--size` parameter
- Headers are preserved in every chunk file
- Output files are named `{prefix}_chunk_{001,002,...}.csv`
- Progress and file sizes are reported during splitting
- Each chunk is independently readable as a valid CSV file

### Output

The script creates sequentially numbered chunks and reports:
- Number of rows per chunk
- File size of each chunk
- Total output size
- List of all created files

## `csv_totals.py`

Scans a directory for CSV files and reports:

- Total rows across all files
- Total datapoints (`rows * columns`)
- Per-file row/column/datapoint totals in table format

Defaults and behavior:

- Recursively scans from detected `GDA` repository root
- Truncates long displayed CSV paths for readable terminal output
- Skips common environment folders and ignores `MachineLearning/` entirely
- Prints realtime progress for each file as it is processed
- Writes full run results to `csv_totals.json` for downstream use

### Usage

```sh
# scan recursively from detected GDA project root (default)
python Scripts/csv_totals.py

# scan from repo root and show top 20 largest files by datapoints
python Scripts/csv_totals.py . --top 20

# treat first row as data (for CSV files without headers)
python Scripts/csv_totals.py --no-header

# write JSON output to a custom path
python Scripts/csv_totals.py --json-out Reports/csv_totals.json
```

## `normalize_data_schema.py`

Normalizes `DataSchema.json` by discovering actual CSV headers from dataset storage paths and ensuring every dataset has consistent schema structure.

What it does:

- Discovers CSV headers from `storage.rawPath` by globbing and reading actual files
- Ensures every dataset has `schema.rawCsv`, `schema.parquet`, and `schema.vocab_schema`
- Populates `rawCsv.columns` and `parquet.columns` with discovered headers
- Creates `vocab_schema.csv_header` and `vocab_schema.normalized` as comma-separated column lists
- Preserves existing metadata when no CSV files are found
- Increments `schemaVersion` (patch level) when changes are detected
- Updates `generatedOn` timestamp when changes are made

### Usage

```sh
# normalize DataSchema.json in repository root
python Scripts/normalize_data_schema.py

# preview changes without writing to disk
python Scripts/normalize_data_schema.py --dry-run

# normalize a custom schema file
python Scripts/normalize_data_schema.py --schema path/to/DataSchema.json
```

### When to run

- After adding new datasets to `DataSchema.json`
- After CSV file headers change upstream
- Before committing schema changes to ensure consistency
- As part of CI/CD validation pipelines

## `extract_csv_headers_mapping.py`

Scans a CSV data tree (default: `D:\\Data\\NGRAWData`) and builds a persistent
JSON mapping file for long-term parquet column normalization.

What it does:

- Walks all CSV files under the target root and extracts header row names
- Stores per-file metadata and deterministic `header_hash` values
- Adds per-column mapping scaffolding:
  - `source_header`
  - `normalized_header`
  - `parquet_name`
  - `dtype_hint`
  - `notes`
- Preserves existing mapping values for unchanged source headers
- Prints explicit schema-change alerts if a previously known file's headers change
- Avoids rewriting output when no structural changes are detected

### Usage

```sh
# scan default NG raw data folder and write default output JSON
python Scripts/extract_csv_headers_mapping.py

# custom scan root and output
python Scripts/extract_csv_headers_mapping.py \
  --root D:/Data/NGRAWData \
  --output DataSources/NationalGrid/Processors/csv_header_mappings.json

# preview only (no file writes)
python Scripts/extract_csv_headers_mapping.py --dry-run

# populate normalized_header/parquet_name/dtype_hint for blank mappings
python Scripts/extract_csv_headers_mapping.py --auto-fill-mappings
```

## `verify_setup.py`

A comprehensive environment verification tool that checks all dependencies
including core Python packages, GPU/RAPIDS libraries, and CUDA installation.
Run this after completing `setup.sh` to ensure your environment is properly
configured for both data processing and machine learning workflows.

### Usage

```sh
# run the verification after setup
python Scripts/verify_setup.py
```

### What it checks

- Core packages (pandas, numpy, pyarrow, plotly, scikit-learn, seaborn, requests)
- Optional packages (rpy2, jupyter, matplotlib)
- GPU availability (nvidia-smi)
- CUDA toolkit (nvcc)
- RAPIDS libraries (cuDF, cuML, Dask-cuDF)
- Python version and executable path
- Simple computation tests to verify functionality

The script provides a color-coded summary showing which components are
working correctly and which may need attention.

## `validate_data_schema.py`

Validates dataset files against the root `DataSchema.json` so schema drift is
caught early. It checks:

- Raw CSV headers against `schema.rawCsv.columns`
- Parquet columns against `schema.parquet.columns` (when parquet exists)

In development environments where parquet files are not present, parquet checks
are skipped by default and reported as `SKIP`.

### Usage

```sh
# run from repo root with defaults
python Scripts/validate_data_schema.py

# make parquet presence mandatory (CI / data-ready environments)
python Scripts/validate_data_schema.py --require-parquet

# custom root or schema path
python Scripts/validate_data_schema.py --root . --schema DataSchema.json
```

### Notes

- Exit code is non-zero only when one or more checks fail.
- `WARN` means the schema entry is intentionally non-strict (or missing explicit
  columns), so only partial validation is possible.