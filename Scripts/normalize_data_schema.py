#!/usr/bin/env python3
"""Normalize DataSchema.json by discovering CSV headers and ensuring schema consistency.

This utility ensures that every dataset in DataSchema.json has:
- schema.rawCsv with explicit columns
- schema.parquet with like-for-like columns mirroring rawCsv
- schema.vocab_schema with csv_header and normalized fields

It discovers actual CSV headers from storage.rawPath and uses them to populate
the schema blocks. If no CSV is found, it preserves existing column metadata or
creates empty structures with appropriate notes.

Usage:
    python Scripts/normalize_data_schema.py
    python Scripts/normalize_data_schema.py --schema path/to/DataSchema.json
    python Scripts/normalize_data_schema.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from datetime import datetime, timezone
from pathlib import Path


def default_repo_root() -> Path:
    """Detect repository root using the same GDA convention as other scripts."""
    try:
        return next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
    except StopIteration:
        return Path.cwd()


def read_csv_header(path: Path) -> list[str]:
    """Read the first row of a CSV file as header, trying multiple encodings."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.reader(handle)
                raw_row = next(reader, [])
                return [cell.strip() for cell in raw_row if cell is not None]
        except Exception:  # noqa: BLE001
            continue
    return []


def extract_column_names(block: dict) -> list[str]:
    """Extract column names from a schema block (rawCsv or parquet)."""
    if not isinstance(block, dict):
        return []

    columns = block.get("columns")
    if not isinstance(columns, list):
        return []

    names: list[str] = []
    for item in columns:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            if name:
                names.append(name)
        elif isinstance(item, str):
            name = item.strip()
            if name:
                names.append(name)

    return names


def make_column_objects(names: list[str]) -> list[dict[str, str]]:
    """Convert column names to schema column objects."""
    return [{"name": name, "type": "mixed"} for name in names]


def discover_header_from_path(repo: Path, raw_path: str) -> list[str]:
    """Discover CSV header by globbing rawPath and reading the first match."""
    if not isinstance(raw_path, str) or not raw_path.strip():
        return []

    pattern = str((repo / raw_path).resolve())
    matches = sorted(glob.glob(pattern, recursive=True))

    for match in matches:
        file_path = Path(match)
        if file_path.is_file() and file_path.suffix.lower() == ".csv":
            header = read_csv_header(file_path)
            if header:
                return header

    return []


def normalize_dataset(repo: Path, dataset: dict) -> tuple[bool, str]:
    """Normalize a single dataset schema block.
    
    Returns (changed, message) tuple.
    """
    dataset_id = dataset.get("id", "unknown")
    storage = dataset.setdefault("storage", {})
    schema = dataset.setdefault("schema", {})
    raw_csv = schema.setdefault("rawCsv", {})
    parquet = schema.setdefault("parquet", {})

    raw_path = storage.get("rawPath") if isinstance(storage, dict) else None
    discovered = discover_header_from_path(repo, raw_path) if raw_path else []

    existing_raw = extract_column_names(raw_csv)
    existing_parquet = [
        col for col in extract_column_names(parquet)
        if col.lower() not in {"all raw columns", "year"}
    ]

    base_columns = discovered or existing_raw or existing_parquet

    changed = False
    details: list[str] = []

    if base_columns:
        new_raw_cols = make_column_objects(base_columns)
        new_parquet_cols = make_column_objects(base_columns)

        if raw_csv.get("columns") != new_raw_cols:
            raw_csv["columns"] = new_raw_cols
            changed = True
            details.append(f"rawCsv.columns={len(base_columns)}")

        if parquet.get("columns") != new_parquet_cols:
            parquet["columns"] = new_parquet_cols
            changed = True
            details.append(f"parquet.columns={len(base_columns)}")
    else:
        # No columns discovered: ensure empty structure with notes.
        if "columns" not in raw_csv:
            raw_csv["columns"] = []
            changed = True
        if "columns" not in parquet:
            parquet["columns"] = []
            changed = True
        if "notes" not in raw_csv:
            raw_csv["notes"] = "No CSV header discovered yet"
            changed = True
        if "notes" not in parquet:
            parquet["notes"] = "No CSV header discovered yet"
            changed = True
        if changed:
            details.append("empty schema structure added")

    vocab_header = ",".join(base_columns) if base_columns else ""
    new_vocab = {
        "csv_header": vocab_header,
        "normalized": vocab_header,
    }

    if schema.get("vocab_schema") != new_vocab:
        schema["vocab_schema"] = new_vocab
        changed = True
        details.append("vocab_schema")

    message = f"{dataset_id}: {', '.join(details)}" if details else f"{dataset_id}: no changes"
    return changed, message


def main() -> int:
    repo = default_repo_root()

    parser = argparse.ArgumentParser(
        description="Normalize DataSchema.json by discovering CSV headers and ensuring schema consistency."
    )
    parser.add_argument(
        "--schema",
        default="DataSchema.json",
        help="Path to DataSchema.json (default: DataSchema.json in repo root).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without writing to disk.",
    )
    args = parser.parse_args()

    schema_path = Path(args.schema).expanduser()
    if not schema_path.is_absolute():
        schema_path = (repo / schema_path).resolve()

    if not schema_path.exists():
        print(f"[ERROR] Schema file not found: {schema_path}")
        return 1

    print(f"Schema path: {schema_path}")
    print(f"Repository root: {repo}")

    with schema_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    datasets = data.get("datasets", [])
    if not isinstance(datasets, list):
        print("[ERROR] No datasets array found in schema")
        return 1

    print(f"Datasets found: {len(datasets)}")
    print("-" * 80)

    changed_count = 0
    for dataset in datasets:
        changed, message = normalize_dataset(repo, dataset)
        if changed:
            changed_count += 1
        print(f"[{'CHANGED' if changed else 'OK':^8}] {message}")

    print("-" * 80)
    print(f"Total datasets: {len(datasets)}")
    print(f"Changed: {changed_count}")
    print(f"Unchanged: {len(datasets) - changed_count}")

    if args.dry_run:
        print("\n[DRY RUN] No changes written to disk.")
        return 0

    if changed_count > 0:
        # Increment schema version (patch level)
        current_version = data.get("schemaVersion", "2.0.0")
        try:
            parts = current_version.split(".")
            if len(parts) == 3:
                major, minor, patch = parts
                new_patch = int(patch) + 1
                data["schemaVersion"] = f"{major}.{minor}.{new_patch}"
                print(f"Schema version: {current_version} → {data['schemaVersion']}")
            else:
                print(f"[WARN] Unexpected schemaVersion format: {current_version}")
        except (ValueError, AttributeError):
            print(f"[WARN] Could not parse schemaVersion: {current_version}")

        data["generatedOn"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        with schema_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(data, handle, indent=2)
            handle.write("\n")

        print(f"\n[WRITTEN] {schema_path}")
    else:
        print("\n[SKIP] No changes needed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
