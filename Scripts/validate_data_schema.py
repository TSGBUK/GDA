#!/usr/bin/env python3
"""Validate dataset files against DataSchema.json.

This script is intended for development and CI-style checks. It compares:
- raw CSV headers against expected raw schema columns
- parquet columns against expected parquet schema columns (when parquet exists)

In dev environments where parquet data is absent, parquet checks are skipped by
 default and reported as such.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple


@dataclass
class CheckResult:
    dataset_id: str
    target: str
    status: str  # PASS | FAIL | SKIP | WARN
    message: str


def load_schema(schema_path: Path) -> dict:
    with schema_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_header(value: str) -> str:
    return value.strip().strip('"').strip()


def read_csv_header(file_path: Path) -> List[str]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        row = next(reader, [])
    return [normalize_header(cell) for cell in row]


def get_expected_columns(schema_block: dict, key: str) -> Optional[List[str]]:
    block = schema_block.get(key)
    if not isinstance(block, dict):
        return None

    cols = block.get("columns")
    if not isinstance(cols, list):
        return None

    expected: List[str] = []
    for item in cols:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            expected.append(item["name"])

    return expected or None


def compare_columns(expected: Sequence[str], actual: Sequence[str]) -> Tuple[Set[str], Set[str]]:
    expected_set = set(expected)
    actual_set = set(actual)
    missing = expected_set - actual_set
    unexpected = actual_set - expected_set
    return missing, unexpected


def find_first_match(root: Path, pattern: str) -> Optional[Path]:
    matches = sorted(root.glob(pattern))
    for match in matches:
        if match.is_file():
            return match
    return None


def validate_raw_csv(root: Path, dataset: dict) -> CheckResult:
    dataset_id = dataset.get("id", "unknown")
    storage = dataset.get("storage", {})
    schema = dataset.get("schema", {})

    expected = get_expected_columns(schema, "rawCsv")
    raw_path = storage.get("rawPath")
    if not isinstance(raw_path, str):
        return CheckResult(dataset_id, "rawCsv", "SKIP", "No rawPath defined")

    sample = find_first_match(root, raw_path)
    if sample is None:
        return CheckResult(dataset_id, "rawCsv", "SKIP", f"No files matched pattern: {raw_path}")

    if not expected:
        return CheckResult(
            dataset_id,
            "rawCsv",
            "WARN",
            f"No explicit rawCsv.columns in schema; sample file found: {sample.relative_to(root)}",
        )

    actual = read_csv_header(sample)
    missing, unexpected = compare_columns(expected, actual)

    if not missing and not unexpected:
        return CheckResult(
            dataset_id,
            "rawCsv",
            "PASS",
            f"Header matches for sample {sample.relative_to(root)}",
        )

    parts: List[str] = []
    if missing:
        parts.append(f"missing={sorted(missing)}")
    if unexpected:
        parts.append(f"unexpected={sorted(unexpected)}")

    return CheckResult(
        dataset_id,
        "rawCsv",
        "FAIL",
        f"Header mismatch in {sample.relative_to(root)}: {'; '.join(parts)}",
    )


def find_parquet_sample(parquet_dir: Path) -> Optional[Path]:
    for file_path in parquet_dir.rglob("*.parquet"):
        if file_path.is_file():
            return file_path
    return None


def should_skip_strict_parquet(expected_cols: Sequence[str]) -> bool:
    lowered = [col.lower() for col in expected_cols]
    return any("all raw columns" in col for col in lowered)


def validate_parquet(root: Path, dataset: dict, require_parquet: bool) -> CheckResult:
    dataset_id = dataset.get("id", "unknown")
    storage = dataset.get("storage", {})
    schema = dataset.get("schema", {})

    parquet_path = storage.get("parquetPath")
    if not isinstance(parquet_path, str):
        return CheckResult(dataset_id, "parquet", "SKIP", "No parquetPath defined")

    parquet_dir = root / parquet_path
    if not parquet_dir.exists():
        status = "FAIL" if require_parquet else "SKIP"
        return CheckResult(dataset_id, "parquet", status, f"Parquet path not found: {parquet_path}")

    sample = find_parquet_sample(parquet_dir)
    if sample is None:
        status = "FAIL" if require_parquet else "SKIP"
        return CheckResult(dataset_id, "parquet", status, f"No parquet files found under: {parquet_path}")

    expected = get_expected_columns(schema, "parquet")
    if not expected:
        return CheckResult(dataset_id, "parquet", "WARN", f"No explicit parquet.columns; sample {sample.relative_to(root)}")

    if should_skip_strict_parquet(expected):
        return CheckResult(dataset_id, "parquet", "SKIP", "Schema uses non-strict parquet placeholder columns")

    try:
        import pyarrow.parquet as pq
    except Exception:
        return CheckResult(dataset_id, "parquet", "SKIP", "pyarrow not available for parquet schema checks")

    table = pq.read_table(sample)
    actual = list(table.schema.names)
    missing, unexpected = compare_columns(expected, actual)

    if not missing and not unexpected:
        return CheckResult(dataset_id, "parquet", "PASS", f"Parquet schema matches for sample {sample.relative_to(root)}")

    parts: List[str] = []
    if missing:
        parts.append(f"missing={sorted(missing)}")
    if unexpected:
        parts.append(f"unexpected={sorted(unexpected)}")

    return CheckResult(
        dataset_id,
        "parquet",
        "FAIL",
        f"Parquet mismatch in {sample.relative_to(root)}: {'; '.join(parts)}",
    )


def print_results(results: Iterable[CheckResult]) -> int:
    failures = 0
    summary = {"PASS": 0, "FAIL": 0, "SKIP": 0, "WARN": 0}

    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
        print(f"[{result.status}] {result.dataset_id}:{result.target} - {result.message}")
        if result.status == "FAIL":
            failures += 1

    print("\nSummary:")
    print(f"  PASS: {summary['PASS']}")
    print(f"  WARN: {summary['WARN']}")
    print(f"  SKIP: {summary['SKIP']}")
    print(f"  FAIL: {summary['FAIL']}")

    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate dataset files against DataSchema.json")
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Project root directory (defaults to repository root)",
    )
    parser.add_argument(
        "--schema",
        default="DataSchema.json",
        help="Schema file path relative to --root (default: DataSchema.json)",
    )
    parser.add_argument(
        "--require-parquet",
        action="store_true",
        help="Treat missing parquet directories/files as failures instead of skips",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    schema_path = (root / args.schema).resolve()

    if not schema_path.exists():
        print(f"[FAIL] Schema file not found: {schema_path}")
        return 1

    schema = load_schema(schema_path)
    datasets = schema.get("datasets", [])
    if not isinstance(datasets, list) or not datasets:
        print("[FAIL] No datasets found in schema")
        return 1

    results: List[CheckResult] = []
    for dataset in datasets:
        if not isinstance(dataset, dict):
            continue
        results.append(validate_raw_csv(root, dataset))
        results.append(validate_parquet(root, dataset, require_parquet=args.require_parquet))

    return print_results(results)


if __name__ == "__main__":
    raise SystemExit(main())
