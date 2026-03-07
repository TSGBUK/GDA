#!/usr/bin/env python3
"""Validate parquet outputs against source CSV files.

This script checks that each CSV has at least one corresponding parquet output
and that the parquet output is not older than the CSV.

Matching rule:
- CSV: <dataset>/<name>.csv
- Parquet candidates: <dataset>/Parquet/**/<name>.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path
from dataclasses import dataclass

SKIP_PARTS = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "grid",
    "node_modules",
    "Processors",
    "Parquet",
}


@dataclass
class ValidationResult:
    ok: int = 0
    missing: int = 0
    stale: int = 0
    skipped: int = 0


def is_ignored(path: Path) -> bool:
    parts = set(path.parts)
    return bool(parts.intersection(SKIP_PARTS))


def iter_csv_files(root: Path):
    for csv_path in root.rglob("*.csv"):
        if is_ignored(csv_path):
            continue
        yield csv_path


def validate_file(csv_path: Path) -> tuple[str, str]:
    dataset_dir = csv_path.parent
    parquet_root = dataset_dir / "Parquet"

    if not parquet_root.exists():
        return "missing", f"[missing] {csv_path}: no Parquet directory"

    parquet_name = f"{csv_path.stem}.parquet"
    candidates = list(parquet_root.glob(f"**/{parquet_name}"))

    if not candidates:
        return "missing", f"[missing] {csv_path}: no parquet match for {parquet_name}"

    csv_mtime = csv_path.stat().st_mtime
    newest = max(p.stat().st_mtime for p in candidates)

    if newest < csv_mtime:
        return "stale", f"[stale] {csv_path}: parquet older than csv"

    return "ok", f"[ok] {csv_path}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate parquet freshness/completeness against CSV files."
    )
    parser.add_argument(
        "--root",
        default="..",
        help="Root folder to scan (default: .. from Scripts).",
    )
    parser.add_argument(
        "--show-ok",
        action="store_true",
        help="Print [ok] lines for files that pass validation.",
    )
    parser.add_argument(
        "--report",
        help="Optional report file path.",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    result = ValidationResult()
    lines: list[str] = []

    for csv_path in iter_csv_files(root):
        state, line = validate_file(csv_path)
        if state == "ok":
            result.ok += 1
            if args.show_ok:
                lines.append(line)
        elif state == "missing":
            result.missing += 1
            lines.append(line)
        elif state == "stale":
            result.stale += 1
            lines.append(line)
        else:
            result.skipped += 1

    summary = [
        "------------------------------------------------------------",
        "Parquet Validation Summary",
        "------------------------------------------------------------",
        f"Root: {root}",
        f"OK: {result.ok}",
        f"Missing: {result.missing}",
        f"Stale: {result.stale}",
        f"Skipped: {result.skipped}",
    ]

    output_lines = lines + summary

    for line in output_lines:
        print(line)

    if args.report:
        Path(args.report).write_text("\n".join(output_lines) + "\n", encoding="utf-8")

    return 0 if (result.missing == 0 and result.stale == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
