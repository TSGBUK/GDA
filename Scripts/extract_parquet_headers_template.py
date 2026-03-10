#!/usr/bin/env python3
"""Scan parquet files and build a header-normalization template JSON.

The output is designed for manual normalization mapping review.
Each discovered source header is emitted with an empty
"internal_normalization" value for you to fill in.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import pyarrow.parquet as pq
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        "pyarrow is required. Install with: pip install pyarrow"
    ) from exc


SKIP_PARTS = {".git", ".venv", "venv", "grid", "__pycache__", "node_modules"}


def detect_repo_root(start: Path) -> Path:
    for parent in [start, *start.parents]:
        if (parent / "DataSources").is_dir() and (parent / "Scripts").is_dir():
            return parent
    return start


def parquet_files(scan_root: Path) -> List[Path]:
    files: List[Path] = []
    for path in scan_root.rglob("*.parquet"):
        if not path.is_file():
            continue
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def dataset_info_for_file(file_path: Path, scan_root: Path, repo_root: Path) -> Tuple[str, str]:
    rel = file_path.relative_to(scan_root)
    parts = list(rel.parts)

    parquet_index = None
    for idx, value in enumerate(parts):
        if value.lower() == "parquet":
            parquet_index = idx
            break

    if parquet_index is None:
        dataset_rel = str(rel.parent).replace("\\", "/")
        parquet_root_rel = dataset_rel
        return dataset_rel, parquet_root_rel

    dataset_parts = parts[:parquet_index]
    parquet_root_parts = parts[: parquet_index + 1]

    dataset_path = Path(*dataset_parts).as_posix()
    parquet_root = Path(*parquet_root_parts).as_posix()
    return dataset_path, parquet_root


def read_columns(file_path: Path) -> List[str]:
    schema = pq.ParquetFile(file_path).schema
    return list(schema.names)


def build_template(
    repo_root: Path,
    max_datasets: int = 0,
    max_files_per_dataset: int = 0,
) -> Dict:
    scan_root = repo_root
    files = parquet_files(scan_root)

    grouped: Dict[str, Dict] = defaultdict(
        lambda: {
            "parquet_root": "",
            "files": [],
            "columns": set(),
        }
    )

    for file_path in files:
        dataset_path, parquet_root = dataset_info_for_file(file_path, scan_root, repo_root)
        bucket = grouped[dataset_path]
        if not bucket["parquet_root"]:
            bucket["parquet_root"] = parquet_root
        bucket["files"].append(file_path)

    dataset_keys = sorted(grouped.keys())
    if max_datasets > 0:
        dataset_keys = dataset_keys[:max_datasets]

    output_datasets = []
    total_files_scanned = 0

    for dataset_key in dataset_keys:
        bucket = grouped[dataset_key]
        files_for_dataset = bucket["files"]
        if max_files_per_dataset > 0:
            files_for_dataset = files_for_dataset[:max_files_per_dataset]

        for parquet_file in files_for_dataset:
            try:
                cols = read_columns(parquet_file)
                bucket["columns"].update(cols)
                total_files_scanned += 1
            except Exception:
                continue

        headers = [
            {
                "source_header": name,
                "internal_normalization": "",
            }
            for name in sorted(bucket["columns"])
        ]

        output_datasets.append(
            {
                "dataset_path": dataset_key,
                "parquet_root": bucket["parquet_root"],
                "files_considered": len(files_for_dataset),
                "headers_discovered": len(headers),
                "headers": headers,
            }
        )

    return {
        "generated_on": datetime.now(timezone.utc).isoformat(),
        "project_root": repo_root.name,
        "scan_root": ".",
        "dataset_count": len(output_datasets),
        "parquet_files_scanned": total_files_scanned,
        "template_version": "1.0.0",
        "datasets": output_datasets,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract parquet headers into a normalization template JSON."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Path inside the repository (default: current directory).",
    )
    parser.add_argument(
        "--output",
        default="parquet_header_normalization_template.json",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--max-datasets",
        type=int,
        default=0,
        help="Limit number of datasets for preview (0 = all).",
    )
    parser.add_argument(
        "--max-files-per-dataset",
        type=int,
        default=0,
        help="Limit parquet files read per dataset for preview (0 = all).",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    repo_root = detect_repo_root(root)

    template = build_template(
        repo_root=repo_root,
        max_datasets=args.max_datasets,
        max_files_per_dataset=args.max_files_per_dataset,
    )

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = (repo_root / output_path).resolve()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(template, indent=2), encoding="utf-8")

    print(f"Wrote template: {output_path}")
    print(f"Datasets: {template['dataset_count']}")
    print(f"Parquet files scanned: {template['parquet_files_scanned']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
