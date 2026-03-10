#!/usr/bin/env python3
"""Build and maintain CSV header mapping metadata for parquet normalization.

Scans a directory tree for CSV files, extracts header rows, and writes a JSON
mapping file used to normalize column names over time.

Key behavior:
- Preserves existing per-column mapping values where possible.
- Detects schema drift using header hash comparison.
- Prints schema-change alerts when a known file's header changes.
- Avoids rewriting the output file when nothing changed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROOT = Path(r"D:\Data\NGRAWData")
DEFAULT_OUTPUT = Path("DataSources/NationalGrid/Processors/csv_header_mappings.json")
SKIP_PARTS = {".git", ".venv", "venv", "__pycache__", "node_modules"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def to_iso_from_stat(st_mtime: float) -> str:
    return datetime.fromtimestamp(st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_header(header: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in header:
        normalized.append((value or "").strip())
    return normalized


def is_substation_loading_file(file_path: Path) -> bool:
    name = file_path.name.lower()
    parent = file_path.parent.name.lower()
    return "substation_load" in name or "substation loading" in parent


def is_likely_timestamp(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    return bool(
        re.match(
            r"^\d{4}-\d{2}-\d{2}(?:[ tT]\d{2}:\d{2}(?::\d{2})?)?",
            text,
        )
    )


def make_unique_headers(headers: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    unique: list[str] = []
    for header in headers:
        base = (header or "").strip() or "unnamed"
        count = counts.get(base, 0) + 1
        counts[base] = count
        unique.append(base if count == 1 else f"{base}__{count}")
    return unique


def to_snake_case(value: str) -> str:
    text = value.strip().lower()
    if not text:
        return ""

    replacements = {
        "%": " percent ",
        "#": " number ",
        "&": " and ",
        "/": " ",
        "\\": " ",
        "(": " ",
        ")": " ",
        "[": " ",
        "]": " ",
        "{": " ",
        "}": " ",
        "-": " ",
        ".": " ",
        ":": " ",
        ";": " ",
        ",": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""

    words = text.split(" ")
    out_words: list[str] = []
    for word in words:
        if word in {"no", "num", "number"}:
            out_words.append("id")
            continue
        if word in {"dt", "dttm", "datetime"}:
            out_words.append("timestamp")
            continue
        cleaned = re.sub(r"[^a-z0-9_]", "", word)
        if cleaned:
            out_words.append(cleaned)

    if not out_words:
        return ""

    compact: list[str] = []
    for word in out_words:
        if compact and compact[-1] == word:
            continue
        compact.append(word)

    return "_".join(compact)


def derive_substation_loading_headers(file_path: Path, encoding: str, delimiter: str) -> list[str]:
    rows: list[list[str]] = []
    with file_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        for row in reader:
            rows.append([str(cell).strip() for cell in row])
            if rows and is_likely_timestamp(rows[-1][0] if rows[-1] else ""):
                break

    if not rows:
        return []

    data_start = next(
        (index for index, row in enumerate(rows) if is_likely_timestamp(row[0] if row else "")),
        None,
    )
    if data_start is None:
        return normalize_header(rows[0])

    label_row = rows[data_start - 1] if data_start >= 1 else []
    unit_row = rows[data_start - 2] if data_start >= 2 else []
    value_count = max(len(label_row), len(unit_row), len(rows[data_start])) - 1

    derived_headers = ["timestamp_utc"]
    for offset in range(value_count):
        label_text = label_row[offset + 1] if offset + 1 < len(label_row) else ""
        unit_text = unit_row[offset + 1] if offset + 1 < len(unit_row) else ""

        metric = "mvar" if "mvar" in unit_text.lower() else "mw" if "mw" in unit_text.lower() else ("mw" if offset % 2 == 0 else "mvar")
        label_slug = to_snake_case(label_text)
        if not label_slug or label_slug in {"mw", "mvar", "power_units", "units", "timestamp", "date", "datetime", "time"}:
            pair_index = (offset // 2) + 1
            label_slug = f"node_{pair_index:03d}"

        derived_headers.append(f"{label_slug}_{metric}")

    return make_unique_headers(derived_headers)


def infer_dtype_hint(source_header: str, normalized_header: str) -> str:
    text = f" {source_header} {normalized_header} ".lower()

    if normalized_header in {"timestamp", "timestamp_utc"} or normalized_header.endswith("_timestamp"):
        return "timestamp"
    if normalized_header.endswith("_mw") or normalized_header.endswith("_mvar"):
        return "float64"

    if any(token in text for token in (" timestamp ", " datetime ", " utc ", "time_stamp")):
        return "timestamp"
    if any(token in text for token in (" date ", "_date", "date_", "trading_day", "settlement_date")):
        return "date"
    if any(token in text for token in ("year", "month", "day", "hour", "minute", "week", "period", "index")):
        return "int64"
    if any(token in text for token in (" is_", " has_", " flag ", " enabled ", " active ", "boolean")):
        return "bool"
    if any(
        token in text
        for token in (
            " mw",
            " mvar",
            "mwh",
            "kwh",
            " kw",
            " kv",
            "voltage",
            "current",
            "temperature",
            " temp ",
            "speed",
            "pressure",
            "load",
            "demand",
            "generation",
            "forecast",
            "price",
            "cost",
            "amount",
            "rate",
            "ratio",
            "percent",
            "power",
            "energy",
            "value",
            "latitude",
            "longitude",
            " lat",
            " lon",
        )
    ):
        return "float64"
    if any(token in text for token in (" id", "name", "type", "source", "target", "status", "code", "postcode", "address")):
        return "string"
    return "string"


def auto_fill_mapping(source_header: str) -> dict[str, str]:
    normalized = to_snake_case(source_header)
    parquet_name = normalized
    dtype_hint = infer_dtype_hint(source_header, normalized)
    note = "auto-filled heuristic v1"

    return {
        "normalized_header": normalized,
        "parquet_name": parquet_name,
        "dtype_hint": dtype_hint,
        "notes": note,
    }


def header_hash(header: list[str]) -> str:
    joined = "\n".join(header)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def read_header(file_path: Path) -> tuple[list[str], str, str]:
    """Return (header, encoding_used, delimiter)."""
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    for encoding in encodings:
        try:
            with file_path.open("r", encoding=encoding, newline="") as handle:
                sample = handle.read(4096)
                handle.seek(0)
                delimiter = ","
                if sample:
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                        delimiter = dialect.delimiter
                    except Exception:
                        delimiter = ","
                if is_substation_loading_file(file_path):
                    return derive_substation_loading_headers(file_path, encoding, delimiter), encoding, delimiter

                reader = csv.reader(handle, delimiter=delimiter)
                row = next(reader, [])
                return make_unique_headers(normalize_header(row)), encoding, delimiter
        except Exception:
            continue
    return [], "unknown", ","


def csv_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.csv"):
        if not path.is_file():
            continue
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def build_column_mappings(headers: list[str], existing: list[dict[str, Any]], auto_fill: bool) -> list[dict[str, Any]]:
    existing_map: dict[tuple[str, int], dict[str, Any]] = {}
    existing_counts: dict[str, int] = {}
    for item in existing:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source_header", "")).strip()
        if source:
            count = existing_counts.get(source, 0) + 1
            existing_counts[source] = count
            existing_map[(source, count)] = item

    output: list[dict[str, Any]] = []
    output_counts: dict[str, int] = {}
    for source in headers:
        count = output_counts.get(source, 0) + 1
        output_counts[source] = count
        previous = existing_map.get((source, count), {})
        auto_values = auto_fill_mapping(source) if auto_fill else {
            "normalized_header": "",
            "parquet_name": "",
            "dtype_hint": "",
            "notes": "",
        }

        existing_normalized = str(previous.get("normalized_header", "")).strip()
        existing_parquet = str(previous.get("parquet_name", "")).strip()
        existing_dtype = str(previous.get("dtype_hint", "")).strip()
        existing_notes = str(previous.get("notes", "")).strip()

        output.append(
            {
                "source_header": source,
                "normalized_header": existing_normalized or auto_values["normalized_header"],
                "parquet_name": existing_parquet or auto_values["parquet_name"],
                "dtype_hint": existing_dtype or auto_values["dtype_hint"],
                "notes": existing_notes or auto_values["notes"],
            }
        )
    return output


def relative_dataset(rel_path: Path) -> str:
    return rel_path.parts[0] if rel_path.parts else ""


def build_file_entry(file_path: Path, root: Path, old_entry: dict[str, Any], auto_fill: bool) -> dict[str, Any]:
    rel = file_path.relative_to(root)
    stat = file_path.stat()
    headers, encoding_used, delimiter = read_header(file_path)
    previous_mappings = old_entry.get("column_mappings", []) if isinstance(old_entry, dict) else []

    return {
        "relative_path": rel.as_posix(),
        "file_path": str(file_path),
        "file_name": file_path.name,
        "dataset": relative_dataset(rel),
        "size_bytes": stat.st_size,
        "modified_utc": to_iso_from_stat(stat.st_mtime),
        "encoding_used": encoding_used,
        "delimiter": delimiter,
        "header_fields": headers,
        "header_count": len(headers),
        "header_hash": header_hash(headers),
        "column_mappings": build_column_mappings(
            headers,
            previous_mappings if isinstance(previous_mappings, list) else [],
            auto_fill=auto_fill,
        ),
    }


def stable_payload(payload: dict[str, Any]) -> dict[str, Any]:
    stable = dict(payload)
    stable.pop("generated_at", None)
    stable.pop("schema_change_count", None)
    return stable


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract CSV headers and maintain a JSON mapping file for parquet normalization."
    )
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="Directory tree to scan for CSV files.")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output JSON path. Relative paths are resolved from current working directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write output; print planned changes and schema alerts.",
    )
    parser.add_argument(
        "--auto-fill-mappings",
        action="store_true",
        help="Auto-populate normalized_header, parquet_name, dtype_hint, and notes when blank.",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = (Path.cwd() / output).resolve()

    if not root.exists() or not root.is_dir():
        print(f"[ERROR] Scan root not found or not a directory: {root}")
        return 1

    old_payload = read_json(output)
    old_files = old_payload.get("files", {}) if isinstance(old_payload.get("files"), dict) else {}

    discovered = csv_files(root)
    new_files: dict[str, Any] = {}
    schema_alerts: list[dict[str, Any]] = []

    for file_path in discovered:
        rel_key = file_path.relative_to(root).as_posix()
        old_entry = old_files.get(rel_key, {}) if isinstance(old_files, dict) else {}
        old_hash = old_entry.get("header_hash") if isinstance(old_entry, dict) else None

        entry = build_file_entry(
            file_path,
            root,
            old_entry if isinstance(old_entry, dict) else {},
            auto_fill=args.auto_fill_mappings,
        )
        new_files[rel_key] = entry

        if isinstance(old_hash, str) and old_hash and old_hash != entry["header_hash"]:
            schema_alerts.append(
                {
                    "relative_path": rel_key,
                    "old_header": old_entry.get("header_fields", []),
                    "new_header": entry["header_fields"],
                }
            )

    removed_files = []
    if isinstance(old_files, dict):
        removed_files = sorted(set(old_files.keys()) - set(new_files.keys()))

    payload = {
        "generated_at": utc_now_iso(),
        "scan_root": str(root),
        "output_file": str(output),
        "file_count": len(new_files),
        "schema_change_count": len(schema_alerts),
        "removed_file_count": len(removed_files),
        "files": dict(sorted(new_files.items())),
    }

    changed = stable_payload(payload) != stable_payload(old_payload if isinstance(old_payload, dict) else {})

    print(f"Scan root: {root}")
    print(f"CSV files discovered: {len(new_files)}")
    if removed_files:
        print(f"Removed since last run: {len(removed_files)}")

    if schema_alerts:
        print("\n[SCHEMA CHANGES DETECTED]")
        for alert in schema_alerts:
            print(f"- {alert['relative_path']}")
            print(f"  old: {alert['old_header']}")
            print(f"  new: {alert['new_header']}")

    if args.dry_run:
        print("\n[DRY RUN] Output file not written.")
        print(f"Would write: {changed}")
        return 0

    if not changed:
        print("\n[SKIP] No changes detected; output was not rewritten.")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    print(f"\n[WRITTEN] {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
