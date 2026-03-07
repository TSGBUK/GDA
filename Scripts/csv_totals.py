#!/usr/bin/env python3
"""Scan CSV files and calculate row and datapoint totals.

Datapoints are calculated as:
    datapoints = data_rows * column_count

By default, the script treats the first CSV row as a header and excludes it
from the row count. Use ``--no-header`` if files do not have headers.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_SKIP_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
    "grid",
    "machinelearning",
}

MAX_DISPLAY_PATH = 64


def default_root() -> Path:
    """Detect repository root using the same GDA convention as other scripts."""
    try:
        return next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
    except StopIteration:
        return Path.cwd()


def should_skip_path(path: Path) -> bool:
    lower = str(path).replace("\\", "/").lower()
    if "/site-packages/" in lower:
        return True
    if "/lib/" in lower and "/python" in lower:
        return True
    return False


def discover_csv_files(root: Path, recursive: bool = True) -> list[Path]:
    files: list[Path] = []

    if recursive:
        for dirpath, dirnames, filenames in os.walk(root):
            current = Path(dirpath)
            if should_skip_path(current):
                dirnames[:] = []
                continue

            dirnames[:] = [
                d for d in dirnames if d.lower() not in DEFAULT_SKIP_DIR_NAMES
            ]
            for name in filenames:
                if name.lower().endswith(".csv"):
                    files.append(current / name)
    else:
        files = [p for p in root.glob("*.csv") if p.is_file()]

    files.sort()
    return files


def count_csv(path: Path, has_header: bool = True) -> tuple[int, int]:
    """Return (data_rows, columns) for a CSV file."""
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.reader(handle)

        try:
            first_row = next(reader)
        except StopIteration:
            return 0, 0

        column_count = len(first_row)

        if has_header:
            row_count = sum(1 for _ in reader)
            return row_count, column_count

        row_count = 1 + sum(1 for _ in reader)
        return row_count, column_count


def format_int(value: int) -> str:
    return f"{value:,}"


def truncate_path(path: str, max_len: int = MAX_DISPLAY_PATH) -> str:
    if len(path) <= max_len:
        return path

    # Keep the tail so filename and nearby folders remain visible.
    tail_len = max_len - 3
    return "..." + path[-tail_len:]


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: list[str]) -> str:
        return " | ".join(val.ljust(widths[i]) for i, val in enumerate(values))

    separator = "-+-".join("-" * w for w in widths)
    lines = [fmt_row(headers), separator]
    lines.extend(fmt_row(r) for r in rows)
    return "\n".join(lines)


def to_posix_display(path: Path) -> str:
    return str(path).replace("\\", "/")


def main() -> int:
    detected_root = default_root()

    parser = argparse.ArgumentParser(
        description="Find CSV files and calculate total rows and datapoints."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=str(detected_root),
        help="Root directory to scan (default: detected GDA repository root).",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Treat first row as data (do not exclude a header row).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="Show top N files by datapoints (default: show all).",
    )
    parser.add_argument(
        "--non-recursive",
        action="store_true",
        help="Only scan CSV files directly inside root.",
    )
    parser.add_argument(
        "--json-out",
        default="csv_totals.json",
        help="JSON output path (relative paths are resolved from scan root).",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Root path is not a directory: {root}")

    csv_files = discover_csv_files(root, recursive=not args.non_recursive)
    json_out = Path(args.json_out).expanduser()
    if not json_out.is_absolute():
        json_out = (root / json_out).resolve()

    run_payload: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "options": {
            "no_header": bool(args.no_header),
            "top": int(args.top),
            "non_recursive": bool(args.non_recursive),
            "json_out": str(json_out),
        },
    }

    if not csv_files:
        print(f"No CSV files found under: {root}")
        run_payload["summary"] = {
            "csv_files_found": 0,
            "csv_files_processed": 0,
            "csv_files_failed": 0,
            "total_rows": 0,
            "total_datapoints": 0,
        }
        run_payload["files"] = []
        run_payload["failures"] = []
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(run_payload, indent=2), encoding="utf-8")
        print(f"JSON output written: {json_out}")
        return 0

    rows_total = 0
    datapoints_total = 0
    running_datapoints = 0
    processed = 0
    failures: list[tuple[Path, str]] = []
    file_stats: list[tuple[Path, int, int, int]] = []
    file_records: list[dict[str, object]] = []

    total_files = len(csv_files)
    print(f"Processing {total_files} CSV file(s)...")

    for index, path in enumerate(csv_files, start=1):
        rel = path.relative_to(root) if path.is_relative_to(root) else path
        rel_text = to_posix_display(rel)
        display_path = truncate_path(rel_text)

        try:
            rows, cols = count_csv(path, has_header=not args.no_header)
        except Exception as exc:  # noqa: BLE001
            err_text = str(exc)
            failures.append((path, err_text))
            file_records.append(
                {
                    "path": rel_text,
                    "rows": None,
                    "columns": None,
                    "datapoints": None,
                    "status": "failed",
                    "error": err_text,
                }
            )
            print(
                f"[{index}/{total_files}] FAILED {display_path} -> {err_text}",
                flush=True,
            )
            continue

        datapoints = rows * cols
        file_stats.append((path, rows, cols, datapoints))
        file_records.append(
            {
                "path": rel_text,
                "rows": rows,
                "columns": cols,
                "datapoints": datapoints,
                "status": "ok",
                "error": None,
            }
        )
        rows_total += rows
        datapoints_total += datapoints
        running_datapoints += datapoints
        processed += 1
        print(
            f"[{index}/{total_files}] OK {display_path} -> rows={format_int(rows)}, cols={format_int(cols)}, "
            f"datapoints={format_int(datapoints)}, running_datapoints={format_int(running_datapoints)}",
            flush=True,
        )

    file_stats.sort(key=lambda item: item[3], reverse=True)

    summary_rows = [
        ["Scan root", str(root)],
        ["CSV files found", format_int(len(csv_files))],
        ["CSV files processed", format_int(processed)],
        ["CSV files failed", format_int(len(failures))],
        ["Total rows", format_int(rows_total)],
        ["Total datapoints", format_int(datapoints_total)],
    ]
    print(render_table(["Metric", "Value"], summary_rows))

    run_payload["summary"] = {
        "csv_files_found": len(csv_files),
        "csv_files_processed": processed,
        "csv_files_failed": len(failures),
        "total_rows": rows_total,
        "total_datapoints": datapoints_total,
    }

    if file_stats:
        limit = args.top if args.top and args.top > 0 else len(file_stats)
        per_file_rows: list[list[str]] = []
        for path, rows, cols, datapoints in file_stats[:limit]:
            rel = path.relative_to(root) if path.is_relative_to(root) else path
            display_path = truncate_path(to_posix_display(rel))
            per_file_rows.append(
                [
                    display_path,
                    format_int(rows),
                    format_int(cols),
                    format_int(datapoints),
                ]
            )

        print("\nPer-file totals:")
        print(render_table(["CSV", "Rows", "Cols", "Datapoints"], per_file_rows))

    if failures:
        fail_rows: list[list[str]] = []
        for path, err in failures:
            rel = path.relative_to(root) if path.is_relative_to(root) else path
            display_path = truncate_path(to_posix_display(rel))
            fail_rows.append([display_path, err])

        print("\nFiles that could not be processed:")
        print(render_table(["CSV", "Error"], fail_rows))

    run_payload["files"] = sorted(
        file_records,
        key=lambda item: int(item["datapoints"] or -1),
        reverse=True,
    )
    run_payload["failures"] = [
        {
            "path": to_posix_display(path.relative_to(root) if path.is_relative_to(root) else path),
            "error": err,
        }
        for path, err in failures
    ]

    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(run_payload, indent=2), encoding="utf-8")
    print(f"\nJSON output written: {json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
